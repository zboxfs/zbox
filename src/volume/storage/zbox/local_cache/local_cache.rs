use std::fmt::{self, Debug};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use linked_hash_map::LinkedHashMap;
use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};

use super::super::http_client::{CacheControl, HttpClient};
use super::{CacheBackend, CacheType, DummyBackend};
use base::crypto::{Crypto, Key};
use base::IntoRef;
use error::{Error, Result};

// cached item in local cache
#[derive(Debug, Clone, Deserialize, Serialize)]
struct CacheItem {
    len: usize,
    is_pinned: bool,
}

impl CacheItem {
    #[inline]
    fn new(len: usize, is_pinned: bool) -> Self {
        CacheItem { len, is_pinned }
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct CacheMeta {
    cache_type: CacheType,

    // local cache capacity and used size, in bytes
    capacity: usize,
    used: usize,

    // update sequence, used to sync with remote
    useq: u64,

    // LRU list
    lru: LinkedHashMap<PathBuf, CacheItem>,
}

pub struct LocalCache {
    meta: CacheMeta,

    // local storage backend
    backend: Box<CacheBackend>,

    // local cache change flag
    is_changed: bool,

    // http client
    client: HttpClient,

    crypto: Crypto,
    key: Key,
}

impl LocalCache {
    const META_FILE_NAME: &'static str = "cache_meta";

    pub fn new(
        cache_type: CacheType,
        capacity_in_mb: usize,
        base: &Path,
        repo_id: &str,
        access_key: &str,
    ) -> Result<Self> {
        let capacity = capacity_in_mb * 1024 * 1024; // capacity is in MB
        let client = HttpClient::new(repo_id, access_key)?;

        let mut meta = CacheMeta::default();
        meta.cache_type = cache_type;
        meta.capacity = capacity;

        let backend: Box<CacheBackend> = match cache_type {
            CacheType::Mem => {
                Box::new(super::mem::MemBackend::new())
            }
            CacheType::File => {
                #[cfg(feature = "storage-zbox-wasm")]
                {
                    return Err(Error::InvalidUri);
                }
                #[cfg(not(feature = "storage-zbox-wasm"))]
                {
                    Box::new(super::file::FileBackend::new(base))
                }
            }
            CacheType::Browser => {
                #[cfg(feature = "storage-zbox-wasm")]
                {
                    let _ = base;
                    Box::new(super::browser::WasmBackend::new())
                }
                #[cfg(not(feature = "storage-zbox-wasm"))]
                {
                    return Err(Error::InvalidUri);
                }
            }
        };

        Ok(LocalCache {
            meta,
            backend,
            is_changed: false,
            client,
            crypto: Crypto::default(),
            key: Key::new_empty(),
        })
    }

    #[inline]
    pub fn set_crypto_ctx(&mut self, crypto: Crypto, key: Key) {
        self.crypto = crypto;
        self.key = key;
    }

    #[inline]
    pub fn repo_exists(&self) -> Result<bool> {
        self.client.repo_exists()
    }

    #[inline]
    pub fn connect(&mut self) -> Result<()> {
        self.client.open_session()?;
        Ok(())
    }

    // evict objects in local cache
    // to_evict: list of tuple (object key, object length)
    fn evict(&mut self, to_evict: &[(PathBuf, usize)]) -> Result<()> {
        for item in to_evict {
            self.backend.remove(&item.0)?;
            self.meta.lru.remove(&item.0);
            self.meta.used -= item.1;
        }

        Ok(())
    }

    // make a specified size place in local cache for an object
    fn reserve_place(&mut self, len: usize) -> Result<()> {
        // if local cache still has enough space
        if self.meta.used + len <= self.meta.capacity {
            return Ok(());
        }

        // otherwise, we need to evict some objects in local cache to make
        // enough space

        let need_len = self.meta.used + len - self.meta.capacity;
        let mut accum_len = 0;
        let mut to_evict: Vec<(PathBuf, usize)> = Vec::new();

        // try to find enough least-used objects in unpinned list first
        for ent in self.meta.lru.entries().filter(|ent| !ent.get().is_pinned) {
            accum_len += ent.get().len;
            to_evict.push((ent.key().clone(), ent.get().len));
            if accum_len >= need_len {
                break;
            }
        }

        // if sapce is still not enough, try to find objects in pinned list
        if accum_len < need_len {
            for ent in self.meta.lru.entries().filter(|ent| ent.get().is_pinned)
            {
                accum_len += ent.get().len;
                to_evict.push((ent.key().clone(), ent.get().len));
                if accum_len >= need_len {
                    break;
                }
            }
        }

        assert!(accum_len >= need_len);

        // do eviction
        self.evict(&to_evict)
    }

    // ensure data is downloaded to local cache
    fn ensure_in_local(
        &mut self,
        rel_path: &Path,
        is_pinned: bool,
    ) -> Result<()> {
        self.is_changed = true;

        // if object is already in cache
        if self.backend.contains(rel_path) {
            let _ = self.meta.lru.get_refresh(rel_path);
            return Ok(());
        }

        // if object is not in cache, get it from remote and then add
        // to local cache
        let remote = {
            let obj =
                self.client.get(rel_path, CacheControl::from(is_pinned))?;
            self.reserve_place(obj.len())?;
            obj
        };
        self.backend.insert(rel_path, &remote)?;

        // add to lru and increase used size
        self.meta.lru.insert(
            rel_path.to_path_buf(),
            CacheItem::new(remote.len(), is_pinned),
        );
        self.meta.used += remote.len();

        Ok(())
    }

    fn load_meta(&mut self) -> Result<CacheMeta> {
        let path = Path::new(Self::META_FILE_NAME);
        let buf = self
            .backend
            .get(&path)
            .and_then(|buf| self.crypto.decrypt(&buf, &self.key))?;
        let mut de = Deserializer::new(&buf[..]);
        let meta: CacheMeta = Deserialize::deserialize(&mut de)?;
        Ok(meta)
    }

    fn save_meta(&mut self) -> Result<()> {
        // get latest update sequence from http client
        self.meta.useq = self.client.get_update_seq();

        // serialize meta and write it to local cache
        let mut buf = Vec::new();
        self.meta.serialize(&mut Serializer::new(&mut buf))?;
        let path = Path::new(Self::META_FILE_NAME);
        self.crypto
            .encrypt(&buf, &self.key)
            .and_then(|buf| self.backend.insert(&path, &buf))
    }

    #[inline]
    pub fn init(&mut self) -> Result<()> {
        self.backend.clear().and_then(|_| self.save_meta())
    }

    pub fn open(&mut self) -> Result<()> {
        // memory-based local cache doesn't need open
        if self.meta.cache_type == CacheType::Mem {
            return Ok(());
        }

        // load cache meta
        match self.load_meta() {
            Ok(meta) => {
                // cache type must match
                if self.meta.cache_type != meta.cache_type {
                    return Err(Error::InvalidUri);
                }

                // get remote update sequence
                let remote_useq = self.client.get_update_seq();

                // only if the update sequences are matched, we can then
                // use the local cache
                if meta.useq == remote_useq {
                    self.meta.useq = remote_useq;
                    self.meta.used = meta.used;
                    self.meta.lru = meta.lru;
                    return Ok(());
                }

                // otherwise, clear the local cache
                warn!(
                    "update seq not match, local: {}, remote: {}, \
                     clear local cache",
                    meta.useq, remote_useq,
                );
                self.backend.clear()?;
                self.meta.used = 0;
                self.meta.lru.clear();
                self.save_meta()
            }
            Err(ref err) if *err == Error::NotFound => self.save_meta(),
            Err(err) => Err(err),
        }
    }

    #[inline]
    pub fn get_to(
        &mut self,
        rel_path: &Path,
        offset: usize,
        dst: &mut [u8],
    ) -> Result<()> {
        self.ensure_in_local(rel_path, false)?;
        self.backend.get_exact(rel_path, offset, dst)
    }

    #[inline]
    pub fn get(&mut self, rel_path: &Path) -> Result<Vec<u8>> {
        self.ensure_in_local(rel_path, true)?;
        self.backend.get(rel_path)
    }

    fn do_put(
        &mut self,
        rel_path: &Path,
        offset: usize,
        obj: &[u8],
        is_pinned: bool,
    ) -> Result<()> {
        // remove from local cache first
        self.del_local(rel_path)?;

        let cache_ctl = CacheControl::from(is_pinned);

        // then save to remote
        self.client.put(rel_path, offset, cache_ctl, obj)?;

        // save object to local cache at last and only save when it is
        // a full-put object
        if offset == 0 {
            self.reserve_place(obj.len())?;

            self.backend.insert(rel_path, obj)?;

            // add to lru and increase used size
            self.meta.lru.insert(
                rel_path.to_path_buf(),
                CacheItem::new(obj.len(), is_pinned),
            );
            self.meta.used += obj.len();
        }

        Ok(())
    }

    #[inline]
    pub fn put(
        &mut self,
        rel_path: &Path,
        offset: usize,
        obj: &[u8],
    ) -> Result<()> {
        self.do_put(rel_path, offset, obj, false)
    }

    // put an object and pin it in local cache
    #[inline]
    pub fn put_pinned(&mut self, rel_path: &Path, obj: &[u8]) -> Result<()> {
        self.do_put(rel_path, 0, obj, true)
    }

    // delete object from local cache only
    fn del_local(&mut self, rel_path: &Path) -> Result<()> {
        self.backend.remove(rel_path)?;
        self.is_changed = true;
        if let Some(cache_obj) = self.meta.lru.remove(rel_path) {
            self.meta.used -= cache_obj.len;
        }
        Ok(())
    }

    #[inline]
    pub fn del(&mut self, rel_path: &Path) -> Result<()> {
        // remove from local cache first then remove from remote
        self.del_local(rel_path)
            .and_then(|_| self.client.del(rel_path))
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.is_changed {
            self.save_meta()?;
            self.client.flush()?;
            self.is_changed = false;
        }
        Ok(())
    }
}

impl Default for LocalCache {
    #[inline]
    fn default() -> Self {
        LocalCache {
            meta: CacheMeta::default(),
            backend: Box::new(DummyBackend::default()),
            is_changed: false,
            client: HttpClient::default(),
            crypto: Crypto::default(),
            key: Key::new_empty(),
        }
    }
}

impl Debug for LocalCache {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("LocalCache")
            .field("meta", &self.meta)
            .field("is_changed", &self.is_changed)
            .finish()
    }
}

impl IntoRef for LocalCache {}

pub type LocalCacheRef = Arc<RwLock<LocalCache>>;

#[cfg(test)]
mod tests {
    extern crate tempdir;

    use self::tempdir::TempDir;
    use super::*;
    use base::init_env;

    fn test_local_cache(cache_type: CacheType, base: &Path) {
        init_env();
        let repo_id = "repo456";
        let access_key = "accessKey456";
        let mut cache =
            LocalCache::new(cache_type, 1, base, &repo_id, &access_key)
                .unwrap();

        let k300 = 300 * 1000;
        let k400 = 400 * 1000;
        let k500 = 500 * 1000;
        let rel_path = Path::new("data/aa/bb/k300");
        let mut obj = vec![1u8; k300];
        obj[0] = 0;
        obj[1] = 1;
        obj[2] = 2;
        let rel_path2 = Path::new("data/aa/bb/k400");
        let mut obj2 = vec![2u8; k400];
        obj2[0] = 0;
        obj2[1] = 1;
        obj2[2] = 2;
        let rel_path3 = Path::new("data/aa/bb/k500");
        let mut obj3 = vec![3u8; k500];
        obj3[0] = 0;
        obj3[1] = 1;
        obj3[2] = 2;
        let not_exists = Path::new("not_exists");

        // check if repo exists
        assert!(!cache.repo_exists().unwrap());

        // test init
        cache.connect().unwrap();
        cache.init().unwrap();
        assert_eq!(cache.meta.lru.len(), 0);

        cache.put(&rel_path, 0, &obj).unwrap();
        cache.put(&rel_path2, 0, &obj2).unwrap();
        assert_eq!(cache.meta.lru.len(), 2);
        assert_eq!(cache.meta.used, k300 + k400);

        cache.put(&rel_path3, 0, &obj3).unwrap();
        assert_eq!(cache.meta.lru.len(), 2);
        assert_eq!(cache.meta.used, k400 + k500);

        cache.put(&rel_path3, 0, &obj3).unwrap();
        assert_eq!(cache.meta.lru.len(), 2);
        assert_eq!(cache.meta.used, k400 + k500);

        // should get from remote
        let mut tgt = vec![0u8; obj.len()];
        cache.get_to(&rel_path, 0, &mut tgt).unwrap();
        assert_eq!(&tgt.len(), &obj.len());
        assert_eq!(&tgt[..5], &obj[..5]);
        assert_eq!(cache.meta.lru.len(), 2);
        assert_eq!(cache.meta.used, k500 + k300);

        // should get from local
        let mut tgt = vec![0u8; obj.len()];
        cache.get_to(&rel_path, 0, &mut tgt).unwrap();
        assert_eq!(tgt.len(), obj.len());
        assert_eq!(&tgt[..5], &obj[..5]);
        assert_eq!(cache.meta.lru.len(), 2);
        assert_eq!(cache.meta.used, k500 + k300);

        // get object not exists should fail
        let result = cache.get(&not_exists).unwrap_err();
        assert_eq!(result, Error::NotFound);

        // delete object in local cache
        cache.del(&rel_path).unwrap();
        assert_eq!(cache.meta.lru.len(), 1);

        // delete object again should succeed
        cache.del(&rel_path).unwrap();

        // delete object not in local cache
        cache.del(&rel_path2).unwrap();
        assert_eq!(cache.meta.lru.len(), 1);

        // test flush
        cache.flush().unwrap();
        assert_eq!(cache.meta.lru.len(), 1);

        // re-open local cache with bigger capacity
        drop(cache);
        let mut cache =
            LocalCache::new(cache_type, 2, base, &repo_id, &access_key)
                .unwrap();
        cache.connect().unwrap();
        cache.open().unwrap();

        // delete object not exists should succeed
        cache.del(&not_exists).unwrap();

        // put objects again
        cache.put(&rel_path, 0, &obj).unwrap();
        cache.put(&rel_path2, 0, &obj2).unwrap();
        cache.put(&rel_path3, 0, &obj3).unwrap();
        cache.flush().unwrap();

        // re-open cache with smaller capacity
        drop(cache);
        let mut cache =
            LocalCache::new(cache_type, 1, base, &repo_id, &access_key)
                .unwrap();
        cache.connect().unwrap();
        cache.open().unwrap();
        if cache_type == CacheType::File {
            assert_eq!(cache.meta.lru.len(), 3);
        }

        // put partial object
        cache.put(&rel_path, 50, &obj).unwrap();

        if cache_type == CacheType::File {
            assert_eq!(cache.meta.lru.len(), 2);
        }
    }

    #[test]
    fn local_cache_mem() {
        test_local_cache(CacheType::Mem, Path::new(""));
    }

    #[test]
    fn local_cache_file() {
        let tmpdir = TempDir::new("zbox_test").expect("Create temp dir failed");
        let base = tmpdir.path().to_path_buf();
        //if base.exists() {
        //std::fs::remove_dir_all(&base).unwrap();
        //}
        test_local_cache(CacheType::File, &base);
    }
}
