use std::cmp::min;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, RwLock};

use linked_hash_map::LinkedHashMap;
use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};

use super::http_client::{CacheControl, HttpClient, HttpClientRef};
use super::vio;
use base::crypto::{Crypto, Key};
use base::utils;
use base::IntoRef;
use error::{Error, Result};

// local cache type
#[derive(Debug, Copy, Clone, PartialEq, Deserialize, Serialize)]
pub enum CacheType {
    Mem,
    File,
}

impl FromStr for CacheType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        if s == "mem" {
            Ok(CacheType::Mem)
        } else if s == "file" {
            Ok(CacheType::File)
        } else {
            Err(Error::InvalidUri)
        }
    }
}

impl Default for CacheType {
    fn default() -> Self {
        CacheType::Mem
    }
}

// object in local cache
#[derive(Debug, Clone, Deserialize, Serialize)]
struct CacheObject {
    len: usize,
    is_pinned: bool,
}

impl CacheObject {
    #[inline]
    fn new(len: usize, is_pinned: bool) -> Self {
        CacheObject { len, is_pinned }
    }
}

#[derive(Default, Deserialize, Serialize)]
struct CacheArea {
    cache_type: CacheType,
    capacity: usize,
    used: usize,
    lru: LinkedHashMap<PathBuf, CacheObject>,

    #[serde(skip_serializing, skip_deserializing, default)]
    mem_store: HashMap<PathBuf, Vec<u8>>,

    // local base dir, for file cache only
    #[serde(skip_serializing, skip_deserializing, default)]
    local_base: PathBuf,

    #[serde(skip_serializing, skip_deserializing, default)]
    client: HttpClientRef,
}

impl CacheArea {
    fn new(
        cache_type: CacheType,
        capacity: usize,
        local_base: &Path,
        client: &HttpClientRef,
    ) -> CacheArea {
        CacheArea {
            cache_type,
            capacity,
            used: 0,
            lru: LinkedHashMap::new(),
            mem_store: HashMap::new(),
            local_base: local_base.to_path_buf(),
            client: client.clone(),
        }
    }

    fn evict(
        &mut self,
        rel_path: &Path,
        obj_len: usize,
        is_pinned: bool,
    ) -> Result<()> {
        // add cache object to lru
        match self.lru.insert(
            rel_path.to_path_buf(),
            CacheObject::new(obj_len, is_pinned),
        ) {
            Some(old_obj) => {
                let delta = obj_len as isize - old_obj.len as isize;
                self.used = (self.used as isize + delta) as usize;
            }
            None => {
                self.used += obj_len;
            }
        }

        // evict least used objects if necessary
        while self.used > self.capacity {
            if let Some((_, ent)) = self
                .lru
                .entries()
                .enumerate()
                .find(|&(_, ref ent)| !ent.get().is_pinned)
            {
                match self.cache_type {
                    CacheType::Mem => {
                        self.mem_store.remove(ent.key());
                    }
                    CacheType::File => {
                        let local_path = self.local_base.join(ent.key());
                        if local_path.exists() {
                            vio::remove_file(&local_path)
                                .map_err(Error::from)
                                .and_then(|_| {
                                    utils::remove_empty_parent_dir(&local_path)
                                })?;
                        }
                    }
                }

                let evicted = ent.remove();
                self.used -= evicted.len;

                continue;
            }
            break;
        }

        Ok(())
    }

    // download and save to local file, return number of bytes saved
    fn download_to_file(
        &mut self,
        local_path: &Path,
        rel_path: &Path,
        cache_ctl: CacheControl,
    ) -> Result<usize> {
        utils::ensure_parents_dir(local_path)?;
        let mut file = vio::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(local_path)?;
        let mut client = self.client.write().unwrap();

        client
            .get_to(rel_path, &mut file, cache_ctl)
            .or_else(|err| {
                // clean the download if it is failed
                drop(file);
                if vio::remove_file(local_path)
                    .map_err(|err| Error::from(err))
                    .and_then(|_| utils::remove_empty_parent_dir(local_path))
                    .is_err()
                {
                    warn!("clean uncompleted download failed");
                }
                Err(err)
            })
    }

    fn ensure_in_local(
        &mut self,
        rel_path: &Path,
        is_pinned: bool,
        cache_ctl: CacheControl,
    ) -> Result<()> {
        match self.cache_type {
            CacheType::Mem => {
                if self.mem_store.contains_key(rel_path) {
                    // object is already in cache
                    self.lru.get_refresh(rel_path);
                    return Ok(());
                }

                // object is not in cache, get it from remote and then add
                // it to cache
                let remote = {
                    let mut client = self.client.write().unwrap();
                    client.get(rel_path, cache_ctl)?
                };
                let remote_len = remote.len();
                self.mem_store.insert(rel_path.to_path_buf(), remote);
                self.evict(rel_path, remote_len, is_pinned)
            }
            CacheType::File => {
                let local_path = self.local_base.join(rel_path);
                if local_path.exists() {
                    // object is already in cache
                    self.lru.get_refresh(rel_path);
                    Ok(())
                } else {
                    // object is not in cache, get it from remote and then add
                    // it to cache
                    let saved_len = self.download_to_file(
                        &local_path,
                        rel_path,
                        cache_ctl,
                    )?;
                    self.evict(rel_path, saved_len, is_pinned)
                }
            }
        }
    }

    fn get_exact(
        &mut self,
        rel_path: &Path,
        offset: usize,
        dst: &mut [u8],
        is_pinned: bool,
        cache_ctl: CacheControl,
    ) -> Result<()> {
        // make sure data is already in local cache
        self.ensure_in_local(rel_path, is_pinned, cache_ctl)?;

        match self.cache_type {
            CacheType::Mem => {
                let obj = self.mem_store.get(rel_path).unwrap();
                let len = dst.len();
                dst.copy_from_slice(&obj[offset..offset + len]);
            }
            CacheType::File => {
                let local_path = self.local_base.join(rel_path);
                let mut file =
                    vio::OpenOptions::new().read(true).open(&local_path)?;
                file.seek(SeekFrom::Start(offset as u64))?;
                file.read_exact(dst)?;
            }
        }

        Ok(())
    }

    fn get_all(
        &mut self,
        rel_path: &Path,
        is_pinned: bool,
        cache_ctl: CacheControl,
    ) -> Result<Vec<u8>> {
        // make sure data is already in local cache
        self.ensure_in_local(rel_path, is_pinned, cache_ctl)?;

        match self.cache_type {
            CacheType::Mem => {
                let obj = self.mem_store.get(rel_path).unwrap();
                Ok(obj.to_owned())
            }
            CacheType::File => {
                let local_path = self.local_base.join(rel_path);
                let mut ret = Vec::new();
                let mut file =
                    vio::OpenOptions::new().read(true).open(&local_path)?;
                file.read_to_end(&mut ret)?;
                Ok(ret)
            }
        }
    }

    fn get_local(&mut self, rel_path: &Path) -> Result<Vec<u8>> {
        match self.cache_type {
            CacheType::Mem => self
                .mem_store
                .get(rel_path)
                .map(|v| v.to_owned())
                .ok_or(Error::NotFound),
            CacheType::File => {
                let local_path = self.local_base.join(rel_path);
                if local_path.exists() {
                    let mut ret = Vec::new();
                    let mut file =
                        vio::OpenOptions::new().read(true).open(&local_path)?;
                    file.read_to_end(&mut ret)?;
                    Ok(ret)
                } else {
                    Err(Error::NotFound)
                }
            }
        }
    }

    fn insert(
        &mut self,
        rel_path: &Path,
        offset: usize,
        obj: &[u8],
        is_pinned: bool,
        cache_ctl: CacheControl,
    ) -> Result<()> {
        // save to remote first
        {
            let mut client = self.client.write().unwrap();
            client.put(rel_path, offset, cache_ctl, obj)?;
        }

        // save object to local
        match self.cache_type {
            CacheType::Mem => {
                if self.mem_store.contains_key(rel_path) {
                    let existing = self.mem_store.get_mut(rel_path).unwrap();
                    existing.truncate(offset);
                    existing.extend_from_slice(obj);
                } else {
                    let obj = if offset > 0 {
                        let mut client = self.client.write().unwrap();
                        client.get(rel_path, cache_ctl)?
                    } else {
                        obj.to_owned()
                    };
                    self.mem_store.insert(rel_path.to_path_buf(), obj);
                }
            }
            CacheType::File => {
                let local_path = self.local_base.join(rel_path);
                if local_path.exists() {
                    let mut file = vio::OpenOptions::new()
                        .write(true)
                        .open(&local_path)?;
                    file.set_len(offset as u64)?;
                    file.seek(SeekFrom::Start(offset as u64))?;
                    file.write_all(obj)?;
                } else {
                    utils::ensure_parents_dir(&local_path)?;

                    // if it is a full put, write directly to local
                    if offset == 0 {
                        let mut file = vio::OpenOptions::new()
                            .write(true)
                            .create(true)
                            .open(&local_path)?;
                        file.write_all(obj)?;
                    } else {
                        // otherwise download and save from remote
                        let saved_len = self.download_to_file(
                            &local_path,
                            rel_path,
                            cache_ctl,
                        )?;
                        assert_eq!(saved_len, offset + obj.len());
                    }
                }
            }
        }

        self.evict(rel_path, offset + obj.len(), is_pinned)
    }

    fn insert_local(&mut self, rel_path: &Path, obj: &[u8]) -> Result<()> {
        match self.cache_type {
            CacheType::Mem => {
                self.mem_store
                    .insert(rel_path.to_path_buf(), obj.to_owned());
            }
            CacheType::File => {
                let local_path = self.local_base.join(rel_path);
                utils::ensure_parents_dir(&local_path)?;
                let mut file = vio::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&local_path)?;
                file.write_all(obj)?;
            }
        }

        self.evict(rel_path, obj.len(), true)
    }

    fn del(&mut self, rel_path: &Path, cache_ctl: CacheControl) -> Result<()> {
        // remove from remote first
        let mut client = self.client.write().unwrap();
        client.del(rel_path, cache_ctl)?;

        match self.cache_type {
            CacheType::Mem => {
                self.mem_store.remove(rel_path);
            }
            CacheType::File => {
                let local_path = self.local_base.join(rel_path);
                if local_path.exists() {
                    vio::remove_file(&local_path)?;
                    utils::remove_empty_parent_dir(&local_path)?;
                }
            }
        }

        if let Some(cache_obj) = self.lru.remove(rel_path) {
            self.used -= cache_obj.len;
        }

        Ok(())
    }

    fn clear(&mut self) -> Result<()> {
        match self.cache_type {
            CacheType::Mem => {
                self.mem_store.clear();
            }
            CacheType::File => {
                for rel_path in self.lru.keys() {
                    let local_path = self.local_base.join(rel_path);
                    if local_path.exists() {
                        vio::remove_file(&local_path)?;
                    }
                }
            }
        }

        self.lru.clear();
        self.used = 0;

        Ok(())
    }
}

impl Debug for CacheArea {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("CacheArea")
            .field("cache_type", &self.cache_type)
            .field("capacity", &self.capacity)
            .field("used", &self.used)
            .field("local_base", &self.local_base)
            .field("lru", &self.lru)
            .finish()
    }
}

/// Local cache
#[derive(Default, Deserialize, Serialize)]
pub struct LocalCache {
    cache_type: CacheType,

    // local cache capacity, in bytes
    capacity: usize,

    // repo update sequence
    update_seq: u64,

    // is meta changed flag
    is_changed: bool,

    // main cache area
    cache: CacheArea,

    // index cache area
    idx_cache: CacheArea,

    #[serde(skip_serializing, skip_deserializing, default)]
    client: HttpClientRef,

    #[serde(skip_serializing, skip_deserializing, default)]
    crypto: Crypto,

    #[serde(skip_serializing, skip_deserializing, default)]
    key: Key,
}

impl LocalCache {
    // local cache persistent file name
    const CACHE_META_FILE: &'static str = "cache_meta";

    // index cache capacity range, in bytes
    const MIN_IDX_CACHE_CAP: usize = 256 * 1024;
    const MAX_IDX_CACHE_CAP: usize = 16 * 1024 * 1024;

    pub fn new(
        cache_type: CacheType,
        capacity_in_mb: usize,
        base: &Path,
        repo_id: &str,
        access_key: &str,
    ) -> Result<Self> {
        let capacity = capacity_in_mb * 1024 * 1024; // capacity is in MB

        let idx_cache_cap = min(
            (capacity_in_mb / 4 + 1) * Self::MIN_IDX_CACHE_CAP,
            Self::MAX_IDX_CACHE_CAP,
        );
        let cache_cap = capacity - idx_cache_cap;

        let client = HttpClient::new(repo_id, access_key)?.into_ref();
        let cache = CacheArea::new(cache_type, cache_cap, base, &client);
        let idx_cache =
            CacheArea::new(cache_type, idx_cache_cap, base, &client);

        Ok(LocalCache {
            cache_type,
            capacity,
            update_seq: 0,
            is_changed: false,
            cache,
            idx_cache,
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

    pub fn repo_exists(&self) -> Result<bool> {
        let client = self.client.read().unwrap();
        client.repo_exists()
    }

    fn write_meta(&mut self) -> Result<()> {
        // get update sequence
        {
            let client = self.client.read().unwrap();
            self.update_seq = client.get_update_seq();
        }

        // serialize and write to local
        let mut buf = Vec::new();
        self.serialize(&mut Serializer::new(&mut buf))?;
        let buf = self.crypto.encrypt(&buf, &self.key)?;
        self.cache
            .insert_local(Path::new(Self::CACHE_META_FILE), &buf)
    }

    // connect to remote repo and open session
    pub fn connect(&mut self) -> Result<()> {
        let mut client = self.client.write().unwrap();
        self.update_seq = client.open_session()?;
        Ok(())
    }

    #[inline]
    pub fn init(&mut self) -> Result<()> {
        self.write_meta()
    }

    pub fn open(&mut self) -> Result<()> {
        if self.cache_type == CacheType::Mem {
            return Ok(());
        }

        // try to load local cache meta file
        match self.cache.get_local(Path::new(Self::CACHE_META_FILE)) {
            Ok(buf) => {
                let buf = self.crypto.decrypt(&buf, &self.key)?;
                let mut de = Deserializer::new(&buf[..]);
                let mut local: Self = Deserialize::deserialize(&mut de)?;

                self.cache.used = local.cache.used;
                self.cache.lru = local.cache.lru;
                self.idx_cache.used = local.idx_cache.used;
                self.idx_cache.lru = local.idx_cache.lru;

                if local.update_seq == self.update_seq {
                    return Ok(());
                }

                // if remote has been changed, invalidate local cache
                warn!(
                    "remote repo is changed, local: {}, remote: {}, \
                     invalidate local cache",
                    local.update_seq, self.update_seq
                );

                // clear whole index cache and pinned objects in data cache,
                // and then save the meta file
                self.idx_cache.clear()?;
                self.cache.clear()?;
                self.write_meta()
            }
            Err(ref err) if *err == Error::NotFound => {
                // if meta file doesn't exist, set up a new local cache
                self.write_meta()
            }
            Err(err) => Err(err),
        }
    }

    #[inline]
    pub fn get_index(&mut self, rel_path: &Path) -> Result<Vec<u8>> {
        self.idx_cache
            .get_all(&rel_path, false, CacheControl::NoCache)
    }

    #[inline]
    pub fn get(
        &mut self,
        rel_path: &Path,
        offset: usize,
        dst: &mut [u8],
    ) -> Result<()> {
        self.cache
            .get_exact(rel_path, offset, dst, false, CacheControl::Long)
    }

    #[inline]
    pub fn get_pinned(&mut self, rel_path: &Path) -> Result<Vec<u8>> {
        self.cache.get_all(rel_path, true, CacheControl::NoCache)
    }

    pub fn put_index(&mut self, rel_path: &Path, index: &[u8]) -> Result<()> {
        self.idx_cache.insert(
            &rel_path,
            0,
            index,
            false,
            CacheControl::NoCache,
        )?;
        self.is_changed = true;
        Ok(())
    }

    pub fn put(
        &mut self,
        rel_path: &Path,
        offset: usize,
        obj: &[u8],
    ) -> Result<()> {
        self.cache
            .insert(rel_path, offset, obj, false, CacheControl::Long)?;
        self.is_changed = true;
        Ok(())
    }

    #[inline]
    pub fn put_pinned(&mut self, rel_path: &Path, obj: &[u8]) -> Result<()> {
        self.cache
            .insert(rel_path, 0, obj, true, CacheControl::NoCache)?;
        self.is_changed = true;
        Ok(())
    }

    #[inline]
    pub fn del(&mut self, rel_path: &Path) -> Result<()> {
        self.cache.del(rel_path, CacheControl::Long)?;
        self.is_changed = true;
        Ok(())
    }

    #[inline]
    pub fn del_pinned(&mut self, rel_path: &Path) -> Result<()> {
        self.cache.del(rel_path, CacheControl::NoCache)?;
        self.is_changed = true;
        Ok(())
    }

    #[inline]
    pub fn del_index(&mut self, rel_path: &Path) -> Result<()> {
        self.idx_cache.del(rel_path, CacheControl::NoCache)?;
        self.is_changed = true;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.is_changed {
            self.write_meta()?;
            self.is_changed = false;
        }
        Ok(())
    }
}

impl Debug for LocalCache {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("LocalCache")
            .field("cache_type", &self.cache_type)
            .field("capacity", &self.capacity)
            .field("update_seq", &self.update_seq)
            .field("is_changed", &self.is_changed)
            .field("cache", &self.cache)
            .field("idx_cache", &self.idx_cache)
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
        let rel_path = Path::new("k300");
        let mut obj = vec![1u8; k300];
        obj[0] = 0;
        obj[1] = 1;
        obj[2] = 2;
        let rel_path2 = Path::new("k400");
        let mut obj2 = vec![2u8; k400];
        obj2[0] = 0;
        obj2[1] = 1;
        obj2[2] = 2;
        let rel_path3 = Path::new("k500");
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
        assert_eq!(cache.cache.lru.len(), 1);
        let meta_len = cache.cache.used;

        cache.put(&rel_path, 0, &obj).unwrap();
        cache.put(&rel_path2, 0, &obj2).unwrap();
        assert_eq!(cache.cache.lru.len(), 3);
        assert_eq!(cache.cache.used, meta_len + k300 + k400);

        cache.put(&rel_path3, 0, &obj3).unwrap();
        assert_eq!(cache.cache.lru.len(), 2);
        assert_eq!(cache.cache.used, meta_len + k500);

        cache.put(&rel_path3, 0, &obj3).unwrap();
        assert_eq!(cache.cache.lru.len(), 2);
        assert_eq!(cache.cache.used, meta_len + k500);

        // should get from remote
        let mut tgt = vec![0u8; obj.len()];
        cache.get(&rel_path, 0, &mut tgt).unwrap();
        assert_eq!(&tgt.len(), &obj.len());
        assert_eq!(&tgt[..5], &obj[..5]);
        assert_eq!(cache.cache.lru.len(), 2);
        assert_eq!(cache.cache.used, meta_len + k300);

        // should get from local
        let mut tgt = vec![0u8; obj.len()];
        cache.get(&rel_path, 0, &mut tgt).unwrap();
        assert_eq!(tgt.len(), obj.len());
        assert_eq!(&tgt[..5], &obj[..5]);
        assert_eq!(cache.cache.lru.len(), 2);
        assert_eq!(cache.cache.used, meta_len + k300);

        // get object not exists should fail
        let result = cache.get(&not_exists, 0, &mut tgt).unwrap_err();
        assert_eq!(result, Error::NotFound);

        // delete object in local cache
        cache.del(&rel_path).unwrap();
        assert_eq!(cache.cache.lru.len(), 1);
        assert_eq!(cache.cache.used, meta_len);

        // delete object again should succeed
        cache.del(&rel_path).unwrap();

        // delete object not in local cache
        cache.del(&rel_path2).unwrap();
        assert_eq!(cache.cache.lru.len(), 1);
        assert_eq!(cache.cache.used, meta_len);

        // test flush
        cache.flush().unwrap();
        assert_eq!(cache.cache.lru.len(), 1);

        // re-open local cache with bigger capacity
        drop(cache);
        let mut cache =
            LocalCache::new(cache_type, 2, base, &repo_id, &access_key)
                .unwrap();
        cache.connect().unwrap();
        cache.open().unwrap();

        // delete object not exists should succeed
        cache.del(&not_exists).unwrap();

        // test cache clear
        cache.cache.clear().unwrap();

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
            assert_eq!(cache.cache.lru.len(), 3);
        }

        // put partial object
        cache.put(&rel_path, 50, &obj).unwrap();
        if cache_type == CacheType::File {
            assert_eq!(cache.cache.lru.len(), 1);
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
        if base.exists() {
            std::fs::remove_dir_all(&base).unwrap();
        }
        test_local_cache(CacheType::File, &base);
    }
}
