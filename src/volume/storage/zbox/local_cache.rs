use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use linked_hash_map::LinkedHashMap;
use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};

use super::http_client::{HttpClient, HttpClientRef};
use super::vio;
use base::crypto::{Crypto, Key};
use base::utils;
use base::IntoRef;
use error::{Error, Result};
use trans::Eid;

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

    #[serde(skip_serializing, skip_deserializing, default)]
    base: PathBuf,

    #[serde(skip_serializing, skip_deserializing, default)]
    client: HttpClientRef,
}

impl CacheArea {
    fn new(
        cache_type: CacheType,
        capacity: usize,
        base: &Path,
        client: &HttpClientRef,
    ) -> CacheArea {
        CacheArea {
            cache_type,
            capacity,
            used: 0,
            lru: LinkedHashMap::new(),
            mem_store: HashMap::new(),
            base: base.to_path_buf(),
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
                        let path = self.base.join(ent.key());
                        if path.exists() {
                            vio::remove_file(&path)
                                .map_err(|err| Error::from(err))
                                .and_then(|_| {
                                    utils::remove_empty_parent_dir(&path)
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
    ) -> Result<usize> {
        utils::ensure_parents_dir(&local_path)?;
        let mut file = vio::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&local_path)?;
        let mut client = self.client.write().unwrap();

        client.get_to(rel_path, &mut file).or_else(|err| {
            // clean the download if it is failed
            drop(file);
            if vio::remove_file(&local_path)
                .map_err(|err| Error::from(err))
                .and_then(|_| utils::remove_empty_parent_dir(&local_path))
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
                    client.get(rel_path)?
                };
                let remote_len = remote.len();
                self.mem_store.insert(rel_path.to_path_buf(), remote);
                self.evict(rel_path, remote_len, is_pinned)?;
            }
            CacheType::File => {
                let path = self.base.join(rel_path);
                if path.exists() {
                    // object is already in cache
                    let result = self.lru.get_refresh(rel_path);
                    assert!(result.is_some());
                    return Ok(());
                }

                // object is not in cache, get it from remote and then add
                // it to cache
                let saved_len = self.download_to_file(&path, rel_path)?;
                self.evict(rel_path, saved_len, is_pinned)?;
            }
        }

        Ok(())
    }

    fn get_exact(
        &mut self,
        rel_path: &Path,
        offset: usize,
        dst: &mut [u8],
        is_pinned: bool,
    ) -> Result<()> {
        // make sure data is already in local cache
        self.ensure_in_local(rel_path, is_pinned)?;

        match self.cache_type {
            CacheType::Mem => {
                let obj = self.mem_store.get(rel_path).unwrap();
                let len = dst.len();
                dst.copy_from_slice(&obj[offset..offset + len]);
            }
            CacheType::File => {
                let path = self.base.join(rel_path);
                let mut file =
                    vio::OpenOptions::new().read(true).open(&path)?;
                file.seek(SeekFrom::Start(offset as u64))?;
                file.read_exact(dst)?;
            }
        }

        Ok(())
    }

    fn get_all(&mut self, rel_path: &Path, is_pinned: bool) -> Result<Vec<u8>> {
        // make sure data is already in local cache
        self.ensure_in_local(rel_path, is_pinned)?;

        match self.cache_type {
            CacheType::Mem => {
                let obj = self.mem_store.get(rel_path).unwrap();
                Ok(obj.to_owned())
            }
            CacheType::File => {
                let path = self.base.join(rel_path);
                let mut ret = Vec::new();
                let mut file =
                    vio::OpenOptions::new().read(true).open(&path)?;
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
                let path = self.base.join(rel_path);
                if path.exists() {
                    let mut ret = Vec::new();
                    let mut file =
                        vio::OpenOptions::new().read(true).open(&path)?;
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
    ) -> Result<()> {
        // save to remote first
        {
            let mut client = self.client.write().unwrap();
            client.put(rel_path, offset, obj)?;
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
                        client.get(rel_path)?
                    } else {
                        obj.to_owned()
                    };
                    self.mem_store.insert(rel_path.to_path_buf(), obj);
                }
            }
            CacheType::File => {
                let path = self.base.join(rel_path);
                if path.exists() {
                    let mut file =
                        vio::OpenOptions::new().write(true).open(&path)?;
                    file.seek(SeekFrom::Start(offset as u64))?;
                    file.write_all(obj)?;
                } else {
                    utils::ensure_parents_dir(&path)?;

                    // if it is a full put, write directly to local
                    if offset == 0 {
                        let mut file = vio::OpenOptions::new()
                            .write(true)
                            .create(true)
                            .open(&path)?;
                        file.write_all(obj)?;
                    } else {
                        // otherwise download and save from remote
                        let saved_len =
                            self.download_to_file(&path, rel_path)?;
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
                let path = self.base.join(rel_path);
                utils::ensure_parents_dir(&path)?;
                let mut file = vio::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&path)?;
                file.write_all(obj)?;
            }
        }

        self.evict(rel_path, obj.len(), true)
    }

    fn del(&mut self, rel_path: &Path) -> Result<()> {
        // remove from remote first
        let mut client = self.client.write().unwrap();
        client.del(rel_path)?;

        match self.cache_type {
            CacheType::Mem => {
                self.mem_store.remove(rel_path);
            }
            CacheType::File => {
                let path = self.base.join(rel_path);
                if path.exists() {
                    vio::remove_file(&path)?;
                    utils::remove_empty_parent_dir(&path)?;
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
                for ent_result in vio::read_dir(&self.base)? {
                    let ent = ent_result?;
                    let path = ent.path();
                    let file_type = ent.file_type()?;
                    if file_type.is_dir() {
                        vio::remove_dir_all(&path)?;
                    } else if file_type.is_file() {
                        vio::remove_file(&path)?;
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
            .field("lru", &self.lru)
            .field("base", &self.base)
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

    // is meta file saved flag
    is_saved: bool,

    // main cache area
    cache: CacheArea,

    // address cache area
    addr_cache: CacheArea,

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

    // address cache dir
    const ADDR_CACHE_DIR: &'static str = "index";

    // fixed reserved address cache sizes
    const ADDR_CACHE_SIZE: usize = 16 * 1024;

    pub fn new(
        cache_type: CacheType,
        capacity_in_mb: usize,
        base: &Path,
        repo_id: &str,
        access_key: &str,
    ) -> Result<Self> {
        let capacity = capacity_in_mb * 1024 * 1024; // capacity is in MB
        let cache_cap = capacity - Self::ADDR_CACHE_SIZE;
        let addr_base = base.join(Self::ADDR_CACHE_DIR);
        let client = HttpClient::new(repo_id, access_key)?.into_ref();
        let cache = CacheArea::new(cache_type, cache_cap, base, &client);
        let addr_cache = CacheArea::new(
            cache_type,
            Self::ADDR_CACHE_SIZE,
            &addr_base,
            &client,
        );

        Ok(LocalCache {
            cache_type,
            capacity,
            update_seq: 0,
            is_saved: false,
            cache,
            addr_cache,
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

        // load local cache meta file
        let buf = self.cache.get_local(Path::new(Self::CACHE_META_FILE))?;
        let buf = self.crypto.decrypt(&buf, &self.key)?;
        let mut de = Deserializer::new(&buf[..]);
        let mut local: Self = Deserialize::deserialize(&mut de)?;

        // if remote has been changed, only invalidate local addr cache
        if local.update_seq != self.update_seq {
            debug!("remote repo is changed, invalidate address cache");
            local.addr_cache.clear()?;
        }

        self.cache.used = local.cache.used;
        self.cache.lru = local.cache.lru;
        self.addr_cache.used = local.addr_cache.used;
        self.addr_cache.lru = local.addr_cache.lru;

        Ok(())
    }

    #[inline]
    pub fn get_address(&mut self, id: &Eid) -> Result<Vec<u8>> {
        let rel_path = Path::new(Self::ADDR_CACHE_DIR).join(id.to_rel_path());
        self.addr_cache.get_all(&rel_path, false)
    }

    #[inline]
    pub fn get(
        &mut self,
        rel_path: &Path,
        offset: usize,
        dst: &mut [u8],
    ) -> Result<()> {
        self.cache.get_exact(rel_path, offset, dst, false)
    }

    #[inline]
    pub fn get_pinned(&mut self, rel_path: &Path) -> Result<Vec<u8>> {
        self.cache.get_all(rel_path, true)
    }

    pub fn put_address(&mut self, id: &Eid, addr: &[u8]) -> Result<()> {
        let rel_path = Path::new(Self::ADDR_CACHE_DIR).join(id.to_rel_path());
        self.addr_cache.insert(&rel_path, 0, addr, false)?;
        self.is_saved = false;
        Ok(())
    }

    pub fn put(
        &mut self,
        rel_path: &Path,
        offset: usize,
        obj: &[u8],
    ) -> Result<()> {
        self.cache.insert(rel_path, offset, obj, false)?;
        self.is_saved = false;
        Ok(())
    }

    pub fn put_pinned(&mut self, rel_path: &Path, obj: &[u8]) -> Result<()> {
        self.cache.insert(rel_path, 0, obj, true)?;
        self.is_saved = false;
        Ok(())
    }

    pub fn del_address(&mut self, id: &Eid) -> Result<()> {
        let rel_path = Path::new(Self::ADDR_CACHE_DIR).join(id.to_rel_path());
        self.addr_cache.del(&rel_path)?;
        self.is_saved = false;
        Ok(())
    }

    pub fn del(&mut self, rel_path: &Path) -> Result<()> {
        self.cache.del(rel_path)?;
        self.is_saved = false;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        if !self.is_saved {
            self.write_meta()?;
            self.is_saved = true;
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
            .field("is_saved", &self.is_saved)
            .field("cache", &self.cache)
            .field("addr_cache", &self.addr_cache)
            .finish()
    }
}

#[cfg(test)]
mod tests {

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
        let rel_path = Path::new("data/aa/bb/aabb111");
        let mut obj = vec![1u8; k300];
        obj[0] = 0;
        obj[1] = 1;
        obj[2] = 2;
        let rel_path2 = Path::new("data/aa/bb/aabb222");
        let mut obj2 = vec![2u8; k400];
        obj2[0] = 0;
        obj2[1] = 1;
        obj2[2] = 2;
        let rel_path3 = Path::new("data/aa/bb/aabb333");
        let mut obj3 = vec![3u8; k500];
        obj3[0] = 0;
        obj3[1] = 1;
        obj3[2] = 2;
        let not_exists = Path::new("not_exists");

        // check if repo exists
        assert!(cache.repo_exists().unwrap());

        // test init
        cache.init().unwrap();
        assert_eq!(cache.cache.lru.len(), 1);
        let meta_len = cache.cache.used;

        cache.put(&rel_path, 0, &obj).unwrap();
        cache.put(&rel_path2, 0, &obj2).unwrap();
        cache.put(&rel_path3, 0, &obj3).unwrap();
        cache.put(&rel_path3, 0, &obj3).unwrap();
        assert_eq!(cache.cache.lru.len(), 3);
        assert_eq!(cache.cache.used, meta_len + k400 + k500);

        // should get from remote
        let mut tgt = vec![0u8; obj.len()];
        cache.get(&rel_path, 0, &mut tgt).unwrap();
        assert_eq!(&tgt.len(), &obj.len());
        assert_eq!(&tgt[..5], &obj[..5]);
        assert_eq!(cache.cache.lru.len(), 3);
        assert_eq!(cache.cache.used, meta_len + k500 + k300);

        // should get from local
        let mut tgt = vec![0u8; obj3.len()];
        cache.get(&rel_path3, 0, &mut tgt).unwrap();
        assert_eq!(tgt.len(), obj3.len());
        assert_eq!(&tgt[..5], &obj3[..5]);
        assert_eq!(cache.cache.lru.len(), 3);
        assert_eq!(cache.cache.used, meta_len + k500 + k300);

        // get object not exists should fail
        let result = cache.get(&not_exists, 0, &mut tgt).unwrap_err();
        assert_eq!(result, Error::NotFound);

        // delete object in local cache
        cache.del(&rel_path).unwrap();
        assert_eq!(cache.cache.lru.len(), 2);
        assert_eq!(cache.cache.used, meta_len + k500);

        // delete object again should succeed
        cache.del(&rel_path).unwrap();

        // delete object not in local cache
        cache.del(&rel_path2).unwrap();
        assert_eq!(cache.cache.lru.len(), 2);
        assert_eq!(cache.cache.used, meta_len + k500);

        // test flush
        cache.flush().unwrap();
        assert_eq!(cache.cache.lru.len(), 2);
        assert!(cache.cache.used > k500);

        // re-open local cache with bigger capacity
        drop(cache);
        let mut cache =
            LocalCache::new(cache_type, 2, base, &repo_id, &access_key)
                .unwrap();
        cache.open().unwrap();

        // delete last object in local cache
        cache.del(&rel_path3).unwrap();

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
        cache.open().unwrap();
        if cache_type == CacheType::File {
            assert_eq!(cache.cache.lru.len(), 4);
        }

        // put partial object
        cache.put(&rel_path, 50, &obj).unwrap();
        if cache_type == CacheType::File {
            assert_eq!(cache.cache.lru.len(), 3);
        }
    }

    #[test]
    fn local_cache_mem() {
        test_local_cache(CacheType::Mem, Path::new(""));
    }

    #[test]
    fn local_cache_file() {
        let base = Path::new("./tt");
        if base.exists() {
            vio::remove_dir_all(&base).unwrap();
        } else {
            vio::create_dir(&base).unwrap();
        }
        test_local_cache(CacheType::File, &base);
    }
}
