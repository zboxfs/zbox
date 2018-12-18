use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::path::{Path, PathBuf};

use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};

use super::local_cache::LocalCache;
use base::crypto::{Crypto, HashKey, Key};
use error::{Error, Result};
use trans::Eid;

// get bucket relative path from bucket id, 8 buckets in total
#[inline]
fn bucket_rel_path(bucket_id: u8) -> PathBuf {
    PathBuf::from(format!("index/{:02x}", bucket_id))
}

#[derive(Clone, Deserialize, Serialize)]
struct Bucket {
    id: u8,

    // address map, key: entity id, value: address
    map: HashMap<Eid, Vec<u8>>,

    #[serde(skip_serializing, skip_deserializing, default)]
    is_changed: bool,
}

impl Bucket {
    fn new(id: u8) -> Self {
        Bucket {
            id,
            map: HashMap::new(),
            is_changed: false,
        }
    }

    #[inline]
    fn get(&self, k: &Eid) -> Option<&Vec<u8>> {
        self.map.get(k)
    }

    #[inline]
    fn insert(&mut self, k: Eid, v: Vec<u8>) -> Option<Vec<u8>> {
        self.is_changed = true;
        self.map.insert(k, v)
    }

    #[inline]
    fn remove(&mut self, k: &Eid) -> Option<Vec<u8>> {
        match self.map.remove(k) {
            Some(v) => {
                self.is_changed = true;
                Some(v)
            }
            None => None,
        }
    }

    fn load(
        rel_path: &Path,
        crypto: &Crypto,
        key: &Key,
        local_cache: &mut LocalCache,
    ) -> Result<Self> {
        let buf = local_cache.get_index(rel_path)?;
        let buf = crypto.decrypt(&buf, key)?;
        let mut de = Deserializer::new(&buf[..]);
        let ret: Self = Deserialize::deserialize(&mut de)?;
        Ok(ret)
    }

    fn save(
        &mut self,
        rel_path: &Path,
        crypto: &Crypto,
        key: &Key,
        local_cache: &mut LocalCache,
    ) -> Result<()> {
        let mut buf = Vec::new();
        self.serialize(&mut Serializer::new(&mut buf)).unwrap();
        let buf = crypto.encrypt(&buf, key)?;
        local_cache.put_index(rel_path, &buf)?;
        self.is_changed = false;
        Ok(())
    }
}

impl Debug for Bucket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Bucket")
            .field("id", &self.id)
            .field("map.len", &self.map.len())
            .field("is_changed", &self.is_changed)
            .finish()
    }
}

// index manager
pub struct IndexMgr {
    buckets: HashMap<u8, Bucket>,

    crypto: Crypto,
    key: Key,
    hash_key: HashKey,
}

impl IndexMgr {
    // number of buckets
    const BUCKET_NUM: u8 = 8;

    pub fn new() -> Self {
        IndexMgr {
            buckets: HashMap::new(),
            crypto: Crypto::default(),
            key: Key::new_empty(),
            hash_key: HashKey::new_empty(),
        }
    }

    pub fn set_crypto_ctx(&mut self, crypto: Crypto, key: Key) {
        self.crypto = crypto;
        self.hash_key = key.derive(0);
        self.key = key;
    }

    // ensure bucket is loaded
    fn load_bucket(
        &mut self,
        id: &Eid,
        local_cache: &mut LocalCache,
    ) -> Result<&mut Bucket> {
        let bucket_id = id[0] % Self::BUCKET_NUM;

        // if bucket is not loaded, load it from local cache
        if !self.buckets.contains_key(&bucket_id) {
            let rel_path = bucket_rel_path(bucket_id);
            match Bucket::load(&rel_path, &self.crypto, &self.key, local_cache)
            {
                Ok(bucket) => {
                    self.buckets.insert(bucket_id, bucket);
                }
                Err(ref err) if *err == Error::NotFound => {
                    // if bucket doesn't created yet
                    let bucket = Bucket::new(bucket_id);
                    self.buckets.insert(bucket_id, bucket);
                }
                Err(err) => {
                    return Err(err);
                }
            }
        }

        let bucket = self.buckets.get_mut(&bucket_id).unwrap();

        Ok(bucket)
    }

    pub fn get_address(
        &mut self,
        id: &Eid,
        local_cache: &mut LocalCache,
    ) -> Result<Vec<u8>> {
        let bucket = self.load_bucket(id, local_cache)?;
        bucket
            .get(id)
            .map(|addr| addr.to_owned())
            .ok_or(Error::NotFound)
    }

    pub fn put_address(
        &mut self,
        id: &Eid,
        addr: &[u8],
        local_cache: &mut LocalCache,
    ) -> Result<()> {
        let bucket = self.load_bucket(id, local_cache)?;
        bucket.insert(id.clone(), addr.to_vec());
        Ok(())
    }

    pub fn del_address(
        &mut self,
        id: &Eid,
        local_cache: &mut LocalCache,
    ) -> Result<()> {
        let bucket = self.load_bucket(id, local_cache)?;
        bucket.remove(id);
        Ok(())
    }

    pub fn flush(&mut self, local_cache: &mut LocalCache) -> Result<()> {
        for (bucket_id, bucket) in self.buckets.iter_mut() {
            if bucket.is_changed {
                let rel_path = bucket_rel_path(*bucket_id);
                bucket.save(&rel_path, &self.crypto, &self.key, local_cache)?;
            }
        }
        Ok(())
    }
}

impl Debug for IndexMgr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("IndexMgr")
            .field("buckets.len", &self.buckets.len())
            .finish()
    }
}
