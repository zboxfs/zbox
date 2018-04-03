use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry;
use std::path::{Path, PathBuf};

use error::Result;
use base::crypto::{Crypto, Key};
use trans::{Eid, Txid};
use volume::storage::space::Space;
use super::{load_obj, remove_dir_all, save_obj};
use super::vio::imp as vio_imp;

// entity map bucket count
const BUCKET_CNT: usize = 4;
const BUCKET_MASK: usize = BUCKET_CNT - 1;

// entity map bucket
type Bucket = HashMap<Eid, Space>;

#[inline]
fn bucket_id(id: &Eid) -> usize {
    id[0] as usize & BUCKET_MASK
}

/// Entity map
#[derive(Debug)]
pub(super) struct Emap {
    buckets: Vec<Bucket>,
    base: PathBuf,
    txid: Txid,
    skey: Key,
    crypto: Crypto,
}

impl Emap {
    const DIR_NAME: &'static str = "emap";

    pub fn new(base: &Path, txid: Txid) -> Self {
        let mut ret = Emap {
            buckets: Vec::new(),
            base: base.to_path_buf(),
            txid,
            skey: Key::new_empty(),
            crypto: Crypto::default(),
        };
        for _ in 0..BUCKET_CNT {
            ret.buckets.push(HashMap::new());
        }
        ret.buckets.shrink_to_fit();
        ret
    }

    pub fn init(&self) -> Result<()> {
        vio_imp::create_dir(self.base.join(Emap::DIR_NAME))?;
        Ok(())
    }

    pub fn set_crypto_key(&mut self, crypto: &Crypto, skey: &Key) {
        self.crypto = crypto.clone();
        self.skey = skey.clone();
    }

    pub fn get(&self, id: &Eid) -> Option<&Space> {
        self.buckets[bucket_id(id)].get(id)
    }

    pub fn entry(&mut self, id: Eid) -> Entry<Eid, Space> {
        self.buckets[bucket_id(&id)].entry(id)
    }

    pub fn remove(&mut self, id: &Eid) -> Option<Space> {
        self.buckets[bucket_id(id)].remove(id)
    }

    pub fn clear(&mut self) {
        for bucket in self.buckets.iter_mut() {
            bucket.clear();
        }
    }

    fn path(base: &Path, txid: Txid) -> PathBuf {
        base.join(Emap::DIR_NAME).join(txid.to_string())
    }

    fn bucket_file_path(&self, txid: Txid, bucket_id: usize) -> PathBuf {
        Emap::path(&self.base, txid)
            .join("bucket")
            .with_extension(bucket_id.to_string())
    }

    fn save_bucket(&self, bucket_id: usize, txid: Txid) -> Result<()> {
        let bucket = &self.buckets[bucket_id];
        let file_path = self.bucket_file_path(txid, bucket_id);
        save_obj(bucket, file_path, &self.skey, &self.crypto)
    }

    fn load_bucket(&self, bucket_id: usize, txid: Txid) -> Result<Bucket> {
        let file_path = self.bucket_file_path(txid, bucket_id);
        load_obj(file_path, &self.skey, &self.crypto)
    }

    fn copy_bucket(&self, bucket_id: usize, txid: Txid) -> Result<()> {
        let src = self.bucket_file_path(self.txid, bucket_id);
        let dst = self.bucket_file_path(txid, bucket_id);
        if src.exists() {
            vio_imp::copy(src, dst)?;
        } else {
            self.save_bucket(bucket_id, txid)?;
        }
        Ok(())
    }

    // merge and save entity map
    pub fn merge(
        &mut self,
        other: &Emap,
        deleted: &HashSet<Eid>,
    ) -> Result<()> {
        let mut changed = [false; BUCKET_CNT];

        // merge new and updated
        for i in 0..BUCKET_CNT {
            for (k, v) in other.buckets[i].iter() {
                self.buckets[i].insert(k.clone(), v.clone());
            }
            if !other.buckets[i].is_empty() {
                changed[i] = true;
            }
        }

        // merge deleted
        for del_id in deleted {
            let bucket_id = bucket_id(del_id);
            self.buckets[bucket_id].remove(del_id);
            changed[bucket_id] = true;
        }

        // save buckets
        vio_imp::create_dir(Emap::path(&self.base, other.txid))?;
        for (bucket_id, is_changed) in changed.into_iter().enumerate() {
            if *is_changed {
                self.save_bucket(bucket_id, other.txid)?;
            } else {
                self.copy_bucket(bucket_id, other.txid)?;
            }
        }

        self.txid = other.txid;

        Ok(())
    }

    pub fn load(&mut self, txid: Txid) -> Result<()> {
        let mut buckets = Vec::new();
        for i in 0..BUCKET_CNT {
            buckets.push(self.load_bucket(i, txid)?);
        }
        self.buckets = buckets;
        self.txid = txid;
        Ok(())
    }

    pub fn cleanup(base: &Path, txid: Txid) -> Result<()> {
        // remove emap folder
        remove_dir_all(Emap::path(base, txid))?;
        Ok(())
    }
}
