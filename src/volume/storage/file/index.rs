use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::path::Path;

use bytes::BufMut;

use super::file_armor::FileArmor;
use base::crypto::{Crypto, HashKey, Key};
use error::{Error, Result};
use trans::{Eid, Id};
use volume::{Arm, ArmAccess, Armor, Seq};

// entity address bucket
#[derive(Clone, Deserialize, Serialize)]
struct Bucket {
    id: Eid,
    seq: u64,
    arm: Arm,
    map: HashMap<Eid, Vec<u8>>,

    #[serde(skip_serializing, skip_deserializing, default)]
    is_changed: bool,
}

impl Bucket {
    #[inline]
    fn new(id: Eid) -> Self {
        Bucket {
            id,
            seq: 0,
            arm: Arm::default(),
            map: HashMap::new(),
            is_changed: false,
        }
    }

    #[inline]
    fn get(&self, id: &Eid) -> Option<&Vec<u8>> {
        self.map.get(id)
    }

    #[inline]
    fn insert(&mut self, id: Eid, addr: Vec<u8>) -> Option<Vec<u8>> {
        self.is_changed = true;
        self.map.insert(id, addr)
    }

    #[inline]
    fn remove(&mut self, id: &Eid) -> Option<Vec<u8>> {
        match self.map.remove(id) {
            Some(addr) => {
                self.is_changed = true;
                Some(addr)
            }
            None => None,
        }
    }
}

impl Id for Bucket {
    #[inline]
    fn id(&self) -> &Eid {
        &self.id
    }

    #[inline]
    fn id_mut(&mut self) -> &mut Eid {
        &mut self.id
    }
}

impl Seq for Bucket {
    #[inline]
    fn seq(&self) -> u64 {
        self.seq
    }

    #[inline]
    fn inc_seq(&mut self) {
        self.seq += 1
    }
}

impl<'de> ArmAccess<'de> for Bucket {
    #[inline]
    fn arm(&self) -> Arm {
        self.arm
    }

    #[inline]
    fn arm_mut(&mut self) -> &mut Arm {
        &mut self.arm
    }
}

impl Debug for Bucket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Bucket")
            .field("id", &self.id)
            .field("seq", &self.seq)
            .field("arm", &self.arm)
            .field("map.len", &self.map.len())
            .field("is_changed", &self.is_changed)
            .finish()
    }
}

// entity index manager
pub struct IndexMgr {
    bkt_armor: FileArmor<Bucket>,
    buckets: HashMap<u8, Bucket>,
    hash_key: HashKey,
}

impl IndexMgr {
    // number of buckets
    const BUCKET_NUM: u8 = 8;

    pub fn new(base: &Path) -> Self {
        IndexMgr {
            bkt_armor: FileArmor::new(base),
            buckets: HashMap::new(),
            hash_key: HashKey::new_empty(),
        }
    }

    #[inline]
    pub fn set_crypto_ctx(
        &mut self,
        crypto: Crypto,
        key: Key,
        hash_key: HashKey,
    ) {
        self.bkt_armor.set_crypto_ctx(crypto, key);
        self.hash_key = hash_key;
    }

    // convert bucket index to id
    fn bucket_idx_to_eid(&self, bucket_idx: u8) -> Eid {
        let mut buf = Vec::with_capacity(1);
        buf.put_u8(bucket_idx);
        let hash = Crypto::hash_with_key(&buf, &self.hash_key);
        Eid::from_slice(&hash)
    }

    // open bucket for an entity, if not exists then create it
    fn open_bucket_raw(
        &mut self,
        id: &Eid,
        create: bool,
    ) -> Result<&mut Bucket> {
        let bucket_idx = id[0] % Self::BUCKET_NUM;
        let bucket_id = self.bucket_idx_to_eid(bucket_idx);

        if !self.buckets.contains_key(&bucket_idx) {
            // load bucket
            match self.bkt_armor.load_item(&bucket_id) {
                Ok(bucket) => {
                    self.buckets.insert(bucket_idx, bucket);
                }
                Err(ref err) if *err == Error::NotFound && create => {
                    // create a new bucket
                    let bucket = Bucket::new(bucket_id);
                    self.buckets.insert(bucket_idx, bucket);
                }
                Err(err) => return Err(err),
            }
        }

        let ret = self.buckets.get_mut(&bucket_idx).unwrap();

        Ok(ret)
    }

    #[inline]
    fn open_bucket_create(&mut self, id: &Eid) -> Result<&mut Bucket> {
        self.open_bucket_raw(id, true)
    }

    #[inline]
    fn open_bucket(&mut self, id: &Eid) -> Result<&mut Bucket> {
        self.open_bucket_raw(id, false)
    }

    // read entity address
    pub fn read_addr(&mut self, id: &Eid) -> Result<Vec<u8>> {
        let bucket = self.open_bucket(id)?;
        bucket
            .get(id)
            .ok_or(Error::NotFound)
            .map(|addr| addr.clone())
    }

    // write entity address
    pub fn write_addr(&mut self, id: &Eid, addr: &[u8]) -> Result<()> {
        let bucket = self.open_bucket_create(id)?;
        bucket.insert(id.clone(), addr.to_vec());
        Ok(())
    }

    // delete entity address
    pub fn del_address(&mut self, id: &Eid) -> Result<()> {
        match self.open_bucket(id) {
            Ok(bucket) => {
                bucket.remove(id);
                Ok(())
            }
            Err(ref err) if *err == Error::NotFound => Ok(()),
            Err(err) => Err(err),
        }
    }

    pub fn flush(&mut self) -> Result<()> {
        for bucket in self.buckets.values_mut() {
            if bucket.is_changed {
                self.bkt_armor.save_item(bucket)?;
                bucket.is_changed = false;
            }
        }
        Ok(())
    }
}

impl Debug for IndexMgr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("IndexMgr")
            .field("buckets", &self.buckets)
            .field("hash_key", &self.hash_key)
            .finish()
    }
}
