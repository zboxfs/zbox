use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::path::Path;

use bytes::BufMut;

use super::file_armor::FileArmor;
use base::crypto::{Crypto, HashKey, Key};
use base::lru::{CountMeter, Lru, PinChecker};
use error::{Error, Result};
use trans::{Eid, Id};
use volume::{Arm, ArmAccess, Armor, Seq};

// entity index stub
#[derive(Clone, Deserialize, Serialize)]
struct Index {
    id: Eid,
    seq: u64,
    arm: Arm,
    map: HashMap<Eid, Vec<u8>>,
}

impl Index {
    fn new(id: Eid) -> Self {
        Index {
            id,
            seq: 0,
            arm: Arm::default(),
            map: HashMap::new(),
        }
    }
}

impl Id for Index {
    #[inline]
    fn id(&self) -> &Eid {
        &self.id
    }

    #[inline]
    fn id_mut(&mut self) -> &mut Eid {
        &mut self.id
    }
}

impl Seq for Index {
    #[inline]
    fn seq(&self) -> u64 {
        self.seq
    }

    #[inline]
    fn inc_seq(&mut self) {
        self.seq += 1
    }
}

impl<'de> ArmAccess<'de> for Index {
    #[inline]
    fn arm(&self) -> Arm {
        self.arm
    }

    #[inline]
    fn arm_mut(&mut self) -> &mut Arm {
        &mut self.arm
    }
}

impl Debug for Index {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Index")
            .field("id", &self.id)
            .field("seq", &self.seq)
            .field("arm", &self.arm)
            .field("map.len", &self.map.len())
            .finish()
    }
}

// entity index manager
pub struct IndexMgr {
    idx_armor: FileArmor<Index>,
    cache: Lru<u8, Index, CountMeter<Index>, PinChecker<Index>>,
    hash_key: HashKey,
}

impl IndexMgr {
    // index cache size
    const CACHE_SIZE: usize = 16;

    pub fn new(base: &Path) -> Self {
        IndexMgr {
            idx_armor: FileArmor::new(base),
            cache: Lru::new(Self::CACHE_SIZE),
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
        self.idx_armor.set_crypto_ctx(crypto, key);
        self.hash_key = hash_key;
    }

    // convert bucket id to index id
    fn bucket_id_to_eid(&self, bucket_id: u8) -> Eid {
        let mut buf = Vec::with_capacity(1);
        buf.put_u8(bucket_id);
        let hash = Crypto::hash_with_key(&buf, &self.hash_key);
        Eid::from_slice(&hash)
    }

    // load index from disk file and put into cache
    fn load_index(&mut self, bucket_id: u8) -> Result<Index> {
        let index_id = self.bucket_id_to_eid(bucket_id);
        self.idx_armor.load_item(&index_id)
    }

    // save index to disk file
    fn save_index(&mut self, id: &Eid) -> Result<()> {
        let bucket_id = id[0];
        let index = self.cache.get_refresh(&bucket_id).unwrap();
        self.idx_armor.save_item(index)
    }

    // open index for an entity, if not exists then create it
    fn open_index(&mut self, id: &Eid, create: bool) -> Result<&mut Index> {
        let bucket_id = id[0];
        if !self.cache.contains_key(&bucket_id) {
            // load index and insert into cache
            match self.load_index(bucket_id) {
                Ok(index) => {
                    self.cache.insert(bucket_id, index);
                }
                Err(ref err) if *err == Error::NotFound => {
                    if create {
                        // create a new index and save it to cache
                        let index =
                            Index::new(self.bucket_id_to_eid(bucket_id));
                        self.cache.insert(bucket_id, index);
                    } else {
                        return Err(Error::NotFound);
                    }
                }
                Err(err) => return Err(Error::from(err)),
            }
        }

        let index = self.cache.get_refresh(&bucket_id).unwrap();

        Ok(index)
    }

    // read entity address from index
    pub fn read_addr(&mut self, id: &Eid) -> Result<Vec<u8>> {
        let index = self.open_index(id, false)?;
        index
            .map
            .get(id)
            .ok_or(Error::NotFound)
            .map(|addr| addr.clone())
    }

    // write entity address to index
    pub fn write_addr(&mut self, id: &Eid, addr: &[u8]) -> Result<()> {
        {
            let index = self.open_index(id, true)?;
            index.map.insert(id.clone(), addr.to_vec());
        }
        self.save_index(id)?;
        Ok(())
    }

    // delete entity address from index
    pub fn del_address(&mut self, id: &Eid) -> Result<()> {
        {
            match self.open_index(id, false) {
                Ok(index) => {
                    index.map.remove(id);
                }
                Err(ref err) if *err == Error::NotFound => return Ok(()),
                Err(err) => return Err(err),
            }
        }
        self.save_index(id)?;
        Ok(())
    }
}

impl Debug for IndexMgr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("IndexMgr")
            .field("hash_key", &self.hash_key)
            .finish()
    }
}
