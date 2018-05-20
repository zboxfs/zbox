use std::fmt::{self, Debug};

use base::crypto::{Crypto, CryptoCtx, Key};
use base::lru::{CountMeter, Lru, Pinnable};
use error::{Error, Result};
use trans::{Eid, Id, Loc, Txid};
use super::armor::{Arm, Armor, Seq};
use super::storage::StorageRef;

// Emap LRU cache size
const CACHE_SIZE: usize = 32;

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
pub struct Cell {
    pub txid: Txid,
    pub pre_txid: Txid,
}

impl Cell {
    fn advance_to(&mut self, txid: Txid) {
        self.pre_txid = self.txid;
        self.txid = txid
    }
}

/// Entity map node
#[derive(Debug, Deserialize, Serialize)]
struct Node {
    id: Eid,
    arm: Arm,
    cell: Cell,

    #[serde(skip_serializing, skip_deserializing, default)] mask: Option<Txid>,
}

impl Id for Node {
    #[inline]
    fn id(&self) -> &Eid {
        &self.id
    }

    #[inline]
    fn id_mut(&mut self) -> &mut Eid {
        &mut self.id
    }
}

impl Seq for Node {
    #[inline]
    fn seq(&self) -> u64 {
        self.cell.txid.val()
    }
}

impl<'de> Armor<'de> for Node {
    #[inline]
    fn arm(&self) -> Arm {
        self.arm
    }

    #[inline]
    fn arm_mut(&mut self) -> &mut Arm {
        &mut self.arm
    }
}

#[derive(Debug, Default, Clone)]
struct EmapPinChecker {}

impl Pinnable<Node> for EmapPinChecker {
    #[inline]
    fn is_pinned(&self, item: &Node) -> bool {
        // pin the nodes which are in trans
        item.mask != None
    }
}

/// Entity map
pub struct Emap {
    cache: Lru<Eid, Node, CountMeter<Node>, EmapPinChecker>,
    storage: StorageRef,
    crypto_ctx: CryptoCtx,
}

impl Emap {
    pub fn new(storage: StorageRef) -> Self {
        Emap {
            cache: Lru::new(CACHE_SIZE),
            storage,
            crypto_ctx: CryptoCtx::default(),
        }
    }

    pub fn set_crypto_ctx(
        &mut self,
        crypto: &Crypto,
        key: &Key,
        subkey_id: u64,
    ) {
        let subkey = Crypto::derive_from_key(key, subkey_id).unwrap();
        self.crypto_ctx.set_with(crypto, &subkey);
    }

    #[inline]
    pub fn set_storage(&mut self, storage: &StorageRef) {
        self.storage = storage.clone();
    }

    // get cell from cache or load it from storage
    fn get_node(&mut self, id: &Eid) -> Result<&mut Node> {
        if !self.cache.contains_key(id) {
            // load node from storage
            let node = Node::load(id, &self.storage, &self.crypto_ctx)?;

            // insert node into cache
            self.cache.insert(id.clone(), node);
        }

        let node = self.cache.get_refresh(id).unwrap();

        // if node is already deleted
        if node.cell.txid.is_del_txid() {
            return Err(Error::NotFound);
        }

        return Ok(node);
    }

    pub fn get(&mut self, loc: &Loc) -> Result<Cell> {
        let node = self.get_node(&loc.eid)?;

        match node.mask {
            Some(txid) => {
                if txid.is_del_txid() {
                    return Err(Error::NotFound);
                }
                if txid == loc.txid {
                    Ok(Cell {
                        txid,
                        pre_txid: node.cell.txid,
                    })
                } else {
                    Ok(node.cell)
                }
            }
            None => Ok(node.cell),
        }
    }

    pub fn put(&mut self, loc: &Loc) -> Result<()> {
        match self.get_node(&loc.eid) {
            Ok(node) => match node.mask {
                Some(txid) => if txid == loc.txid {
                    return Ok(());
                } else {
                    return Err(Error::InTrans);
                },
                None => {
                    node.mask = Some(loc.txid);
                    return Ok(());
                }
            },
            Err(ref err) if *err == Error::NotFound => {}
            Err(err) => return Err(err),
        }

        let node = Node {
            id: loc.eid.clone(),
            arm: Arm::default(),
            cell: Cell::default(),
            mask: Some(loc.txid),
        };
        self.cache.insert(loc.eid.clone(), node);
        Ok(())
    }

    pub fn del(&mut self, loc: &Loc) -> Result<()> {
        let del_loc = Loc::new(&loc.eid, Txid::new_del_txid());
        self.put(&del_loc)
    }

    pub fn commit(&mut self, loc: &Loc) -> Result<()> {
        let node = self.cache.get_mut(&loc.eid).unwrap();
        let txid = node.mask.take().unwrap();
        node.cell.advance_to(txid);
        node.save(&self.storage, &self.crypto_ctx)?;
        Ok(())
    }

    pub fn abort(&mut self, loc: &Loc) -> Result<()> {
        {
            let node = self.cache.get_mut(&loc.eid).unwrap();
            if node.cell.txid == loc.txid || node.cell.txid.is_del_txid() {
                node.del_arm(&self.storage, &self.crypto_ctx.hash_key)?;
            } else {
                node.mask = None;
            }
        }

        self.cache.remove(&loc.eid);
        Ok(())
    }

    pub fn remove(&mut self, id: &Eid) -> Result<()> {
        Node::remove_no_order(id, &self.storage, &self.crypto_ctx)?;
        self.cache.remove(id);
        Ok(())
    }
}

impl Debug for Emap {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Emap").field("cache", &self.cache).finish()
    }
}
