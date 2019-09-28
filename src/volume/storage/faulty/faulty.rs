use std::fmt::{self, Debug};
use std::sync::{Arc, RwLock};

use base::crypto::{Crypto, Key};
use base::lru::{CountMeter, Lru, PinChecker};
use base::IntoRef;
use error::Result;
use trans::Eid;
use volume::address::Span;
use volume::storage::faulty_ctl::Controller;
use volume::storage::mem::MemStorage;
use volume::storage::Storable;

// how many memory storage kept in memory
const MEM_LRU_SIZE: usize = 4;

type MemStorageLru =
    Lru<String, MemStorage, CountMeter<MemStorage>, PinChecker<MemStorage>>;

lazy_static! {
    // static hashmap to store repos
    static ref MEM_STORAGES: Arc<RwLock<MemStorageLru>> =
        { Arc::new(RwLock::new(Lru::new(MEM_LRU_SIZE))) };
}

/// Faulty Storage
///
/// This storage is to simulate ramdon IO error, used for test only.
pub struct FaultyStorage {
    loc: String,
    inner: &'static MEM_STORAGES,
    ctlr: Controller,
}

impl FaultyStorage {
    pub fn new(loc: &str) -> Self {
        FaultyStorage {
            loc: loc.to_string(),
            inner: &MEM_STORAGES,
            ctlr: Controller::new(),
        }
    }
}

impl Storable for FaultyStorage {
    fn exists(&self) -> Result<bool> {
        self.ctlr.make_random_error()?;

        let inner = self.inner.read().unwrap();
        Ok(inner.contains_key(&self.loc))
    }

    #[inline]
    fn connect(&mut self) -> Result<()> {
        Ok(())
    }

    fn init(&mut self, _crypto: Crypto, _key: Key) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        assert!(!inner.contains_key(&self.loc));
        inner.insert(self.loc.to_string(), MemStorage::new());
        Ok(())
    }

    fn open(&mut self, _crypto: Crypto, _key: Key, _force: bool) -> Result<()> {
        self.ctlr.make_random_error()?;

        let inner = self.inner.read().unwrap();
        assert!(inner.contains_key(&self.loc));
        Ok(())
    }

    fn get_super_block(&mut self, suffix: u64) -> Result<Vec<u8>> {
        self.ctlr.make_random_error()?;

        let mut inner = self.inner.write().unwrap();
        let ms = inner.get_refresh(&self.loc).unwrap();
        ms.get_super_block(suffix)
    }

    fn put_super_block(&mut self, super_blk: &[u8], suffix: u64) -> Result<()> {
        self.ctlr.make_random_error()?;

        let mut inner = self.inner.write().unwrap();
        let ms = inner.get_refresh(&self.loc).unwrap();
        ms.put_super_block(super_blk, suffix)
    }

    fn get_wal(&mut self, id: &Eid) -> Result<Vec<u8>> {
        self.ctlr.make_random_error()?;

        let mut inner = self.inner.write().unwrap();
        let ms = inner.get_refresh(&self.loc).unwrap();
        ms.get_wal(id)
    }

    fn put_wal(&mut self, id: &Eid, wal: &[u8]) -> Result<()> {
        self.ctlr.make_random_error()?;

        let mut inner = self.inner.write().unwrap();
        let ms = inner.get_refresh(&self.loc).unwrap();
        ms.put_wal(id, wal)
    }

    fn del_wal(&mut self, id: &Eid) -> Result<()> {
        self.ctlr.make_random_error()?;

        let mut inner = self.inner.write().unwrap();
        let ms = inner.get_refresh(&self.loc).unwrap();
        ms.del_wal(id)
    }

    fn get_address(&mut self, id: &Eid) -> Result<Vec<u8>> {
        self.ctlr.make_random_error()?;

        let mut inner = self.inner.write().unwrap();
        let ms = inner.get_refresh(&self.loc).unwrap();
        ms.get_address(id)
    }

    fn put_address(&mut self, id: &Eid, addr: &[u8]) -> Result<()> {
        self.ctlr.make_random_error()?;

        let mut inner = self.inner.write().unwrap();
        let ms = inner.get_refresh(&self.loc).unwrap();
        ms.put_address(id, addr)
    }

    fn del_address(&mut self, id: &Eid) -> Result<()> {
        self.ctlr.make_random_error()?;

        let mut inner = self.inner.write().unwrap();
        let ms = inner.get_refresh(&self.loc).unwrap();
        ms.del_address(id)
    }

    fn get_blocks(&mut self, dst: &mut [u8], span: Span) -> Result<()> {
        self.ctlr.make_random_error()?;

        let mut inner = self.inner.write().unwrap();
        let ms = inner.get_refresh(&self.loc).unwrap();
        ms.get_blocks(dst, span)
    }

    fn put_blocks(&mut self, span: Span, blks: &[u8]) -> Result<()> {
        self.ctlr.make_random_error()?;

        let mut inner = self.inner.write().unwrap();
        let ms = inner.get_refresh(&self.loc).unwrap();
        ms.put_blocks(span, blks)
    }

    fn del_blocks(&mut self, span: Span) -> Result<()> {
        self.ctlr.make_random_error()?;

        let mut inner = self.inner.write().unwrap();
        let ms = inner.get_refresh(&self.loc).unwrap();
        ms.del_blocks(span)
    }

    fn flush(&mut self) -> Result<()> {
        self.ctlr.make_random_error()?;

        let mut inner = self.inner.write().unwrap();
        let ms = inner.get_refresh(&self.loc).unwrap();
        ms.flush()
    }
}

impl Debug for FaultyStorage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FaultyStorage")
            .field("loc", &self.loc)
            .finish()
    }
}

impl IntoRef for FaultyStorage {}

#[cfg(test)]
mod tests {
    use super::*;
    use base::init_env;
    use error::Error;

    #[test]
    fn static_storages() {
        init_env();

        let crypto = Crypto::default();
        let key = Key::new_empty();
        let loc = "foo";
        let loc2 = "bar";
        let id = Eid::new();
        let id2 = Eid::new();
        let buf = vec![1, 2, 3];
        let buf2 = vec![4, 5, 6];

        {
            let mut fs = FaultyStorage::new(&loc);
            let mut fs2 = FaultyStorage::new(&loc2);
            fs.init(crypto.clone(), key.clone()).unwrap();
            fs2.init(crypto.clone(), key.clone()).unwrap();
            fs.put_address(&id, &buf).unwrap();
            fs2.put_address(&id2, &buf2).unwrap();
        }

        {
            let mut fs = FaultyStorage::new(&loc);
            let mut fs2 = FaultyStorage::new(&loc2);
            assert!(fs.exists().unwrap());
            assert!(fs2.exists().unwrap());
            fs.open(crypto.clone(), key.clone()).unwrap();
            fs2.open(crypto.clone(), key.clone()).unwrap();
            assert_eq!(fs.get_address(&id).unwrap(), buf);
            assert_eq!(fs.get_address(&id2).unwrap_err(), Error::NotFound);
            assert_eq!(fs2.get_address(&id2).unwrap(), buf2);
            assert_eq!(fs2.get_address(&id).unwrap_err(), Error::NotFound);
        }
    }
}
