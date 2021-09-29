use std::fmt::{self, Debug};

use crate::base::crypto::{Crypto, Key};
use crate::base::IntoRef;
use crate::error::Result;
use crate::trans::Eid;
use crate::volume::address::Span;
use crate::volume::storage::faulty_ctl::Controller;
use crate::volume::storage::mem::MemStorage;
use crate::volume::storage::Storable;

/// Faulty Storage
///
/// This storage is to simulate ramdon IO error, used for test only.
pub struct FaultyStorage {
    inner: MemStorage,
    ctlr: Controller,
}

impl FaultyStorage {
    pub fn new(loc: &str) -> Self {
        FaultyStorage {
            inner: MemStorage::new(loc),
            ctlr: Controller::new(),
        }
    }
}

impl Storable for FaultyStorage {
    #[inline]
    fn exists(&self) -> Result<bool> {
        self.ctlr.make_random_error()?;
        self.inner.exists()
    }

    #[inline]
    fn connect(&mut self, force: bool) -> Result<()> {
        self.inner.connect(force)
    }

    #[inline]
    fn init(&mut self, crypto: Crypto, key: Key) -> Result<()> {
        self.inner.init(crypto, key)
    }

    #[inline]
    fn open(&mut self, crypto: Crypto, key: Key, force: bool) -> Result<()> {
        self.ctlr.make_random_error()?;
        self.inner.open(crypto, key, force)
    }

    #[inline]
    fn get_super_block(&mut self, suffix: u64) -> Result<Vec<u8>> {
        self.ctlr.make_random_error()?;
        self.inner.get_super_block(suffix)
    }

    #[inline]
    fn put_super_block(&mut self, super_blk: &[u8], suffix: u64) -> Result<()> {
        self.ctlr.make_random_error()?;
        self.inner.put_super_block(super_blk, suffix)
    }

    #[inline]
    fn get_wal(&mut self, id: &Eid) -> Result<Vec<u8>> {
        self.ctlr.make_random_error()?;
        self.inner.get_wal(id)
    }

    #[inline]
    fn put_wal(&mut self, id: &Eid, wal: &[u8]) -> Result<()> {
        self.ctlr.make_random_error()?;
        self.inner.put_wal(id, wal)
    }

    #[inline]
    fn del_wal(&mut self, id: &Eid) -> Result<()> {
        self.ctlr.make_random_error()?;
        self.inner.del_wal(id)
    }

    #[inline]
    fn get_address(&mut self, id: &Eid) -> Result<Vec<u8>> {
        self.ctlr.make_random_error()?;
        self.inner.get_address(id)
    }

    #[inline]
    fn put_address(&mut self, id: &Eid, addr: &[u8]) -> Result<()> {
        self.ctlr.make_random_error()?;
        self.inner.put_address(id, addr)
    }

    #[inline]
    fn del_address(&mut self, id: &Eid) -> Result<()> {
        self.ctlr.make_random_error()?;
        self.inner.del_address(id)
    }

    #[inline]
    fn get_blocks(&mut self, dst: &mut [u8], span: Span) -> Result<()> {
        self.ctlr.make_random_error()?;
        self.inner.get_blocks(dst, span)
    }

    #[inline]
    fn put_blocks(&mut self, span: Span, blks: &[u8]) -> Result<()> {
        self.ctlr.make_random_error()?;
        self.inner.put_blocks(span, blks)
    }

    #[inline]
    fn del_blocks(&mut self, span: Span) -> Result<()> {
        self.ctlr.make_random_error()?;
        self.inner.del_blocks(span)
    }

    #[inline]
    fn flush(&mut self) -> Result<()> {
        self.ctlr.make_random_error()?;
        self.inner.flush()
    }

    #[inline]
    fn destroy(&mut self) -> Result<()> {
        unimplemented!()
    }
}

impl Debug for FaultyStorage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FaultyStorage")
            .field("inner", &self.inner)
            .finish()
    }
}

impl IntoRef for FaultyStorage {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::init_env;
    use crate::error::Error;

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
            fs.open(crypto.clone(), key.clone(), false).unwrap();
            fs2.open(crypto.clone(), key.clone(), false).unwrap();
            assert_eq!(fs.get_address(&id).unwrap(), buf);
            assert_eq!(fs.get_address(&id2).unwrap_err(), Error::NotFound);
            assert_eq!(fs2.get_address(&id2).unwrap(), buf2);
            assert_eq!(fs2.get_address(&id).unwrap_err(), Error::NotFound);
        }
    }
}
