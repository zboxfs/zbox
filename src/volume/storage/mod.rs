mod storage;

pub use self::storage::{
    Reader, Storage, StorageRef, WalReader, WalWriter, Writer,
};

#[cfg(feature = "storage-mem")]
mod mem;

#[cfg(feature = "storage-file")]
mod file;

#[cfg(feature = "storage-faulty")]
mod faulty;

#[cfg(feature = "storage-faulty")]
pub use self::faulty::Controller as FaultyController;

#[cfg(feature = "storage-sqlite")]
mod sqlite;

#[cfg(feature = "storage-redis")]
mod redis;

#[cfg(feature = "storage-zbox")]
mod zbox;

#[cfg(any(feature = "storage-file", feature = "storage-zbox"))]
mod index_mgr;

use std::fmt::Debug;

use base::crypto::{Crypto, Key};
use error::Result;
use trans::Eid;
use volume::address::Span;

/// Storable trait
pub trait Storable: Debug + Send + Sync {
    // check if storage exists
    fn exists(&self) -> Result<bool>;

    // make connection to storage
    fn connect(&mut self) -> Result<()>;

    // initial a storage
    fn init(&mut self, crypto: Crypto, key: Key) -> Result<()>;

    // open a storage
    fn open(&mut self, crypto: Crypto, key: Key) -> Result<()>;

    // super block read/write, must not buffered
    // write no need to be atomic, but must gurantee any successful
    // write is persistent
    fn get_super_block(&mut self, suffix: u64) -> Result<Vec<u8>>;
    fn put_super_block(&mut self, super_blk: &[u8], suffix: u64) -> Result<()>;

    // wal read/write, must not buffered
    // update no need to be atomic, but must gurantee any successful update
    // is persistent.
    // Note: wal deletion doesn't need to be persistent and can be buffered
    fn get_wal(&mut self, id: &Eid) -> Result<Vec<u8>>;
    fn put_wal(&mut self, id: &Eid, wal: &[u8]) -> Result<()>;
    fn del_wal(&mut self, id: &Eid) -> Result<()>;

    // address read/write, can be buffered
    // storage doesn't need to gurantee update is persistent
    fn get_address(&mut self, id: &Eid) -> Result<Vec<u8>>;
    fn put_address(&mut self, id: &Eid, addr: &[u8]) -> Result<()>;
    fn del_address(&mut self, id: &Eid) -> Result<()>;

    // block read/write, can be buffered
    // storage doesn't need to gurantee update is persistent
    fn get_blocks(&mut self, dst: &mut [u8], span: Span) -> Result<()>;
    fn put_blocks(&mut self, span: Span, blks: &[u8]) -> Result<()>;
    fn del_blocks(&mut self, span: Span) -> Result<()>;

    // flush possibly buffered wal, address and block to storage,
    // storage must gurantee write is persistent
    fn flush(&mut self) -> Result<()>;
}

/// Dummy storage
#[derive(Debug, Default)]
pub struct DummyStorage;

impl Storable for DummyStorage {
    #[inline]
    fn exists(&self) -> Result<bool> {
        unimplemented!()
    }

    #[inline]
    fn connect(&mut self) -> Result<()> {
        unimplemented!()
    }

    #[inline]
    fn init(&mut self, _crypto: Crypto, _key: Key) -> Result<()> {
        unimplemented!()
    }

    #[inline]
    fn open(&mut self, _crypto: Crypto, _key: Key) -> Result<()> {
        unimplemented!()
    }

    #[inline]
    fn get_super_block(&mut self, _suffix: u64) -> Result<Vec<u8>> {
        unimplemented!()
    }

    #[inline]
    fn put_super_block(
        &mut self,
        _super_blk: &[u8],
        _suffix: u64,
    ) -> Result<()> {
        unimplemented!()
    }

    #[inline]
    fn get_wal(&mut self, _id: &Eid) -> Result<Vec<u8>> {
        unimplemented!()
    }

    #[inline]
    fn put_wal(&mut self, _id: &Eid, _wal: &[u8]) -> Result<()> {
        unimplemented!()
    }

    #[inline]
    fn del_wal(&mut self, _id: &Eid) -> Result<()> {
        unimplemented!()
    }

    #[inline]
    fn get_address(&mut self, _id: &Eid) -> Result<Vec<u8>> {
        unimplemented!()
    }

    #[inline]
    fn put_address(&mut self, _id: &Eid, _addr: &[u8]) -> Result<()> {
        unimplemented!()
    }

    #[inline]
    fn del_address(&mut self, _id: &Eid) -> Result<()> {
        unimplemented!()
    }

    #[inline]
    fn get_blocks(&mut self, _dst: &mut [u8], _span: Span) -> Result<()> {
        unimplemented!()
    }

    #[inline]
    fn put_blocks(&mut self, _span: Span, _blks: &[u8]) -> Result<()> {
        unimplemented!()
    }

    #[inline]
    fn del_blocks(&mut self, _span: Span) -> Result<()> {
        unimplemented!()
    }

    #[inline]
    fn flush(&mut self) -> Result<()> {
        unimplemented!()
    }
}
