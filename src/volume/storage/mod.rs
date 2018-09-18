mod file;
mod mem;
mod storage;

pub use self::file::FileStorage;
pub use self::mem::MemStorage;
pub use self::storage::{Reader, Storage, StorageRef, Writer};

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

use std::fmt::Debug;

use base::crypto::{Crypto, Key};
use error::Result;
use trans::Eid;

/// Storable trait
pub trait Storable: Debug + Send + Sync {
    // check if storage exists
    fn exists(&self) -> Result<bool>;

    // initial a storage
    fn init(&mut self, crypto: Crypto, key: Key) -> Result<()>;

    // open a storage
    fn open(&mut self, crypto: Crypto, key: Key) -> Result<()>;

    // super block operations
    fn get_super_block(&mut self, suffix: u64) -> Result<Vec<u8>>;
    fn put_super_block(&mut self, super_blk: &[u8], suffix: u64) -> Result<()>;

    // address operations
    fn get_address(&mut self, id: &Eid) -> Result<Vec<u8>>;
    fn put_address(&mut self, id: &Eid, addr: &[u8]) -> Result<()>;
    fn del_address(&mut self, id: &Eid) -> Result<()>;

    // block operations
    fn get_blocks(
        &mut self,
        dst: &mut [u8],
        start_idx: u64,
        cnt: usize,
    ) -> Result<()>;
    fn put_blocks(
        &mut self,
        start_idx: u64,
        cnt: usize,
        blks: &[u8],
    ) -> Result<()>;
    fn del_blocks(&mut self, start_idx: u64, cnt: usize) -> Result<()>;
}
