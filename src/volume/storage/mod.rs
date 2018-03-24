mod file;
mod mem;
#[cfg(feature = "zbox-cloud")]
mod zbox;

use std::fmt::Debug;
use std::io::Result as IoResult;

use error::Result;
use base::crypto::{Crypto, Key};
use trans::{Eid, Txid};

pub use self::file::FileStorage;
pub use self::mem::MemStorage;

/// Storage trait
pub trait Storage: Debug {
    /// Check if storage exists
    fn exists(&self, location: &str) -> Result<bool>;

    // Volume initialisation
    // ------------------------
    /// Initialise storage with storage key and volume header
    fn init(
        &mut self,
        volume_id: &Eid,
        crypto: &Crypto,
        skey: &Key,
    ) -> Result<()>;

    // Super block read/write
    // ------------------------
    /// Get super block
    fn get_super_blk(&self) -> Result<Vec<u8>>;

    /// Put super block
    fn put_super_blk(&mut self, super_blk: &[u8]) -> Result<()>;

    // Open storage
    // ------------------------
    /// Open storage, return last comitted transaction id
    fn open(
        &mut self,
        volume_id: &Eid,
        crypto: &Crypto,
        skey: &Key,
    ) -> Result<Txid>;

    // Entity read/write/delete
    // ------------------------
    /// Read an entity
    fn read(
        &mut self,
        id: &Eid,
        offset: u64,
        buf: &mut [u8],
        txid: Txid,
    ) -> IoResult<usize>;

    /// Write but to an entity, return how many bytes written
    fn write(
        &mut self,
        id: &Eid,
        offset: u64,
        buf: &[u8],
        txid: Txid,
    ) -> IoResult<usize>;

    /// Delete an entity
    fn del(&mut self, id: &Eid, txid: Txid) -> Result<Option<Eid>>;

    // Transaction
    // ------------------------
    /// Begin transaction
    fn begin_trans(&mut self, txid: Txid) -> Result<()>;

    /// Abort transaction
    ///
    /// If any errors happend between begin_trans() and commit_trans(), this
    /// should be called to abort a transaction.
    fn abort_trans(&mut self, txid: Txid) -> Result<()>;

    /// Commit transaction
    fn commit_trans(&mut self, txid: Txid) -> Result<()>;
}
