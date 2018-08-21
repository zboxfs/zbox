//! trans module document
//!

pub mod cow;
mod eid;
pub mod trans;
mod txid;
mod txmgr;
mod wal;

pub use self::eid::{Eid, Id};
pub use self::txid::Txid;
pub use self::txmgr::{TxHandle, TxMgr, TxMgrRef};
pub use self::wal::EntityType;

use error::Result;

/// Finish trait, used with writer which implements std::io::Write trait
pub trait Finish {
    fn finish(self) -> Result<usize>;
}
