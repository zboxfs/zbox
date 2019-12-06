#![allow(clippy::module_inception)]
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
pub use self::txmgr::{TxHandle, TxMgr, TxMgrRef, TxMgrWeakRef};
pub use self::wal::EntityType;

use std::io::Write;

use error::Result;

/// Finish trait, used with writer which implements std::io::Write trait
pub trait Finish: Write {
    fn finish(self) -> Result<()>;
}
