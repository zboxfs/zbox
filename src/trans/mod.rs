//! trans module document
//!

pub mod cow;
mod eid;
pub mod trans;
mod txid;
mod txmgr;

pub use self::eid::{CloneNew, Eid, Id, Loc};
pub use self::txmgr::{TxHandle, TxMgr, TxMgrRef};
pub use self::txid::Txid;
