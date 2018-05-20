//! volume module document
//!

mod armor;
mod emap;
mod storage;
mod super_blk;
mod txlog;
mod volume;

pub use self::volume::{Meta, Persistable, Reader, Volume, VolumeRef, Writer};
