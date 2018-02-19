//! volume module document
//!

mod storage;
mod volume;

pub use self::volume::{Meta, Persistable, Reader, Volume, VolumeRef, Writer};
