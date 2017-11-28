//! volume module document
//!

mod storage;
mod volume;

pub use self::volume::{Volume, VolumeRef, Meta, Reader, Writer, Persistable};
