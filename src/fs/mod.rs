//! fs module document
//!

pub mod fnode;
mod fs;

pub use self::fnode::{Fnode, FnodeRef, FileType, Version, Metadata, DirEntry};
pub use self::fs::{Fs, FsRef};

use content::StoreRef;
use volume::VolumeRef;
use trans::TxMgrRef;

/// Open File Handle
#[derive(Debug, Clone)]
pub struct Handle {
    pub fnode: FnodeRef,
    pub store: StoreRef,
    pub txmgr: TxMgrRef,
    pub vol: VolumeRef,
}
