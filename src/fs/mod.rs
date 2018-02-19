//! fs module document
//!

pub mod fnode;
mod fs;

pub use self::fnode::{DirEntry, FileType, Fnode, FnodeRef, Metadata, Version};
pub use self::fs::{Fs, ShutterRef};

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
    pub shutter: ShutterRef,
}
