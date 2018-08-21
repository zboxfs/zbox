//! fs module document
//!

pub mod fnode;
mod fs;

pub use self::fnode::{DirEntry, FileType, Fnode, FnodeRef, Metadata, Version};
pub use self::fs::{Fs, ShutterRef};

use base::crypto::{Cipher, Cost, Crypto};
use content::StoreRef;
use trans::TxMgrRef;
use volume::VolumeRef;

// Configuration
#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    pub cost: Cost,
    pub cipher: Cipher,
    pub version_limit: u8,
    pub compression: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            cost: Cost::default(),
            cipher: if Crypto::is_aes_hardware_available() {
                Cipher::Aes
            } else {
                Cipher::Xchacha
            },
            version_limit: Fnode::DEFAULT_VERSION_LIMIT,
            compression: false,
        }
    }
}

/// Open File Handle
#[derive(Debug, Clone)]
pub struct Handle {
    pub fnode: FnodeRef,
    pub store: StoreRef,
    pub txmgr: TxMgrRef,
    pub vol: VolumeRef,
    pub shutter: ShutterRef,
}
