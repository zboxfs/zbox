//! zbox document
//!

//#![feature(optin_builtin_traits)]

extern crate bytes;
extern crate env_logger;
extern crate linked_hash_map;
#[macro_use]
extern crate log;
extern crate lz4;
extern crate rmp_serde;
extern crate serde;
#[macro_use]
extern crate serde_derive;

macro_rules! map_io_err {
    ($x:expr) => {
        $x.map_err(|e| IoError::new(ErrorKind::Other, e.description()));
    }
}

mod base;
mod content;
mod error;
mod file;
mod fs;
mod repo;
mod trans;
mod version;
mod volume;

pub use self::error::{Error, Result};
pub use self::base::crypto::{OpsLimit, MemLimit, Cipher};
pub use self::file::File;
pub use self::repo::{RepoOpener, OpenOptions, Repo};

#[no_mangle]
pub extern "C" fn zbox_init() {
    base::global_init();
}
