//! Zbox is a zero-knowledge, privacy-focused embeddable file system.
//!
//! It keeps files securely, privately and reliably on underneath storages.
//! By encapsulating files and directories into an encrypted repository, it
//! provides a virtual file system and exclusive access to authorised
//! application.
//!
//! The most core part of this module is [`Repo`] and [`File`], which provides
//! most file system operations and file I/O.
//!
//! - [`Repo`] provides similar file system manipulation methods as [`std::fs`]
//! - [`File`] provides similar file I/O methods as [`std::fs::File`]
//!
//! [`zbox_init`] initialises the environment and should be called before
//! any other methods provied by Zbox. It can be called more than once.
//!
//! # Example
//!
//! Create and open a [`Repo`] using OS file system as storage.
//!
//! ```no_run
//! use zbox::{zbox_init, RepoOpener};
//!
//! // initialise zbox environment, called first
//! zbox_init();
//!
//! // create and open the repository
//! let mut repo = RepoOpener::new()
//!     .create(true)
//!     .open("file://./my_repo", "your password")
//!     .unwrap();
//! ```
//!
//! [`File`] content IO using [`Read`] and [`Write`] traits.
//!
//! ```
//! # use zbox::{zbox_init, RepoOpener};
//! use std::io::prelude::*;
//! use zbox::OpenOptions;
//! # zbox_init();
//! # let mut repo = RepoOpener::new()
//! #    .create(true)
//! #    .open("mem://foo", "pwd")
//! #    .unwrap();
//!
//! // create and open file for writing
//! let mut file = OpenOptions::new()
//!     .create(true)
//!     .open(&mut repo, "/my_file")
//!     .unwrap();
//!
//! // use std::io::Write trait to write data into it
//! file.write_all(b"Hello, world!").unwrap();
//!
//! // finish the writting to make a permanent version of content
//! file.finish().unwrap();
//!
//! // read file content using std::io::Read trait
//! let mut content = String::new();
//! file.read_to_string(&mut content).unwrap();
//! assert_eq!(content, "Hello, world!");
//! ```
//!
//! Directory navigation can use [`Path`] and [`PathBuf`].
//!
//! ```
//! # use zbox::{zbox_init, RepoOpener};
//! use std::path::Path;
//! # zbox_init();
//! # let mut repo = RepoOpener::new()
//! #    .create(true)
//! #    .open("mem://foo", "pwd")
//! #    .unwrap();
//!
//! let path = Path::new("/foo/bar");
//! repo.create_dir_all(&path).unwrap();
//! assert!(repo.is_dir(path.parent().unwrap()));
//! ```
//!
//! [`std::fs`]: https://doc.rust-lang.org/std/fs/index.html
//! [`std::fs::File`]: https://doc.rust-lang.org/std/fs/struct.File.html
//! [`Read`]: https://doc.rust-lang.org/std/io/trait.Read.html
//! [`Write`]: https://doc.rust-lang.org/std/io/trait.Write.html
//! [`Path`]: https://doc.rust-lang.org/std/path/struct.Path.html
//! [`PathBuf`]: https://doc.rust-lang.org/std/path/struct.PathBuf.html
//! [`zbox_init`]: fn.zbox_init.html
//! [`Repo`]: struct.Repo.html
//! [`File`]: struct.File.html

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
pub use self::base::{Time, Version};
pub use self::base::crypto::{Error as CryptoError, OpsLimit, MemLimit, Cost,
                             Cipher};
pub use self::trans::Eid;
pub use self::fs::fnode::{FileType, Metadata, DirEntry};
pub use self::file::File;
pub use self::repo::{RepoOpener, OpenOptions, RepoInfo, Repo};

/// Initialise Zbox environment.
#[no_mangle]
pub extern "C" fn zbox_init() {
    base::global_init();
}
