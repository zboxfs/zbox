//! Zbox is a zero-details, privacy-focused embeddable file system.
//!
//! It keeps files securely, privately and reliably on underlying storages.
//! By encapsulating files and directories into an encrypted repository, it
//! provides a virtual file system and exclusive access to authorised
//! application.
//!
//! The most core parts of this module are [`Repo`] and [`File`], which provides
//! most file system operations and file I/O.
//!
//! - [`Repo`] provides similar file system manipulation methods to [`std::fs`]
//! - [`File`] provides similar file I/O methods to [`std::fs::File`]
//!
//! [`init_env`] initialises the environment and should be called before
//! any other methods provied by Zbox.
//!
//! After repository is opened by [`RepoOpener`], all of the other functions
//! provided by Zbox will be thread-safe.
//!
//! # Examples
//!
//! Create and open a [`Repo`] using OS file system as storage.
//!
//! ```no_run
//! # #![allow(unused_mut, unused_variables)]
//! use zbox::{init_env, RepoOpener};
//!
//! // initialise zbox environment, called first
//! init_env();
//!
//! // create and open a repository
//! let mut repo = RepoOpener::new()
//!     .create(true)
//!     .open("file://./my_repo", "your password")
//!     .unwrap();
//! ```
//!
//! [`File`] content IO using [`Read`] and [`Write`] traits.
//!
//! ```
//! # use zbox::{init_env, RepoOpener};
//! use std::io::prelude::*;
//! use std::io::{Seek, SeekFrom};
//! use zbox::OpenOptions;
//! # init_env();
//! # let mut repo = RepoOpener::new()
//! #    .create(true)
//! #    .open("mem://foo", "pwd")
//! #    .unwrap();
//!
//! // create and open a file for writing
//! let mut file = OpenOptions::new()
//!     .create(true)
//!     .open(&mut repo, "/my_file.txt")
//!     .unwrap();
//!
//! // use std::io::Write trait to write data into it
//! file.write_all(b"Hello, world!").unwrap();
//!
//! // finish writting to make a permanent content version
//! file.finish().unwrap();
//!
//! // read file content using std::io::Read trait
//! let mut content = String::new();
//! file.seek(SeekFrom::Start(0)).unwrap();
//! file.read_to_string(&mut content).unwrap();
//! assert_eq!(content, "Hello, world!");
//! ```
//!
//! Directory navigation can use [`Path`] and [`PathBuf`]. The path separator
//! should always be "/".
//!
//! ```
//! # use zbox::{init_env, RepoOpener};
//! use std::path::Path;
//! # init_env();
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
//! [`init_env`]: fn.init_env.html
//! [`Repo`]: struct.Repo.html
//! [`File`]: struct.File.html
//! [`RepoOpener`]: struct.RepoOpener.html

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
    };
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

pub mod ffi;

pub use self::base::crypto::{Cipher, MemLimit, OpsLimit};
pub use self::base::init_env;
pub use self::error::{Error, Result};
pub use self::file::{File, VersionReader};
pub use self::fs::fnode::{DirEntry, FileType, Metadata, Version};
pub use self::repo::{OpenOptions, Repo, RepoInfo, RepoOpener};
pub use self::trans::Eid;

#[cfg(feature = "storage-faulty")]
#[macro_use]
extern crate lazy_static;

#[cfg(feature = "storage-faulty")]
pub use self::volume::FaultyController;

#[cfg(feature = "storage-sqlite")]
extern crate libsqlite3_sys;

#[cfg(feature = "storage-redis")]
extern crate redis;

#[cfg(target_os = "android")]
extern crate jni;

#[cfg(target_os = "android")]
extern crate android_log;

#[cfg(target_os = "android")]
pub mod jni_lib;
