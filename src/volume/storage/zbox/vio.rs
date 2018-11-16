//! Virtual IO
//!
//! This module is to provide a zero-cost abstraction for OS file system API.

pub use std::fs::{
    copy, create_dir, create_dir_all, metadata, read_dir, remove_dir,
    remove_dir_all, remove_file, rename, File, OpenOptions, ReadDir,
};
