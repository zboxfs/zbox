//! This example to demonstrate how to copy data from and to ZboxFS.
//!
//! To run this example, use the command below:
//!
//! $ cargo run --example copy_in_out

extern crate zbox;

use std::env::temp_dir;
use std::io::{copy, Seek, SeekFrom};
use zbox::{init_env, OpenOptions, RepoOpener};

fn main() {
    // initialise zbox environment, called first
    init_env();

    // create and open a repository
    let mut repo = RepoOpener::new()
        .create(true)
        .open("mem://copy_in_out", "pwd")
        .unwrap();

    // create and open a file for writing
    let mut file = OpenOptions::new()
        .create(true)
        .open(&mut repo, "/copy_in_out.rs")
        .unwrap();

    // open source file on OS file system
    let mut src = std::fs::OpenOptions::new()
        .read(true)
        .open("./examples/copy_in_out.rs")
        .unwrap();

    // use std::io::copy to read data from source file and write it to ZboxFS
    copy(&mut src, &mut file).unwrap();

    // finish writting to make a permanent content version
    file.finish().unwrap();

    // open target file on OS temporary folder
    let mut tgt_path = temp_dir();
    tgt_path.push("copy_in_out.rs");
    let mut tgt = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(&tgt_path)
        .unwrap();

    // use std::io::copy to read data from ZboxFS and write it to target file
    file.seek(SeekFrom::Start(0)).unwrap();
    copy(&mut file, &mut tgt).unwrap();
}
