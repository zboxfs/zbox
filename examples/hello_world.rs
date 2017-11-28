extern crate zbox;

use std::io::{Read, Write};
use zbox::{zbox_init, RepoOpener, OpenOptions};

fn main() {
    // initialise zbox environment
    zbox_init();

    let repo_uri = "file://./hello_world_repo";
    let pwd = "your secret";
    let file_path = "/my_file";
    let input = String::from("Hello, world").into_bytes();

    // create repo
    let mut repo = RepoOpener::new()
        .create(true)
        .open(&repo_uri, &pwd)
        .unwrap();

    // create file
    let mut f = OpenOptions::new()
        .create(true)
        .open(&mut repo, &file_path)
        .unwrap();

    // write data to file
    f.write_all(&input).unwrap();
    f.finish().unwrap();

    // read data from file
    let mut buf = Vec::new();
    f.read_to_end(&mut buf).unwrap();
    let output = String::from_utf8(buf).unwrap();
    println!("{:?}", output);
}
