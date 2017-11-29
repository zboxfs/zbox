extern crate zbox;

use std::io::{Read, Write};
use zbox::{zbox_init, RepoOpener, OpenOptions};

fn main() {
    // Initialise zbox environment, need to called first and only called once.
    zbox_init();

    // Speicify repository location using URI-like string.
    // Currently, two types of prefixes are supported:
    //   - "file://": use OS file as storage
    //   - "mem://": use memory as storage
    // After the prefix is the actual location of repository. Here we're
    // going to create an OS file repository called 'my_repo' under current
    // directory.
    let repo_uri = "file://./my_repo";

    // Speicify password of the repository.
    let pwd = "your secret";

    // Create and open the repository.
    let mut repo = RepoOpener::new()
        .create(true)
        .open(&repo_uri, &pwd)
        .unwrap();

    // Speicify file path we need to create in the repository and its data.
    let file_path = "/my_file";
    let data = String::from("Hello, world").into_bytes();

    // Create and open a regular file for writing, this file is inside
    // repository so it will be encrypted and kept privately.
    let mut f = OpenOptions::new()
        .create(true)
        .open(&mut repo, &file_path)
        .unwrap();

    // Like normal file operations, we can use std::io::Write trait to write
    // data into it.
    f.write_all(&data).unwrap();

    // But need to finish the writting before a permanent version of content
    // can be made.
    f.finish().unwrap();

    // Now we can read content from the file using std::io::Read trait.
    let mut buf = Vec::new();
    f.read_to_end(&mut buf).unwrap();

    // Convert content from bytes to string and print it to stdout. It should
    // display 'Hello, world' to your terminal.
    let output = String::from_utf8(buf).unwrap();
    println!("{}", output);
}
