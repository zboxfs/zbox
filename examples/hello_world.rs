extern crate zbox;

use std::io::{Read, Write};
use zbox::{zbox_init, RepoOpener, OpenOptions};

fn main() {
    // initialise zbox environment, called first
    zbox_init();

    // create and open a repository
    let mut repo = RepoOpener::new()
        .create(true)
        .open("file://./my_repo", "your password")
        .unwrap();

    // create and open a file for writing
    let mut file = OpenOptions::new()
        .create(true)
        .open(&mut repo, "/my_file")
        .unwrap();

    // use std::io::Write trait to write data into it
    file.write_all(b"Hello, world!").unwrap();

    // finish the writting to make a permanent version of content
    file.finish().unwrap();

    // read file content using std::io::Read trait
    let mut content = String::new();
    file.read_to_string(&mut content).unwrap();

    println!("{}", content);

    // cleanup
    drop(file);
    drop(repo);
    std::fs::remove_dir_all("./my_repo").unwrap();
}
