//! This example is to demonstrate how to use UTF8 string in ZboxFS.
//!
//! To run this example, use the command below:
//!
//! $ cargo run --example utf8

extern crate zbox;

use zbox::{init_env, RepoOpener};

fn main() {
    // initialise zbox environment, called first
    init_env();

    // create and open a repository
    let mut repo = RepoOpener::new()
        .create(true)
        .open("mem://utf8", "your password")
        .unwrap();

    repo.create_dir("/Hello").unwrap();
    repo.create_dir("/你好").unwrap();
    repo.create_dir("/こんにちは").unwrap();
    repo.create_dir("/안녕하세요").unwrap();
    repo.create_dir("/Здравствуйте").unwrap();
}
