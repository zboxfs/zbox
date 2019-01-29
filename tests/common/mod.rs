extern crate tempdir;

pub mod controller;
pub mod crypto;
pub mod fuzzer;

use self::tempdir::TempDir;
use std::fs;
use zbox::{init_env, Repo, RepoOpener};

#[allow(dead_code)]
#[derive(Debug)]
pub struct TestEnv {
    pub repo: Repo,
    pub tmpdir: TempDir,
}

impl TestEnv {
    #[allow(dead_code)]
    pub fn new() -> Self {
        init_env();
        let tmpdir = TempDir::new("zbox_test").expect("Create temp dir failed");
        let dir = tmpdir.path().join("repo");
        //let dir = std::path::PathBuf::from("./tt");
        if dir.exists() {
            fs::remove_dir_all(&dir).unwrap();
        }
        let uri = "file://".to_string() + dir.to_str().unwrap();
        //let uri = "zbox://accessKey456@repo456?cache_type=mem&cache_size=1";
        let repo = RepoOpener::new()
            //.create(true)
            //.cipher(zbox::Cipher::Xchacha)
            .create_new(true)
            .open(&uri, "pwd")
            .unwrap();
        TestEnv { repo, tmpdir }
    }
}
