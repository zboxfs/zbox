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
    pub fn new() -> Self {
        init_env();
        let tmpdir = TempDir::new("zbox_test").expect("Create temp dir failed");
        let dir = tmpdir.path().join("repo");
        //let dir = std::path::PathBuf::from("./tt");
        if dir.exists() {
            fs::remove_dir_all(&dir).unwrap();
        }
        let path = "file://".to_string() + dir.to_str().unwrap();
        let repo = RepoOpener::new()
            .create_new(true)
            .open(&path, "pwd")
            .unwrap();
        TestEnv { repo, tmpdir }
    }
}
