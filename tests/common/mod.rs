extern crate tempdir;

pub mod fuzz;

use self::tempdir::TempDir;
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
        let path = "file://".to_string() + dir.to_str().unwrap();
        let repo = RepoOpener::new()
            .create_new(true)
            .open(&path, "pwd")
            .unwrap();
        TestEnv { repo, tmpdir }
    }
}
