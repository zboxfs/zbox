#![allow(dead_code)]
extern crate tempdir;

#[cfg(any(
    feature = "storage-faulty",
    feature = "storage-file",
    feature = "storage-zbox-faulty"
))]
pub mod controller;
pub mod crypto;
#[cfg(any(
    feature = "storage-faulty",
    feature = "storage-file",
    feature = "storage-zbox-faulty"
))]
pub mod fuzzer;

use self::tempdir::TempDir;
use zbox::{init_env, Repo, RepoOpener};

#[derive(Debug)]
pub struct TestEnv {
    pub repo: Repo,
    pub tmpdir: Option<TempDir>,
}

cfg_if! {
    if #[cfg(feature = "storage-file")] {
        impl TestEnv {
            pub fn new() -> Self {
                init_env();
                let tmpdir = TempDir::new("zbox_test").expect("Create temp dir failed");
                let dir = tmpdir.path().join("repo");
                if dir.exists() {
                    std::fs::remove_dir_all(&dir).unwrap();
                }
                let uri = "file://".to_string() + dir.to_str().unwrap();
                let repo = RepoOpener::new()
                    .create_new(true)
                    .open(&uri, "pwd")
                    .unwrap();
                TestEnv { repo, tmpdir: Some(tmpdir) }
            }
        }
    } else if #[cfg(any(feature = "storage-zbox-native", feature = "storage-zbox-faulty"))] {
        impl TestEnv {
            pub fn new() -> Self {
                init_env();
                let uri = "zbox://accessKey456@repo456?cache_type=mem&cache_size=1";
                let repo = RepoOpener::new()
                    .cipher(zbox::Cipher::Xchacha)
                    .create_new(true)
                    .open(&uri, "pwd")
                    .unwrap();
                TestEnv { repo, tmpdir: None }
            }
        }
    } else {
        impl TestEnv {
            pub fn new() -> Self {
                init_env();
                let uri = "mem://foo";
                let repo = RepoOpener::new()
                    .create_new(true)
                    .open(&uri, "pwd")
                    .unwrap();
                TestEnv { repo, tmpdir: None }
            }
        }
    }
}
