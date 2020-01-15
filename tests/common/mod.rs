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
                let uri = "zbox://accessKey456@repo456?cache_type=mem&cache_size=1mb";
                let repo = RepoOpener::new()
                    .cipher(zbox::Cipher::Xchacha)
                    .create_new(true)
                    .open(&uri, "pwd")
                    .unwrap();
                TestEnv { repo, tmpdir: None }
            }
        }
    } else if #[cfg(feature = "storage-sqlite")] {
        impl TestEnv {
            pub fn new() -> Self {
                init_env();
                let tmpdir = TempDir::new("zbox_test")
                    .expect("Create temp dir failed");
                let file = tmpdir.path().join("zbox.db");
                let uri = "sqlite://".to_string() + file.to_str().unwrap();
                let repo = RepoOpener::new()
                    .create_new(true)
                    .open(&uri, "pwd")
                    .unwrap();
                TestEnv { repo, tmpdir: Some(tmpdir) }
            }
        }
    } else if #[cfg(feature = "storage-redis")] {
        // to test redis storage, start a local redis server first:
        // docker run --rm -p 6379:6379 redis:latest
        //
        // Note: test cases should run one by one and clear redis db before
        // start the next test case
        impl TestEnv {
            pub fn new() -> Self {
                init_env();
                let uri = "redis://localhost:6379".to_string();
                let repo = RepoOpener::new()
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
                let uri = format!("mem://{}", crypto::random_u32(u32::max_value()));
                let repo = RepoOpener::new()
                    .create_new(true)
                    .dedup_file(true)
                    .open(&uri, "pwd")
                    .unwrap();
                TestEnv { repo, tmpdir: None }
            }
        }
    }
}
