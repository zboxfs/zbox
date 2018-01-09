use tempdir::TempDir;
use zbox::{init_env, Repo, RepoOpener};

#[derive(Debug)]
pub struct TestEnv {
    pub repo: Repo,
    pub tmpdir: TempDir,
}

impl TestEnv {
    #[allow(dead_code)]
    pub fn reopen(&mut self) {
        let tmpdir = TempDir::new("zbox_test").expect("Create temp dir failed");
        let dir = tmpdir.path().join("repo");
        let path = "file://".to_string() + dir.to_str().unwrap();
        let dummy_repo =
            RepoOpener::new().create(true).open(&path, "pwd").unwrap();

        let info = self.repo.info();
        self.repo = dummy_repo;
        self.repo = RepoOpener::new().open(info.uri(), "pwd").unwrap();
    }
}

pub fn setup() -> TestEnv {
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
