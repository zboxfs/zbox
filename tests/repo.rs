extern crate tempdir;

extern crate zbox;

use tempdir::TempDir;

use zbox::{init_env, Error, RepoOpener, OpsLimit, MemLimit, Cipher};

#[test]
fn repo_oper() {
    init_env();

    let pwd = "pwd";
    let tmpdir = TempDir::new("zbox_test").expect("Create temp dir failed");
    let dir = tmpdir.path().to_path_buf();
    let base = "file://".to_string() + dir.to_str().unwrap();

    // case #1: create a new repo with default options and then re-open it
    let path = base.clone() + "/repo";
    RepoOpener::new().create(true).open(&path, &pwd).unwrap();
    RepoOpener::new().open(&path, &pwd).unwrap();

    // case #2: create a new repo with custom options and then re-open it
    let path = base.clone() + "/repo2";
    RepoOpener::new()
        .create(true)
        .ops_limit(OpsLimit::Moderate)
        .mem_limit(MemLimit::Moderate)
        .cipher(Cipher::Aes)
        .open(&path, &pwd)
        .unwrap();
    let repo = RepoOpener::new().open(&path, &pwd).unwrap();
    let info = repo.info();
    assert_eq!(info.ops_limit(), OpsLimit::Moderate);
    assert_eq!(info.mem_limit(), MemLimit::Moderate);
    assert_eq!(info.cipher(), Cipher::Aes);
    assert!(!info.is_read_only());

    // case #3: open repo in read-only mode
    let path = base.clone() + "/repo3";
    {
        RepoOpener::new()
            .create(true)
            .read_only(true)
            .open(&path, &pwd)
            .is_err();
        RepoOpener::new().create(true).open(&path, &pwd).unwrap();
    }
    let mut repo = RepoOpener::new().read_only(true).open(&path, &pwd).unwrap();
    let info = repo.info();
    assert!(info.is_read_only());
    assert_eq!(repo.create_dir("/dir"), Err(Error::ReadOnly));

    // case #4: change repo password
    let path = base.clone() + "/repo4";
    let new_pwd = "new pwd";
    {
        RepoOpener::new().create(true).open(&path, &pwd).unwrap();
    }
    {
        let mut repo = RepoOpener::new().open(&path, &pwd).unwrap();
        repo.reset_password(
            &pwd,
            &new_pwd,
            OpsLimit::Moderate,
            MemLimit::Interactive,
        ).unwrap();
        let info = repo.info();
        assert_eq!(info.ops_limit(), OpsLimit::Moderate);
        assert_eq!(info.mem_limit(), MemLimit::Interactive);
    }
    RepoOpener::new().open(&path, &pwd).is_err();
    let repo = RepoOpener::new().open(&path, &new_pwd).unwrap();
    let info = repo.info();
    assert_eq!(info.ops_limit(), OpsLimit::Moderate);
    assert_eq!(info.mem_limit(), MemLimit::Interactive);
}
