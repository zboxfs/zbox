extern crate tempdir;

extern crate zbox;

use std::fs;
use tempdir::TempDir;

use zbox::{
    init_env, Cipher, Error, MemLimit, OpenOptions, OpsLimit, RepoOpener,
};

#[test]
fn repo_oper() {
    init_env();

    let pwd = "pwd";
    let tmpdir = TempDir::new("zbox_test").expect("Create temp dir failed");
    let dir = tmpdir.path().to_path_buf();
    //let dir = std::path::PathBuf::from("./tt");
    if dir.exists() {
        fs::remove_dir_all(&dir).unwrap();
    }
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
        .version_limit(5)
        .open(&path, &pwd)
        .unwrap();
    let repo = RepoOpener::new().open(&path, &pwd).unwrap();
    let info = repo.info().unwrap();
    assert_eq!(info.ops_limit(), OpsLimit::Moderate);
    assert_eq!(info.mem_limit(), MemLimit::Moderate);
    assert_eq!(info.cipher(), Cipher::Aes);
    assert_eq!(info.version_limit(), 5);
    assert!(!info.is_read_only());

    // case #3: open repo in read-only mode
    let path = base.clone() + "/repo3";
    {
        assert!(
            RepoOpener::new()
                .create(true)
                .read_only(true)
                .open(&path, &pwd)
                .is_err()
        );
        RepoOpener::new().create(true).open(&path, &pwd).unwrap();
    }
    let mut repo = RepoOpener::new().read_only(true).open(&path, &pwd).unwrap();
    let info = repo.info().unwrap();
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
        let info = repo.info().unwrap();
        assert_eq!(info.ops_limit(), OpsLimit::Moderate);
        assert_eq!(info.mem_limit(), MemLimit::Interactive);
    }
    RepoOpener::new().open(&path, &pwd).is_err();
    let repo = RepoOpener::new().open(&path, &new_pwd).unwrap();
    let info = repo.info().unwrap();
    assert_eq!(info.ops_limit(), OpsLimit::Moderate);
    assert_eq!(info.mem_limit(), MemLimit::Interactive);

    // case #5: open memory storage without create
    {
        assert!(RepoOpener::new().open("mem://foo", &pwd).is_err());
    }

    // case #6: test create_new option
    {
        let path = base.clone() + "/repo6";
        RepoOpener::new()
            .create_new(true)
            .open(&path, &pwd)
            .unwrap();
        assert_eq!(
            RepoOpener::new()
                .create_new(true)
                .open(&path, &pwd)
                .unwrap_err(),
            Error::AlreadyExists
        );
        RepoOpener::new().create(true).open(&path, &pwd).unwrap();
    }

    // case #7: test version_limit option
    {
        let path = base.clone() + "/repo7";
        assert_eq!(
            RepoOpener::new()
                .create_new(true)
                .version_limit(0)
                .open(&path, &pwd)
                .unwrap_err(),
            Error::InvalidArgument
        );
        let mut repo = RepoOpener::new()
            .create_new(true)
            .version_limit(1)
            .open(&path, &pwd)
            .unwrap();

        let buf = [1u8, 2u8, 3u8];
        let buf2 = [4u8, 5u8, 6u8];
        let buf3 = [7u8, 8u8, 9u8];

        let mut f = OpenOptions::new()
            .create(true)
            .open(&mut repo, "/file")
            .unwrap();
        f.write_once(&buf[..]).unwrap();
        f.write_once(&buf2[..]).unwrap();
        let hist = f.history().unwrap();
        assert_eq!(hist.len(), 1);

        let mut f2 = OpenOptions::new()
            .create(true)
            .version_limit(2)
            .open(&mut repo, "/file2")
            .unwrap();
        f2.write_once(&buf[..]).unwrap();
        f2.write_once(&buf2[..]).unwrap();
        f2.write_once(&buf3[..]).unwrap();
        let hist = f2.history().unwrap();
        assert_eq!(hist.len(), 2);
    }

    // case #8: test file read/write after repo is closed
    {
        let path = base.clone() + "/repo8";
        let mut repo = RepoOpener::new()
            .create_new(true)
            .version_limit(1)
            .open(&path, &pwd)
            .unwrap();

        let mut f = OpenOptions::new()
            .create(true)
            .open(&mut repo, "/file")
            .unwrap();

        drop(repo);

        let buf = [1u8, 2u8, 3u8];
        assert_eq!(f.write_once(&buf[..]).unwrap_err(), Error::Closed);
        assert_eq!(f.metadata().unwrap_err(), Error::Closed);
        assert_eq!(f.history().unwrap_err(), Error::Closed);
        assert_eq!(f.curr_version().unwrap_err(), Error::Closed);
    }

    // case #9: test file read/write after repo is dropped
    {
        let path = base.clone() + "/repo9";
        let mut repo = RepoOpener::new()
            .create_new(true)
            .version_limit(1)
            .open(&path, &pwd)
            .unwrap();

        let mut f = OpenOptions::new()
            .create(true)
            .open(&mut repo, "/file")
            .unwrap();

        drop(repo);

        let buf = [1u8, 2u8, 3u8];
        assert_eq!(f.write_once(&buf[..]).unwrap_err(), Error::Closed);
        assert_eq!(f.metadata().unwrap_err(), Error::Closed);
        assert_eq!(f.history().unwrap_err(), Error::Closed);
        assert_eq!(f.curr_version().unwrap_err(), Error::Closed);
    }
}
