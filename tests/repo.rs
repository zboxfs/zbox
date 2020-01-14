#![cfg(any(
    feature = "storage-mem",
    feature = "storage-file",
    feature = "storage-sqlite",
    feature = "storage-redis"
))]

extern crate tempdir;

extern crate zbox;

use std::io::{Read, Seek, SeekFrom};
use tempdir::TempDir;
#[allow(unused_imports)]
use zbox::{
    init_env, Cipher, Error, MemLimit, OpenOptions, OpsLimit, Repo, RepoOpener,
};

#[cfg(all(
    any(
        feature = "storage-mem",
        feature = "storage-file",
        feature = "storage-sqlite"
    ),
    not(feature = "storage-redis")
))]
#[test]
fn repo_oper() {
    init_env();

    let pwd = "pwd";
    let tmpdir = TempDir::new("zbox_test").expect("Create temp dir failed");
    let dir = tmpdir.path().to_path_buf();
    let base = {
        #[cfg(all(
            feature = "storage-mem",
            not(feature = "storage-file"),
            not(feature = "storage-sqlite"),
            not(feature = "storage-redis")
        ))]
        {
            "mem://".to_string()
        }

        #[cfg(feature = "storage-file")]
        {
            "file://".to_string() + dir.to_str().unwrap()
        }

        #[cfg(feature = "storage-sqlite")]
        {
            "sqlite://".to_string() + dir.to_str().unwrap()
        }
    };

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
        assert!(RepoOpener::new()
            .create(true)
            .read_only(true)
            .open(&path, &pwd)
            .is_err());
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
        )
        .unwrap();
        let info = repo.info().unwrap();
        assert_eq!(info.ops_limit(), OpsLimit::Moderate);
        assert_eq!(info.mem_limit(), MemLimit::Interactive);
    }
    RepoOpener::new().open(&path, &pwd).unwrap_err();
    let repo = RepoOpener::new().open(&path, &new_pwd).unwrap();
    let info = repo.info().unwrap();
    assert_eq!(info.ops_limit(), OpsLimit::Moderate);
    assert_eq!(info.mem_limit(), MemLimit::Interactive);

    // case #5: open memory storage without create
    {
        assert!(RepoOpener::new().open("mem://tests.repo", &pwd).is_err());
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
            Error::RepoExists
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
        assert_eq!(f.write_once(&buf[..]).unwrap_err(), Error::RepoClosed);
        assert_eq!(f.metadata().unwrap_err(), Error::RepoClosed);
        assert_eq!(f.history().unwrap_err(), Error::RepoClosed);
        assert_eq!(f.curr_version().unwrap_err(), Error::RepoClosed);
        assert_eq!(f.set_len(42).unwrap_err(), Error::RepoClosed);
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
        assert_eq!(f.write_once(&buf[..]).unwrap_err(), Error::RepoClosed);
        assert_eq!(f.metadata().unwrap_err(), Error::RepoClosed);
        assert_eq!(f.history().unwrap_err(), Error::RepoClosed);
        assert_eq!(f.curr_version().unwrap_err(), Error::RepoClosed);
    }

    // case #10: test repair_super_block()
    {
        let path = base.clone() + "/repo10";
        let repo = RepoOpener::new()
            .create_new(true)
            .open(&path, &pwd)
            .unwrap();

        drop(repo);

        Repo::repair_super_block(&path, &pwd).unwrap();
    }

    // case #11: test repo exclusive access
    {
        let path = base.clone() + "/repo11";
        let _repo = RepoOpener::new()
            .create_new(true)
            .open(&path, &pwd)
            .unwrap();
        assert_eq!(
            RepoOpener::new().open(&path, &pwd).unwrap_err(),
            Error::RepoOpened
        );
    }

    // case #12: test force open repo
    {
        let path = base.clone() + "/repo12";
        let _repo = RepoOpener::new()
            .create_new(true)
            .open(&path, &pwd)
            .unwrap();
        let _repo2 = RepoOpener::new().force(true).open(&path, &pwd).unwrap();
    }

    // case #13: test destroy repo
    {
        let path = base.clone() + "/repo13";
        {
            let _repo = RepoOpener::new()
                .create_new(true)
                .open(&path, &pwd)
                .unwrap();
        }
        Repo::destroy(&path).unwrap();
        assert!(RepoOpener::new().open(&path, &pwd).is_err());
    }

    // to suppress unused variable warning
    drop(dir);
    drop(tmpdir);
}

fn smoke_test(uri: String) {
    init_env();

    // initialise repo and write file
    {
        let mut repo =
            RepoOpener::new().create(true).open(&uri, "pwd").unwrap();

        let mut file = OpenOptions::new()
            .create(true)
            .open(&mut repo, "/my_file.txt")
            .unwrap();

        file.write_once(b"Hello, World!").unwrap();

        // read file content using std::io::Read trait
        let mut content = String::new();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.read_to_string(&mut content).unwrap();
        assert_eq!(content, "Hello, World!");
    }

    // open repo again and read file
    {
        let mut repo =
            RepoOpener::new().create(false).open(&uri, "pwd").unwrap();

        let mut file = OpenOptions::new()
            .create(false)
            .open(&mut repo, "/my_file.txt")
            .unwrap();

        // read file content using std::io::Read trait
        let mut content = String::new();
        file.read_to_string(&mut content).unwrap();
        assert_eq!(content, "Hello, World!");

        // list dir
        let dirs = repo.read_dir("/").unwrap();
        assert_eq!(dirs.len(), 1);
    }

    // destroy repo
    {
        Repo::destroy(&uri).unwrap();
        assert!(RepoOpener::new().open(&uri, "pwd").is_err());
    }
}

#[test]
fn repo_smoke_test() {
    let tmpdir = TempDir::new("zbox_test").expect("Create temp dir failed");

    let uri = {
        #[cfg(all(
            feature = "storage-mem",
            not(feature = "storage-file"),
            not(feature = "storage-sqlite"),
            not(feature = "storage-redis")
        ))]
        {
            "mem://repo_smoke_test".to_string()
        }
        #[cfg(feature = "storage-file")]
        {
            let base = "file://".to_string() + tmpdir.path().to_str().unwrap();
            base + "/repo"
        }

        #[cfg(feature = "storage-sqlite")]
        {
            let file = tmpdir.path().join("zbox.db");
            "sqlite://".to_string() + file.to_str().unwrap()
        }

        #[cfg(feature = "storage-redis")]
        {
            "redis://localhost:6379".to_string()
        }
    };

    smoke_test(uri);

    // to suppress unused variable warning
    drop(tmpdir);
}
