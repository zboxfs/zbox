extern crate rand;
extern crate tempdir;
extern crate zbox;

mod common;

use rand::{Rng, XorShiftRng};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::{Arc, RwLock};
use std::thread;
use zbox::{Error, File, OpenOptions};

#[test]
fn file_open_close() {
    let mut env = common::TestEnv::new();
    let mut repo = &mut env.repo;

    let f = OpenOptions::new()
        .create(true)
        .open(&mut repo, "/file")
        .unwrap();
    assert!(f.metadata().unwrap().is_file());
    assert!(repo.path_exists("/file"));
    assert!(repo.is_file("/file"));
}

fn verify_content(f: &mut File, buf: &[u8]) {
    let mut dst = Vec::new();
    let ver_num = f.history().unwrap().last().unwrap().num();
    let mut rdr = f.version_reader(ver_num).unwrap();
    let result = rdr.read_to_end(&mut dst).unwrap();
    assert_eq!(result, buf.len());
    assert_eq!(&dst[..], &buf[..]);
}

#[test]
fn file_read_write() {
    let mut env = common::TestEnv::new();
    let mut repo = &mut env.repo;

    let buf = [1u8, 2u8, 3u8];
    let buf2 = [4u8, 5u8, 6u8, 7u8, 8u8];
    let mut buf3 = Vec::new();
    buf3.extend_from_slice(&buf);
    buf3.extend_from_slice(&buf2);

    // #1, create and write a new file
    {
        let mut f = OpenOptions::new()
            .create(true)
            .open(&mut repo, "/file")
            .unwrap();
        f.write_once(&buf[..]).unwrap();
        verify_content(&mut f, &buf);

        // use repo file creation shortcut
        repo.create_file("/file1.1").unwrap();
        assert_eq!(
            repo.create_file("file1.2").unwrap_err(),
            Error::InvalidPath
        );
    }

    // #2, overwrite file
    {
        let mut f = OpenOptions::new()
            .write(true)
            .open(&mut repo, "/file")
            .unwrap();
        f.write_once(&buf2[..]).unwrap();

        verify_content(&mut f, &buf2);

        let meta = f.metadata().unwrap();
        let hist = f.history().unwrap();
        assert_eq!(meta.len(), buf2.len());
        assert_eq!(meta.curr_version(), 3);
        assert_eq!(hist.len(), 3);
    }

    // #3, append to file
    {
        let mut f = OpenOptions::new()
            .append(true)
            .open(&mut repo, "/file")
            .unwrap();
        f.write_once(&buf[..]).unwrap();

        let mut combo = Vec::new();
        combo.extend_from_slice(&buf2);
        combo.extend_from_slice(&buf);
        verify_content(&mut f, &combo);

        let meta = f.metadata().unwrap();
        let hist = f.history().unwrap();
        assert_eq!(meta.len(), buf.len() + buf2.len());
        assert_eq!(meta.curr_version(), 4);
        assert_eq!(hist.len(), 4);
    }

    // #4, truncate file
    {
        let mut f = OpenOptions::new()
            .write(true)
            .open(&mut repo, "/file")
            .unwrap();
        f.set_len(3).unwrap();

        verify_content(&mut f, &buf2[..3]);

        let meta = f.metadata().unwrap();
        assert_eq!(meta.len(), 3);
        assert_eq!(meta.curr_version(), 5);
    }

    // #5, extend file
    {
        let mut f = OpenOptions::new()
            .write(true)
            .open(&mut repo, "/file")
            .unwrap();
        f.set_len(5).unwrap();

        let mut combo = Vec::new();
        combo.extend_from_slice(&buf2[..3]);
        combo.extend_from_slice(&[0, 0]);
        verify_content(&mut f, &combo);

        let meta = f.metadata().unwrap();
        assert_eq!(meta.len(), 5);
        assert_eq!(meta.curr_version(), 6);
    }

    // #6, set file length to zero
    {
        let mut f = OpenOptions::new()
            .write(true)
            .open(&mut repo, "/file")
            .unwrap();
        f.set_len(0).unwrap();

        let meta = f.metadata().unwrap();
        assert_eq!(meta.len(), 0);
        assert_eq!(meta.curr_version(), 7);
    }

    // #7, create file under another file should fail
    {
        OpenOptions::new()
            .create(true)
            .open(&mut repo, "/file/new")
            .is_err();

        // write data to the file should okay
        let mut f = OpenOptions::new()
            .write(true)
            .open(&mut repo, "/file")
            .unwrap();
        f.write_once(&buf[..]).unwrap();
    }

    // #8, finish without in writing
    {
        let mut f = OpenOptions::new()
            .create(true)
            .open(&mut repo, "/file8")
            .unwrap();
        assert_eq!(f.finish().unwrap_err(), Error::NotWrite);
    }

    // #9, test create_new open flag
    {
        OpenOptions::new()
            .create_new(true)
            .open(&mut repo, "/file9")
            .unwrap();
        assert_eq!(
            OpenOptions::new()
                .create_new(true)
                .open(&mut repo, "/file9")
                .unwrap_err(),
            Error::AlreadyExists
        );
        OpenOptions::new()
            .create(true)
            .open(&mut repo, "/file9")
            .unwrap();
    }

    // #10, file do not keep history
    {
        assert_eq!(
            OpenOptions::new()
                .create(true)
                .version_limit(0)
                .open(&mut repo, "/file10")
                .unwrap_err(),
            Error::InvalidArgument
        );

        let mut f = OpenOptions::new()
            .create(true)
            .version_limit(1)
            .open(&mut repo, "/file10")
            .unwrap();

        f.write_once(&buf[..]).unwrap();
        f.write_once(&buf2[..]).unwrap();

        verify_content(&mut f, &buf3);

        let meta = f.metadata().unwrap();
        assert_eq!(meta.len(), buf3.len());
        assert_eq!(f.history().unwrap().len(), 1);
    }

    // #11, version reader
    {
        let mut f = OpenOptions::new()
            .create(true)
            .version_limit(2)
            .open(&mut repo, "/file11")
            .unwrap();

        f.write_once(&buf[..]).unwrap();
        f.write_once(&buf2[..]).unwrap();

        verify_content(&mut f, &buf3);

        let meta = f.metadata().unwrap();
        let history = f.history().unwrap();
        assert_eq!(meta.len(), buf3.len());
        assert_eq!(history.len(), 2);

        let ver_num = history.last().unwrap().num();
        let mut rdr = f.version_reader(ver_num).unwrap();
        let mut dst = Vec::new();
        let result = rdr.read_to_end(&mut dst).unwrap();
        assert_eq!(result, buf3.len());
        assert_eq!(&dst[..], &buf3[..]);

        let mut rdr = f.version_reader(ver_num - 1).unwrap();
        let mut dst = Vec::new();
        let result = rdr.read_to_end(&mut dst).unwrap();
        assert_eq!(result, buf.len());
        assert_eq!(&dst[..], &buf[..]);
    }

    // #12, single-part write
    {
        let mut f = OpenOptions::new()
            .create(true)
            .open(&mut repo, "/file12")
            .unwrap();

        f.write_once(&buf[..]).unwrap();
        f.write_once(&buf2[..]).unwrap();

        let curr_ver = f.curr_version().unwrap();

        let mut rdr = f.version_reader(curr_ver).unwrap();
        let mut dst = Vec::new();
        rdr.read_to_end(&mut dst).unwrap();
        assert_eq!(&dst[..], &buf3[..]);

        let mut rdr = f.version_reader(curr_ver - 1).unwrap();
        let mut dst = Vec::new();
        rdr.read_to_end(&mut dst).unwrap();
        assert_eq!(&dst[..], &buf[..]);
    }

    // #13, write-only file
    {
        let mut f = OpenOptions::new()
            .read(false)
            .create(true)
            .open(&mut repo, "/file13")
            .unwrap();
        f.write_once(&buf[..]).unwrap();
        let mut dst = Vec::new();
        assert!(f.read_to_end(&mut dst).is_err());
    }

    // #14, write and set_len
    {
        let mut f = OpenOptions::new()
            .create(true)
            .version_limit(1)
            .open(&mut repo, "/file14")
            .unwrap();
        let mut f2 = OpenOptions::new()
            .create(true)
            .version_limit(1)
            .open(&mut repo, "/file14-1")
            .unwrap();

        f2.write_once(&buf[..1]).unwrap();
        verify_content(&mut f2, &buf[..1]);

        f.write_once(&buf[..]).unwrap();
        verify_content(&mut f, &buf[..]);
        f.set_len(1).unwrap();
        verify_content(&mut f, &buf[..1]);

        // note here file position is 3 which is beyond EOF,
        // the file is extended with zero and then write with data
        f.write_once(&buf[..]).unwrap();
        verify_content(&mut f, &[1, 0, 0, 1, 2, 3]);
    }

    // #15, test create open flag
    {
        assert!(!repo.is_file("/file15"));
        assert_eq!(
            OpenOptions::new()
                .create(false)
                .open(&mut repo, "/file15")
                .unwrap_err(),
            Error::NotFound
        );
        OpenOptions::new()
            .create(true)
            .open(&mut repo, "/file15")
            .unwrap();
        OpenOptions::new()
            .create(false)
            .open(&mut repo, "/file9")
            .unwrap();
    }

    // #16, read while file is in writing
    {
        let mut f = OpenOptions::new()
            .create(true)
            .open(&mut repo, "/file16")
            .unwrap();
        f.write_once(&[0, 1, 2, 3, 4, 5, 6]).unwrap();

        f.seek(SeekFrom::Start(1)).unwrap();
        f.write_once(&[20, 21]).unwrap();

        // now the file position is 3
        let mut buf = [0u8; 1];
        f.read_exact(&mut buf).unwrap();
        assert_eq!(&buf[..], &[3]);

        // create a new version and continue read
        f.write_once(&[30, 31]).unwrap();
        f.read_exact(&mut buf).unwrap();
        assert_eq!(&buf[..], &[6]);

        verify_content(&mut f, &[0, 20, 21, 3, 30, 31, 6]);
    }

    // #17, read after set_len
    {
        let mut f = OpenOptions::new()
            .create(true)
            .open(&mut repo, "/file17")
            .unwrap();
        f.write_once(&[0, 1, 2, 3]).unwrap();

        let mut buf = [0u8; 2];
        f.seek(SeekFrom::Start(1)).unwrap();
        f.read_exact(&mut buf).unwrap();
        assert_eq!(&buf[..], &[1, 2]);

        f.set_len(2).unwrap();

        let mut buf = Vec::new();
        assert_eq!(f.read_to_end(&mut buf).unwrap(), 0);
    }
}

#[test]
fn file_read_write_mt() {
    let env_ref = Arc::new(RwLock::new(common::TestEnv::new()));
    let worker_cnt = 4;
    let task_cnt = 8;

    // concurrent write to different files
    let mut workers = Vec::new();
    for i in 0..worker_cnt {
        let env = env_ref.clone();
        workers.push(thread::spawn(move || {
            let base = i * task_cnt;
            for j in base..base + task_cnt {
                let path = format!("/{}", j);
                let buf = [j; 3];
                let mut env = env.write().unwrap();
                let mut f = OpenOptions::new()
                    .create(true)
                    .open(&mut env.repo, &path)
                    .unwrap();
                f.write_once(&buf[..]).unwrap();
            }
        }));
    }
    for w in workers {
        w.join().unwrap();
    }

    // concurrent read different files
    let mut workers = Vec::new();
    for i in 0..worker_cnt {
        let env = env_ref.clone();
        workers.push(thread::spawn(move || {
            let base = i * task_cnt;
            for j in base..base + task_cnt {
                let path = format!("/{}", j);
                let buf = [j; 3];
                let mut env = env.write().unwrap();
                let mut f = env.repo.open_file(&path).unwrap();
                let mut dst = Vec::new();
                let result = f.read_to_end(&mut dst).unwrap();
                assert_eq!(result, buf.len());
                assert_eq!(&dst[..], &buf[..]);
            }
        }));
    }
    for w in workers {
        w.join().unwrap();
    }

    // concurrent write to same file
    let mut workers = Vec::new();
    for i in 0..worker_cnt {
        let env = env_ref.clone();
        workers.push(thread::spawn(move || {
            let buf = [i; 3];
            let mut env = env.write().unwrap();
            let mut f = OpenOptions::new()
                .create(true)
                .open(&mut env.repo, "/99")
                .unwrap();
            f.write_once(&buf[..]).unwrap();
        }));
    }
    for w in workers {
        w.join().unwrap();
    }

    // concurrent read same file
    {
        // reset file content
        let buf = [99u8; 3];
        let mut env = env_ref.write().unwrap();
        let mut f = OpenOptions::new()
            .write(true)
            .open(&mut env.repo, "/99")
            .unwrap();
        f.write_once(&buf[..]).unwrap();
    }
    let mut workers = Vec::new();
    for _ in 0..worker_cnt {
        let mut env = env_ref.write().unwrap();
        let mut f = env.repo.open_file("/99").unwrap();
        workers.push(thread::spawn(move || {
            let buf = [99u8; 3];
            let mut dst = Vec::new();
            let result = f.read_to_end(&mut dst).unwrap();
            assert_eq!(result, buf.len());
            assert_eq!(&dst[..], &buf[..]);
        }));
    }
    for w in workers {
        w.join().unwrap();
    }
}

#[test]
fn file_content_dedup() {
    let mut env = common::TestEnv::new();
    let mut repo = &mut env.repo;

    let buf = [42u8; 16];

    {
        let mut f = OpenOptions::new()
            .create(true)
            .version_limit(1)
            .open(&mut repo, "/file")
            .unwrap();
        let mut f2 = OpenOptions::new()
            .create(true)
            .version_limit(1)
            .open(&mut repo, "/file2")
            .unwrap();
        let mut f3 = OpenOptions::new()
            .create(true)
            .version_limit(1)
            .open(&mut repo, "/file3")
            .unwrap();

        // Those should all point to same content, but how do we verify it?
        // Probably need to inject some debug println() in fnode.rs.
        f.write_once(&buf).unwrap();
        f2.write_once(&buf).unwrap();
        f.write_once(&buf).unwrap();
        f2.write_once(&buf).unwrap();
        f.write_once(&buf).unwrap();
        f3.write_once(&buf).unwrap();
    }
}

#[test]
fn file_truncate() {
    let mut env = common::TestEnv::new();
    let mut repo = &mut env.repo;

    let buf = [1u8, 2u8, 3u8];

    // write
    {
        let mut f = OpenOptions::new()
            .create(true)
            .open(&mut repo, "/file")
            .unwrap();
        f.write_once(&buf[..]).unwrap();
    }

    // open file in truncate mode
    {
        let mut f = OpenOptions::new()
            .truncate(true)
            .open(&mut repo, "/file")
            .unwrap();

        let meta = f.metadata().unwrap();
        assert_eq!(meta.len(), 0);
        assert_eq!(meta.curr_version(), 3);

        // write some data
        f.write_once(&buf[..]).unwrap();
        let meta = f.metadata().unwrap();
        assert_eq!(meta.len(), 3);
        assert_eq!(meta.curr_version(), 4);

        // then truncate again
        f.set_len(0).unwrap();
        let meta = f.metadata().unwrap();
        assert_eq!(meta.len(), 0);
        assert_eq!(meta.curr_version(), 5);
    }
}

#[test]
fn file_shrink() {
    let mut env = common::TestEnv::new();
    let mut repo = &mut env.repo;

    let mut rng = XorShiftRng::new_unseeded();
    let mut buf = vec![0; 16 * 1024 * 1024];
    rng.fill_bytes(&mut buf);

    let mut f = OpenOptions::new()
        .create(true)
        .version_limit(1)
        .open(&mut repo, "/file")
        .unwrap();
    f.write_once(&buf[..]).unwrap();

    // those operations will shrink the segment, turn on debug log
    // and watch the output
    f.set_len(3).unwrap();
    f.set_len(2).unwrap();
    f.set_len(1).unwrap();
}

#[test]
fn file_copy() {
    let mut env = common::TestEnv::new();
    let mut repo = &mut env.repo;

    let buf = [1u8, 2u8, 3u8];

    {
        let mut f = OpenOptions::new()
            .create(true)
            .open(&mut repo, "/file")
            .unwrap();
        f.write_once(&buf[..]).unwrap();
    }

    // #1, copy to non-existing file
    repo.copy("/file", "/file2").unwrap();

    // #2, copy to existing file
    repo.copy("/file", "/file2").unwrap();
    {
        let mut f = repo.open_file("/file2").unwrap();
        let mut dst = Vec::new();
        let result = f.read_to_end(&mut dst).unwrap();
        assert_eq!(result, buf.len());
        assert_eq!(&dst[..], &buf[..]);
    }

    // #3, copy to file itself
    repo.copy("/file", "/file").unwrap();
    {
        let mut f = repo.open_file("/file").unwrap();
        let mut dst = Vec::new();
        let result = f.read_to_end(&mut dst).unwrap();
        assert_eq!(result, buf.len());
        assert_eq!(&dst[..], &buf[..]);
    }
}

#[test]
fn file_seek() {
    let mut env = common::TestEnv::new();
    let mut repo = &mut env.repo;

    let buf = [1u8, 2u8, 3u8];

    // write
    {
        let mut f = OpenOptions::new()
            .create(true)
            .open(&mut repo, "/file")
            .unwrap();
        f.write_once(&buf[..]).unwrap();
    }

    // #1: seek and read
    {
        let mut f = repo.open_file("/file").unwrap();

        // seek from start
        let mut dst = Vec::new();
        let pos = f.seek(SeekFrom::Start(2)).unwrap();
        assert_eq!(pos, 2);
        let result = f.read_to_end(&mut dst).unwrap();
        assert_eq!(result, buf.len() - 2);
        assert_eq!(&dst[..], &buf[2..]);

        // seek from end
        let mut dst = Vec::new();
        let pos = f.seek(SeekFrom::End(-2)).unwrap();
        assert_eq!(pos, 1);
        let result = f.read_to_end(&mut dst).unwrap();
        assert_eq!(result, buf.len() - 1);
        assert_eq!(&dst[..], &buf[1..]);

        // seek from current
        let mut dst = Vec::new();
        let pos = f.seek(SeekFrom::Start(1)).unwrap();
        assert_eq!(pos, 1);
        let pos = f.seek(SeekFrom::Current(1)).unwrap();
        assert_eq!(pos, 2);
        let result = f.read_to_end(&mut dst).unwrap();
        assert_eq!(result, buf.len() - 2);
        assert_eq!(&dst[..], &buf[2..]);
    }

    // #2: seek and write in middle
    {
        let mut f = OpenOptions::new()
            .write(true)
            .open(&mut repo, "/file")
            .unwrap();
        f.seek(SeekFrom::Start(1)).unwrap();
        f.write_all(&buf[..]).unwrap();
        assert!(f.seek(SeekFrom::Start(1)).is_err());
        f.finish().unwrap();

        // verify
        let mut dst = Vec::new();
        f.seek(SeekFrom::Start(0)).unwrap();
        let result = f.read_to_end(&mut dst).unwrap();
        assert_eq!(result, buf.len() + 1);
        assert_eq!(&dst[..], &[1, 1, 2, 3]);

        dst.clear();
        f.seek(SeekFrom::Start(1)).unwrap();
        f.read_to_end(&mut dst).unwrap();
        assert_eq!(&dst[..], &buf[..]);
    }

    // #3: seek and write beyond EOF
    {
        // make test file
        let mut f = OpenOptions::new()
            .create(true)
            .open(&mut repo, "/file3")
            .unwrap();
        f.write_once(&buf[..]).unwrap();

        // seek beyond EOF and write
        let mut f = OpenOptions::new()
            .write(true)
            .open(&mut repo, "/file3")
            .unwrap();
        f.seek(SeekFrom::Start(buf.len() as u64 + 1)).unwrap();
        f.write_once(&buf[..]).unwrap();

        // verify
        let mut dst = Vec::new();
        f.seek(SeekFrom::Start(0)).unwrap();
        let result = f.read_to_end(&mut dst).unwrap();
        assert_eq!(result, buf.len() * 2 + 1);
        assert_eq!(&dst[..], &[1, 2, 3, 0, 1, 2, 3]);
    }
}

#[test]
fn file_delete() {
    let mut env = common::TestEnv::new();
    let mut repo = &mut env.repo;

    // create empty file then delete
    {
        OpenOptions::new()
            .create(true)
            .open(&mut repo, "/file")
            .unwrap();
        repo.remove_file("/file").unwrap();
    }

    // write to file then delete
    {
        // write #1
        {
            let mut f = OpenOptions::new()
                .create(true)
                .open(&mut repo, "/file")
                .unwrap();
            f.write_once(&[1u8, 2u8, 3u8]).unwrap();
        }

        // write #2
        {
            let mut f = OpenOptions::new()
                .write(true)
                .open(&mut repo, "/file")
                .unwrap();
            f.write_once(&[4u8, 5u8, 6u8, 7u8]).unwrap();
        }

        repo.remove_file("/file").unwrap();
    }
}

#[test]
fn file_rename() {
    let mut env = common::TestEnv::new();
    let repo = &mut env.repo;

    // #1, rename non-existing file
    {
        assert_eq!(
            repo.rename("/non-existing", "/foo").unwrap_err(),
            Error::NotFound
        );
    }

    // #2, rename existing file to non-existing file
    {
        repo.create_file("/file2").unwrap();
        repo.rename("/file2", "/file2a").unwrap();

        assert!(!repo.path_exists("/file2"));
        assert!(repo.path_exists("/file2a"));
        let dirs = repo.read_dir("/").unwrap();
        assert_eq!(dirs.len(), 1);
        assert_eq!(dirs[0].path().to_str().unwrap(), "/file2a");
    }

    // #3, rename existing file to existing file
    {
        let buf = [1u8, 2u8, 3u8];
        let mut f = OpenOptions::new()
            .create(true)
            .open(repo, "/file3")
            .unwrap();
        f.write_once(&buf[..]).unwrap();
        repo.create_file("/file3a").unwrap();
        repo.rename("/file3", "/file3a").unwrap();

        assert!(!repo.path_exists("/file3"));
        assert!(repo.path_exists("/file3a"));
        let dirs = repo.read_dir("/").unwrap();
        assert_eq!(dirs.len(), 2);

        let mut f = OpenOptions::new().open(repo, "/file3a").unwrap();
        verify_content(&mut f, &buf);
    }

    // #4, rename existing file to dir
    {
        repo.create_file("/file4").unwrap();
        repo.create_dir("/dir4").unwrap();
        assert_eq!(repo.rename("/file4", "/dir4").unwrap_err(), Error::IsDir);

        assert!(repo.path_exists("/file4"));
        assert!(repo.path_exists("/dir4"));
    }

    // #5, rename existing dir to file
    {
        repo.create_dir("/dir5").unwrap();
        repo.create_file("/file5").unwrap();
        assert_eq!(repo.rename("/dir5", "/file5").unwrap_err(), Error::NotDir);

        assert!(repo.path_exists("/file5"));
        assert!(repo.path_exists("/dir5"));
    }

    // #6, rename existing file to itself
    {
        repo.create_file("/file6").unwrap();
        repo.rename("/file6", "/file6").unwrap();
        assert!(repo.path_exists("/file6"));
    }
}
