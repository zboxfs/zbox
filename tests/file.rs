extern crate tempdir;
extern crate zbox;

mod common;

use std::io::{Read, Write, Seek, SeekFrom};
use std::sync::{Arc, RwLock};
use std::thread;
use zbox::{Error, OpenOptions};

#[test]
fn file_open_close() {
    let mut env = common::setup();
    let mut repo = &mut env.repo;

    let f = OpenOptions::new()
        .create(true)
        .open(&mut repo, "/file")
        .unwrap();
    assert!(f.metadata().is_file());
    assert!(repo.path_exists("/file"));
    assert!(repo.is_file("/file"));
}

#[test]
fn file_read_write() {
    let mut env = common::setup();
    let mut repo = &mut env.repo;

    let buf = [1u8, 2u8, 3u8];
    let buf2 = [4u8, 5u8, 6u8, 7u8, 8u8];

    // #1, create and write a new file
    {
        let mut f = OpenOptions::new()
            .create(true)
            .open(&mut repo, "/file")
            .unwrap();
        f.write_all(&buf[..]).unwrap();
        f.finish().unwrap();

        let mut dst = Vec::new();
        let result = f.read_to_end(&mut dst).unwrap();
        assert_eq!(result, buf.len());
        assert_eq!(&dst[..], &buf[..]);

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
        f.write_all(&buf2[..]).unwrap();
        f.finish().unwrap();

        let mut dst = Vec::new();
        let result = f.read_to_end(&mut dst).unwrap();
        assert_eq!(result, buf2.len());
        assert_eq!(&dst[..], &buf2[..]);

        let meta = f.metadata();
        let hist = f.history();
        assert_eq!(meta.len(), buf2.len());
        assert_eq!(meta.version(), 3);
        assert_eq!(hist.len(), 3);
    }

    // #3, append to file
    {
        let mut f = OpenOptions::new()
            .append(true)
            .open(&mut repo, "/file")
            .unwrap();
        f.write_all(&buf[..]).unwrap();
        f.finish().unwrap();

        let mut dst = Vec::new();
        let result = f.read_to_end(&mut dst).unwrap();
        assert_eq!(result, buf.len() + buf2.len());
        assert_eq!(&dst[..buf2.len()], &buf2[..]);
        assert_eq!(&dst[buf2.len()..], &buf[..]);

        let meta = f.metadata();
        let hist = f.history();
        assert_eq!(meta.len(), buf.len() + buf2.len());
        assert_eq!(meta.version(), 4);
        assert_eq!(hist.len(), 4);
    }

    // #4, truncate file
    {
        let mut f = OpenOptions::new()
            .write(true)
            .open(&mut repo, "/file")
            .unwrap();
        f.set_len(3).unwrap();

        let mut dst = Vec::new();
        let result = f.read_to_end(&mut dst).unwrap();
        assert_eq!(result, 3);
        assert_eq!(&dst[..], &buf2[..3]);

        let meta = f.metadata();
        assert_eq!(meta.len(), 3);
        assert_eq!(meta.version(), 5);
    }

    // #5, extend file
    {
        let mut f = OpenOptions::new()
            .write(true)
            .open(&mut repo, "/file")
            .unwrap();
        f.set_len(5).unwrap();

        let mut dst = Vec::new();
        let result = f.read_to_end(&mut dst).unwrap();
        assert_eq!(result, 5);
        assert_eq!(&dst[..3], &buf2[..3]);
        assert_eq!(&dst[3..], &[0, 0]);

        let meta = f.metadata();
        assert_eq!(meta.len(), 5);
        assert_eq!(meta.version(), 6);
    }

    // #6, set file length to zero
    {
        let mut f = OpenOptions::new()
            .write(true)
            .open(&mut repo, "/file")
            .unwrap();
        f.set_len(0).unwrap();

        let meta = f.metadata();
        assert_eq!(meta.len(), 0);
        assert_eq!(meta.version(), 7);
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
        f.write_all(&buf[..]).unwrap();
        f.finish().unwrap();
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

        f.write_all(&buf[..]).unwrap();
        f.finish().unwrap();

        f.write_all(&buf2[..]).unwrap();
        f.finish().unwrap();

        let mut dst = Vec::new();
        let result = f.read_to_end(&mut dst).unwrap();
        assert_eq!(result, buf2.len());
        assert_eq!(&dst[..], &buf2[..]);

        let meta = f.metadata();
        assert_eq!(meta.len(), buf2.len());
        assert_eq!(f.history().len(), 1);
    }

    // #11, version reader
    {
        let mut f = OpenOptions::new()
            .create(true)
            .version_limit(2)
            .open(&mut repo, "/file11")
            .unwrap();

        f.write_all(&buf[..]).unwrap();
        f.finish().unwrap();

        f.write_all(&buf2[..]).unwrap();
        f.finish().unwrap();

        let mut dst = Vec::new();
        let result = f.read_to_end(&mut dst).unwrap();
        assert_eq!(result, buf2.len());
        assert_eq!(&dst[..], &buf2[..]);

        let meta = f.metadata();
        let history = f.history();
        assert_eq!(meta.len(), buf2.len());
        assert_eq!(history.len(), 2);

        let ver_num = history.last().unwrap().num();
        let mut rdr = f.version_reader(ver_num).unwrap();
        let mut dst = Vec::new();
        let result = rdr.read_to_end(&mut dst).unwrap();
        assert_eq!(result, buf2.len());
        assert_eq!(&dst[..], &buf2[..]);

        let mut rdr = f.version_reader(ver_num - 1).unwrap();
        let mut dst = Vec::new();
        let result = rdr.read_to_end(&mut dst).unwrap();
        assert_eq!(result, buf.len());
        assert_eq!(&dst[..], &buf[..]);
    }
}

#[test]
fn file_read_write_mt() {
    let env_ref = Arc::new(RwLock::new(common::setup()));
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
                f.write_all(&buf[..]).unwrap();
                f.finish().unwrap();
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
            f.write_all(&buf[..]).unwrap();
            f.finish().unwrap();
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
        f.write_all(&buf[..]).unwrap();
        f.finish().unwrap();
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
fn file_truncate() {
    let mut env = common::setup();
    let mut repo = &mut env.repo;

    let buf = [1u8, 2u8, 3u8];

    // write
    {
        let mut f = OpenOptions::new()
            .create(true)
            .open(&mut repo, "/file")
            .unwrap();
        f.write_all(&buf[..]).unwrap();
        f.finish().unwrap();
    }

    // open file in truncate mode
    {
        let mut f = OpenOptions::new()
            .truncate(true)
            .open(&mut repo, "/file")
            .unwrap();

        let meta = f.metadata();
        assert_eq!(meta.len(), 0);
        assert_eq!(meta.version(), 3);

        // write some data
        f.write_all(&buf[..]).unwrap();
        f.finish().unwrap();
        let meta = f.metadata();
        assert_eq!(meta.len(), 3);
        assert_eq!(meta.version(), 4);

        // then truncate again
        f.set_len(0).unwrap();
        let meta = f.metadata();
        assert_eq!(meta.len(), 0);
        assert_eq!(meta.version(), 5);
    }
}

#[test]
fn file_copy() {
    let mut env = common::setup();
    let mut repo = &mut env.repo;

    let buf = [1u8, 2u8, 3u8];

    {
        let mut f = OpenOptions::new()
            .create(true)
            .open(&mut repo, "/file")
            .unwrap();
        f.write_all(&buf[..]).unwrap();
        f.finish().unwrap();
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
    let mut env = common::setup();
    let mut repo = &mut env.repo;

    let buf = [1u8, 2u8, 3u8];

    // write
    {
        let mut f = OpenOptions::new()
            .create(true)
            .open(&mut repo, "/file")
            .unwrap();
        f.write_all(&buf[..]).unwrap();
        f.finish().unwrap();
    }

    // #1: seek and read
    {
        let mut f = repo.open_file("/file").unwrap();

        // seek from start
        let mut dst = Vec::new();
        f.seek(SeekFrom::Start(2)).unwrap();
        let result = f.read_to_end(&mut dst).unwrap();
        assert_eq!(result, buf.len() - 2);
        assert_eq!(&dst[..], &buf[2..]);

        // seek from end
        let mut dst = Vec::new();
        f.seek(SeekFrom::End(-2)).unwrap();
        let result = f.read_to_end(&mut dst).unwrap();
        assert_eq!(result, buf.len() - 1);
        assert_eq!(&dst[..], &buf[1..]);

        // seek from current
        let mut dst = Vec::new();
        f.seek(SeekFrom::Start(1)).unwrap();
        f.seek(SeekFrom::Current(1)).unwrap();
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
        f.write_all(&buf[..]).unwrap();
        f.finish().unwrap();

        // seek beyond EOF and write
        let mut f = OpenOptions::new()
            .write(true)
            .open(&mut repo, "/file3")
            .unwrap();
        f.seek(SeekFrom::Start(buf.len() as u64 + 1)).unwrap();
        f.write_all(&buf[..]).unwrap();
        f.finish().unwrap();

        // verify
        let mut dst = Vec::new();
        let result = f.read_to_end(&mut dst).unwrap();
        assert_eq!(result, buf.len() * 2 + 1);
        assert_eq!(&dst[..], &[1, 2, 3, 0, 1, 2, 3]);
    }
}

#[test]
fn file_delete() {
    let mut env = common::setup();
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
            f.write_all(&[1u8, 2u8, 3u8]).unwrap();
            f.finish().unwrap();
        }

        // write #2
        {
            let mut f = OpenOptions::new()
                .write(true)
                .open(&mut repo, "/file")
                .unwrap();
            f.write_all(&[4u8, 5u8, 6u8, 7u8]).unwrap();
            f.finish().unwrap();
        }

        let result = repo.remove_file("/file");
        assert!(result.is_ok());
    }
}

#[test]
fn file_rename() {
    let mut env = common::setup();
    let mut repo = &mut env.repo;

    {
        OpenOptions::new()
            .create(true)
            .open(&mut repo, "/file")
            .unwrap();
        repo.rename("/file", "/file2").unwrap();

        repo.open_file("/file").is_err();
        repo.open_file("/file2").unwrap();
    }

    {
        OpenOptions::new()
            .create(true)
            .open(&mut repo, "/file")
            .unwrap();
        repo.rename("/file", "/file2").is_err();
    }
}
