#[cfg(not(feature = "vio-test"))]
pub mod imp {
    pub use std::fs::{copy, create_dir, create_dir_all, read_dir, remove_dir,
                      remove_dir_all, remove_file, rename, File, OpenOptions,
                      ReadDir};
}

#[cfg(feature = "vio-test")]
pub mod imp {

    use std::fs;
    use std::io::{Error, ErrorKind, Read, Result, Seek, SeekFrom, Write};
    use std::path::Path;

    use base::crypto::{Crypto, RandomSeed};

    const ERR_SAMPLE_CNT: usize = 256;
    static mut ERR_FLAG: bool = false;
    static mut ERR_SAMPLES: [u8; ERR_SAMPLE_CNT] = [0u8; ERR_SAMPLE_CNT];
    static mut ERR_INDEX: usize = 0;

    pub fn turn_on_random_error() {
        unsafe {
            ERR_FLAG = true;
        }
    }

    pub fn turn_off_random_error() {
        unsafe {
            ERR_FLAG = false;
        }
    }

    pub fn reset_random_error(seed: &RandomSeed) {
        unsafe {
            Crypto::random_buf_deterministic(&mut ERR_SAMPLES[..], seed);
            ERR_FLAG = false;
            ERR_INDEX = 0;
        }
    }

    // randomly raise io error
    fn make_random_error() -> Result<()> {
        unsafe {
            if ERR_FLAG {
                let sample = ERR_SAMPLES[ERR_INDEX];
                ERR_INDEX = (ERR_INDEX + 1) % ERR_SAMPLE_CNT;
                // 42 is a magic number ;)
                return match sample {
                    42 => Err(Error::new(
                        ErrorKind::Other,
                        "Test random IO error",
                    )),
                    _ => Ok(()),
                };
            }
        }
        Ok(())
    }

    pub struct File {
        inner: fs::File,
    }

    impl File {
        pub fn open<P: AsRef<Path>>(path: P) -> Result<File> {
            make_random_error()?;
            let inner = fs::File::open(path)?;
            Ok(File { inner })
        }

        pub fn metadata(&self) -> Result<fs::Metadata> {
            make_random_error()?;
            self.inner.metadata()
        }

        pub fn sync_all(&self) -> Result<()> {
            make_random_error()?;
            self.inner.sync_all()
        }
    }

    impl Read for File {
        fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
            make_random_error()?;
            self.inner.read(buf)
        }
    }

    impl Write for File {
        fn write(&mut self, buf: &[u8]) -> Result<usize> {
            make_random_error()?;
            self.inner.write(buf)
        }

        fn flush(&mut self) -> Result<()> {
            make_random_error()?;
            self.inner.flush()
        }
    }

    impl Seek for File {
        fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
            make_random_error()?;
            self.inner.seek(pos)
        }
    }

    pub struct OpenOptions {
        inner: fs::OpenOptions,
    }

    impl OpenOptions {
        pub fn new() -> Self {
            OpenOptions {
                inner: fs::OpenOptions::new(),
            }
        }

        pub fn read(&mut self, read: bool) -> &mut OpenOptions {
            self.inner.read(read);
            self
        }

        pub fn write(&mut self, write: bool) -> &mut OpenOptions {
            self.inner.write(write);
            self
        }

        pub fn truncate(&mut self, truncate: bool) -> &mut OpenOptions {
            self.inner.truncate(truncate);
            self
        }

        pub fn create(&mut self, create: bool) -> &mut OpenOptions {
            self.inner.create(create);
            self
        }

        pub fn create_new(&mut self, create_new: bool) -> &mut OpenOptions {
            self.inner.create_new(create_new);
            self
        }

        pub fn open<P: AsRef<Path>>(&self, path: P) -> Result<File> {
            make_random_error()?;
            let file = self.inner.open(path)?;
            Ok(File { inner: file })
        }
    }

    pub fn copy<P: AsRef<Path>, Q: AsRef<Path>>(from: P, to: Q) -> Result<u64> {
        make_random_error()?;
        fs::copy(from, to)
    }

    pub fn create_dir<P: AsRef<Path>>(path: P) -> Result<()> {
        make_random_error()?;
        fs::create_dir(path)
    }

    pub fn create_dir_all<P: AsRef<Path>>(path: P) -> Result<()> {
        make_random_error()?;
        fs::create_dir_all(path)
    }

    pub fn remove_dir_all<P: AsRef<Path>>(path: P) -> Result<()> {
        make_random_error()?;
        fs::remove_dir_all(path)
    }

    pub fn remove_file<P: AsRef<Path>>(path: P) -> Result<()> {
        make_random_error()?;
        fs::remove_file(path)
    }

    pub fn rename<P: AsRef<Path>, Q: AsRef<Path>>(
        from: P,
        to: Q,
    ) -> Result<()> {
        make_random_error()?;
        fs::rename(from, to)
    }

    pub fn read_dir<P: AsRef<Path>>(path: P) -> Result<fs::ReadDir> {
        make_random_error()?;
        fs::read_dir(path)
    }
}
