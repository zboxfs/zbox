use std::error::Error as StdError;
use std::io::{self, Read, Write, Error as IoError, ErrorKind, Seek, SeekFrom};
use std::fmt::{self, Debug};

use trans::{TxMgr, TxHandle};
use fs::Handle;
use fs::fnode::{Fnode, Version, Metadata, Reader as FnodeReader,
                Writer as FnodeWriter};
use super::{Result, Error};

/// A reader for a specific vesion of file content.
///
/// This reader is returned by the [`version_reader`] function, and implements
/// [`Read`] trait.
///
/// [`version_reader`]: struct.File.html#method.version_reader
/// [`Read`]: https://doc.rust-lang.org/std/io/trait.Read.html
#[derive(Debug)]
pub struct VersionReader<'a> {
    handle: &'a Handle,
    rdr: FnodeReader,
}

impl<'a> VersionReader<'a> {
    fn new(handle: &'a Handle, ver: usize) -> Result<Self> {
        let rdr = FnodeReader::new(handle.fnode.clone(), ver)?;
        Ok(VersionReader { handle, rdr })
    }
}

impl<'a> Read for VersionReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.rdr.read(buf)
    }
}

impl<'a> Seek for VersionReader<'a> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.rdr.seek(pos)
    }
}

/// A reference to an open file in the repository.
///
/// An instance of a `File` can be read and/or written depending on what options
/// it was opened with. Files also implement [`Seek`] to alter the logical cursor
/// that the file contains internally.
///
/// Files are automatically closed when they go out of scope.
///
/// As Zbox internally cached file content, it is no need to use buffered
/// reader, such as [`BufReader<R>`].
///
/// # Examples
///
/// Create a new file and write bytes to it:
///
/// ```
/// use std::io::prelude::*;
/// # use zbox::{init_env, Result, RepoOpener};
///
/// # fn foo() -> Result<()> {
/// # init_env();
/// # let mut repo = RepoOpener::new().create(true).open("mem://foo", "pwd")?;
/// let mut file = repo.create_file("/foo.txt")?;
/// file.write_all(b"Hello, world!")?;
/// file.finish()?;
/// # Ok(())
/// # }
/// # foo().unwrap();
/// ```
///
/// Read the content of a file into a [`String`]:
///
/// ```
/// # use zbox::{init_env, Result, RepoOpener};
/// use std::io::prelude::*;
/// # use zbox::OpenOptions;
///
/// # fn foo() -> Result<()> {
/// # init_env();
/// # let mut repo = RepoOpener::new().create(true).open("mem://foo", "pwd")?;
/// # {
/// #     let mut file = OpenOptions::new()
/// #         .create(true)
/// #         .open(&mut repo, "/foo.txt")?;
/// #     file.write_all(b"Hello, world!")?;
/// #     file.finish()?;
/// # }
/// let mut file = repo.open_file("/foo.txt")?;
/// let mut content = String::new();
/// file.read_to_string(&mut content)?;
/// assert_eq!(content, "Hello, world!");
/// # Ok(())
/// # }
/// # foo().unwrap();
/// ```
///
/// # Versioning
///
/// `File` contents support up to 255 revision versions. [`Version`] is
/// immutable once it is created.
///
/// By default, the maximum number of versions of a `File` is `10`, which is
/// configurable by [`version_limit`]. After reaching this limit, the oldest
/// [`Version`] will be automatically deleted after adding a new one.
///
/// Version number starts from `1` and continuously increases by 1.
///
/// # Writing
///
/// The file content is cached internally for deduplication and will be handled
/// automatically, thus calling [`flush`] is not recommendated.
///
/// `File` is multi-versioned, each time updating the content will create a new
/// permanent [`Version`]. There are two ways of writing data:
///
/// - **Multi-part Write**
///
///   This is done by updating `File` using [`Write`] trait. After all writing
///   operations, [`finish`] must be called to create a new version.
///
///   ## Examples
///   ```
///   # use zbox::{init_env, Result, RepoOpener};
///   use std::io::prelude::*;
///   # use zbox::OpenOptions;
///
///   # fn foo() -> Result<()> {
///   # init_env();
///   # let mut repo = RepoOpener::new().create(true).open("mem://foo", "pwd")?;
///   let mut file = OpenOptions::new().create(true).open(&mut repo, "/foo.txt")?;
///   file.write_all(b"foo ")?;
///   file.write_all(b"bar")?;
///   file.finish()?;
///
///   let mut content = String::new();
///   file.read_to_string(&mut content)?;
///   assert_eq!(content, "foo bar");
///
///   # Ok(())
///   # }
///   # foo().unwrap();
///   ```
///
/// - **Single-part Write**
///
///   This can be done by calling [`write_once`], which will call [`finish`]
///   internally to create a new version.
///
///   ## Examples
///   ```
///   #![allow(unused_mut, unused_variables)]
///   # use zbox::{init_env, Result, RepoOpener};
///   use std::io::Read;
///   # use zbox::OpenOptions;
///
///   # fn foo() -> Result<()> {
///   # init_env();
///   # let mut repo = RepoOpener::new().create(true).open("mem://foo", "pwd")?;
///   let mut file = OpenOptions::new().create(true).open(&mut repo, "/foo.txt")?;
///   file.write_once(b"foo bar")?;
///
///   let mut content = String::new();
///   file.read_to_string(&mut content)?;
///   assert_eq!(content, "foo bar");
///
///   # Ok(())
///   # }
///   # foo().unwrap();
///   ```
///
/// # Reading
///
/// As `File` contains multiple versions, [`Read`] operation must be
/// associated with a version. By default, the latest version is binded for
/// reading. To read a specific version, a [`VersionReader`], which supports
/// [`Read`] trait, can be used.
///
/// `File` internally maintain a reader, which will be opened for current
/// version when it is used at first time. Once the reader is opened,
/// subsequent write operations have no effect on it. So be carefull when
/// doing both reading and writing at the same time.
///
/// ## Examples
///
/// Read multiple versions using [`VersionReader`].
///
/// ```
/// use std::io::prelude::*;
/// # use zbox::{init_env, Result, RepoOpener};
/// # use zbox::OpenOptions;
///
/// # fn foo() -> Result<()> {
/// # init_env();
/// # let mut repo = RepoOpener::new().create(true).open("mem://foo", "pwd")?;
/// let mut file = OpenOptions::new().create(true).open(&mut repo, "/foo.txt")?;
/// file.write_once(b"foo")?;
/// file.write_once(b"bar")?;
///
/// // get latest version number
/// let curr_ver = file.curr_version();
///
/// let mut rdr = file.version_reader(curr_ver)?;
/// let mut content = String::new();
/// rdr.read_to_string(&mut content)?;
/// assert_eq!(content, "bar");
///
/// let mut rdr = file.version_reader(curr_ver - 1)?;
/// let mut content = String::new();
/// rdr.read_to_string(&mut content)?;
/// assert_eq!(content, "foo");
///
/// # Ok(())
/// # }
/// # foo().unwrap();
/// ```
///
/// Read the file content while it is in writing, notice that the read is not
/// affected by the following write.
///
/// ```
/// use std::io::prelude::*;
/// # use zbox::{init_env, Result, RepoOpener};
/// # use zbox::OpenOptions;
///
/// # fn foo() -> Result<()> {
/// # init_env();
/// # let mut repo = RepoOpener::new().create(true).open("mem://foo", "pwd")?;
/// let mut file = OpenOptions::new().create(true).open(&mut repo, "/foo.txt")?;
/// file.write_once(&[1, 2, 3, 4])?;
///
/// let mut buf = [0; 2];
/// file.read_exact(&mut buf)?;
/// assert_eq!(&buf[..], &[1, 2]);
///
/// // create a new version
/// file.write_once(&[5, 6, 7])?;
///
/// // notice this read still continues on previous version
/// file.read_exact(&mut buf)?;
/// assert_eq!(&buf[..], &[3, 4]);
///
/// # Ok(())
/// # }
/// # foo().unwrap();
/// ```
///
/// [`Seek`]: https://doc.rust-lang.org/std/io/trait.Seek.html
/// [`BufReader<R>`]: https://doc.rust-lang.org/std/io/struct.BufReader.html
/// [`flush`]: https://doc.rust-lang.org/std/io/trait.Write.html#tymethod.flush
/// [`String`]: https://doc.rust-lang.org/std/string/struct.String.html
/// [`Read`]: https://doc.rust-lang.org/std/io/trait.Read.html
/// [`Write`]: https://doc.rust-lang.org/std/io/trait.Write.html
/// [`Version`]: struct.Version.html
/// [`VersionReader`]: struct.VersionReader.html
/// [`version_limit`]: struct.OpenOptions.html#method.version_limit
/// [`finish`]: struct.File.html#method.finish
/// [`write_once`]: struct.File.html#method.write_once
pub struct File {
    handle: Handle,
    pos: SeekFrom, // always SeekFrom::Start
    rdr: Option<FnodeReader>,
    wtr: Option<FnodeWriter>,
    tx_handle: Option<TxHandle>,
    can_read: bool,
    can_write: bool,
}

impl File {
    pub(super) fn new(
        handle: Handle,
        pos: SeekFrom,
        can_read: bool,
        can_write: bool,
    ) -> Self {
        File {
            handle,
            pos,
            rdr: None,
            wtr: None,
            tx_handle: None,
            can_read,
            can_write,
        }
    }

    /// Queries metadata about the underlying file.
    pub fn metadata(&self) -> Metadata {
        let fnode = self.handle.fnode.read().unwrap();
        fnode.metadata()
    }

    /// Returns a list of all the file content versions.
    pub fn history(&self) -> Vec<Version> {
        let fnode = self.handle.fnode.read().unwrap();
        fnode.history()
    }

    /// Returns the current content version number.
    pub fn curr_version(&self) -> usize {
        let fnode = self.handle.fnode.read().unwrap();
        fnode.curr_ver_num()
    }

    /// Returns content byte size of the current version.
    fn curr_len(&self) -> usize {
        let fnode = self.handle.fnode.read().unwrap();
        fnode.curr_len()
    }

    /// Return reader of specified version.
    ///
    /// The returned reader implements [`Read`] trait. To get the version
    /// number, firstly call [`history`] to get the list of all versions and
    /// then choose the version number from it.
    ///
    /// [`Read`]: https://doc.rust-lang.org/std/io/trait.Read.html
    /// [`history`]: struct.File.html#method.history
    pub fn version_reader(&self, ver_num: usize) -> Result<VersionReader> {
        if !self.can_read {
            return Err(Error::CannotRead);
        }
        VersionReader::new(&self.handle, ver_num)
    }

    // calculate seek position based on file current size
    fn seek_pos(&self, pos: SeekFrom) -> SeekFrom {
        let curr_len = self.curr_len();
        let pos: i64 = match pos {
            SeekFrom::Start(p) => p as i64,
            SeekFrom::End(p) => curr_len as i64 + p,
            SeekFrom::Current(p) => {
                match self.pos {
                    SeekFrom::Start(q) => p + q as i64,
                    SeekFrom::End(q) => curr_len as i64 + p + q,
                    SeekFrom::Current(_) => unreachable!(),
                }
            }
        };
        SeekFrom::Start(pos as u64)
    }

    fn begin_write(&mut self) -> Result<()> {
        if self.wtr.is_some() {
            return Err(Error::NotFinish);
        }

        if !self.can_write {
            return Err(Error::CannotWrite);
        }

        assert!(self.tx_handle.is_none());

        // append zeros if current position is beyond EOF
        let curr_len = self.curr_len();
        match self.pos {
            SeekFrom::Start(pos) => {
                let pos = pos as usize;
                if pos > curr_len {
                    // append zeros by setting file length
                    self.set_len(pos)?;

                    // then seek to new EOF
                    self.pos = self.seek_pos(SeekFrom::End(0));
                }
            }
            _ => unreachable!(),
        }

        // begin write
        let tx_handle = TxMgr::begin_trans(&self.handle.txmgr)?;
        tx_handle.run(|| {
            let mut wtr =
                FnodeWriter::new(self.handle.clone(), tx_handle.txid)?;
            wtr.seek(self.seek_pos(self.pos))?;
            self.wtr = Some(wtr);
            Ok(())
        })?;
        self.tx_handle = Some(tx_handle);

        Ok(())
    }

    /// Complete multi-part write to create a new version.
    ///
    /// # Errors
    ///
    /// Calling this function without writing data before will return
    /// [`Error::NotWrite`] error.
    ///
    /// [`Error::NotWrite`]: enum.Error.html
    pub fn finish(&mut self) -> Result<()> {
        match self.wtr.take() {
            Some(wtr) => {
                let tx_handle = self.tx_handle.take().unwrap();

                tx_handle.run(|| wtr.finish())?;
                tx_handle.commit()?;

                // reset position
                self.pos = SeekFrom::Start(0);

                Ok(())
            }
            None => Err(Error::NotWrite),
        }
    }

    /// Single-part write to create a new version.
    ///
    /// This function provides a convenient way of combining [`Write`] and
    /// [`finish`].
    ///
    /// [`Write`]: https://doc.rust-lang.org/std/io/trait.Write.html
    /// [`finish`]: struct.File.html#method.finish
    pub fn write_once(&mut self, buf: &[u8]) -> Result<()> {
        match self.wtr {
            Some(_) => Err(Error::NotFinish),
            None => {
                self.begin_write()?;
                match self.wtr {
                    Some(ref mut wtr) => {
                        match self.tx_handle {
                            Some(ref tx_handle) => {
                                tx_handle.run(|| {
                                    wtr.write_all(buf)?;
                                    Ok(())
                                })?;
                            }
                            None => unreachable!(),
                        }
                    }
                    None => unreachable!(),
                }
                self.finish()
            }
        }
    }

    /// Truncates or extends the underlying file, create a new version of
    /// content which size to become `size`.
    ///
    /// If the size is less than the current content size, then the new
    /// content will be shrunk. If it is greater than the current content size,
    /// then the content will be extended to `size` and have all of the
    /// intermediate data filled in with 0s.
    ///
    /// # Errors
    ///
    /// This function will return an error if the file is not opened for
    /// writing or not finished writing.
    pub fn set_len(&mut self, len: usize) -> Result<()> {
        if self.wtr.is_some() {
            return Err(Error::NotFinish);
        }

        if !self.can_write {
            return Err(Error::CannotWrite);
        }

        let tx_handle = TxMgr::begin_trans(&self.handle.txmgr)?;
        tx_handle.run_all(|| {
            Fnode::set_len(self.handle.clone(), len, tx_handle.txid)
        })?;
        Ok(())
    }
}

impl Read for File {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if !self.can_read {
            return Err(IoError::new(
                ErrorKind::Other,
                Error::CannotRead.description(),
            ));
        }
        if self.rdr.is_none() {
            let mut rdr = map_io_err!(
                FnodeReader::new_current(self.handle.fnode.clone())
            )?;
            rdr.seek(self.pos)?;
            self.rdr = Some(rdr);
        }
        match self.rdr {
            Some(ref mut rdr) => rdr.read(buf),
            None => unreachable!(),
        }
    }
}

impl Write for File {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.wtr.is_none() {
            map_io_err!(self.begin_write())?;
        }
        match self.wtr {
            Some(ref mut wtr) => {
                match self.tx_handle {
                    Some(ref tx_handle) => {
                        let mut ret = 0;
                        map_io_err!(tx_handle.run(|| {
                            ret = wtr.write(buf)?;
                            Ok(())
                        }))?;
                        Ok(ret)
                    }
                    None => unreachable!(),
                }
            }
            None => unreachable!(),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self.wtr {
            Some(ref mut wtr) => {
                match self.tx_handle {
                    Some(ref tx_handle) => {
                        map_io_err!(tx_handle.run(|| {
                            wtr.flush()?;
                            Ok(())
                        }))?;
                        Ok(())
                    }
                    None => unreachable!(),
                }
            }
            None => Err(IoError::new(
                ErrorKind::PermissionDenied,
                Error::CannotWrite.description(),
            )),
        }
    }
}

impl Seek for File {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        if self.wtr.is_some() {
            return Err(IoError::new(
                ErrorKind::Other,
                Error::NotFinish.description(),
            ));
        }

        let mut sought = 0;

        if let Some(ref mut rdr) = self.rdr {
            sought = rdr.seek(pos)?;
        }
        if sought == 0 {
            self.pos = self.seek_pos(pos);
        } else {
            self.pos = SeekFrom::Start(sought);
        }
        Ok(sought)
    }
}

impl Debug for File {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("File")
            .field("pos", &self.pos)
            .field("rdr", &self.rdr)
            .field("wtr", &self.wtr)
            .field("can_read", &self.can_read)
            .field("can_write", &self.can_write)
            .finish()
    }
}
