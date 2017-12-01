use std::error::Error as StdError;
use std::io::{Read, Write, Result as IoResult, Error as IoError, ErrorKind,
              Seek, SeekFrom};
use std::fmt::{self, Debug};

use trans::{TxMgr, TxHandle};
use fs::Handle;
use fs::fnode::{Fnode, Version, Metadata, Reader as FnodeReader,
                Writer as FnodeWriter};
use super::{Result, Error};

/// Version reader
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
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        self.rdr.read(buf)
    }
}

impl<'a> Seek for VersionReader<'a> {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        self.rdr.seek(pos)
    }
}

/// A reference to an open file in the repository.
///
/// An instance of a `File` can be read and/or written depending on what options
/// it was opened with. Files also implement [`Seek`] to alter the logical cursor
/// that the file contains internally.
///
/// Files are automatically flushed and closed when they go out of scope. So
/// calling [`flush`] is not recommendated.
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
/// # use zbox::{zbox_init, Result, RepoOpener};
///
/// # fn foo() -> Result<()> {
/// # zbox_init();
/// # let mut repo = RepoOpener::new().create(true).open("mem://foo", "pwd")?;
/// let mut file = repo.create_file("/foo.txt")?;
/// file.write_all(b"Hello, world!")?;
/// # Ok(())
/// # }
/// # foo().unwrap();
/// ```
///
/// Read the current version of content of a file into a [`String`]:
///
/// ```
/// # use zbox::{zbox_init, Result, RepoOpener};
/// use std::io::prelude::*;
/// # use zbox::OpenOptions;
///
/// # fn foo() -> Result<()> {
/// # zbox_init();
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
/// [`Seek`]: https://doc.rust-lang.org/std/io/trait.Seek.html
/// [`BufReader<R>`]: https://doc.rust-lang.org/std/io/struct.BufReader.html
/// [`flush`]: https://doc.rust-lang.org/std/io/trait.Write.html#tymethod.flush
/// [`String`]: https://doc.rust-lang.org/std/string/struct.String.html
pub struct File {
    handle: Handle,
    ver: usize,
    pos: SeekFrom, // always SeekFrom::Start
    rdr: Option<FnodeReader>,
    wtr: Option<FnodeWriter>,
    tx_handle: Option<TxHandle>,
    read_only: bool,
}

impl File {
    pub fn new(handle: Handle, pos: SeekFrom, read_only: bool) -> Self {
        File {
            handle,
            ver: 0,
            pos,
            rdr: None,
            wtr: None,
            tx_handle: None,
            read_only,
        }
    }

    /// Get file metadata
    pub fn metadata(&self) -> Metadata {
        let fnode = self.handle.fnode.read().unwrap();
        fnode.metadata()
    }

    /// Get file history
    pub fn history(&self) -> Vec<Version> {
        let fnode = self.handle.fnode.read().unwrap();
        fnode.history()
    }

    /// Get current version number
    pub fn curr_version(&self) -> usize {
        let fnode = self.handle.fnode.read().unwrap();
        fnode.curr_ver_num()
    }

    /// Get current version content size
    fn curr_len(&self) -> usize {
        let fnode = self.handle.fnode.read().unwrap();
        fnode.curr_len()
    }

    /// Create a version reader
    pub fn version_reader(&self, ver: usize) -> Result<VersionReader> {
        VersionReader::new(&self.handle, ver)
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

        if self.read_only {
            return Err(Error::ReadOnly);
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

    /// Finish multi-part writing
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

    /// Single-part write
    pub fn write_once(mut self, buf: &[u8]) -> Result<()> {
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

    /// Set file length by appending to end or truncating
    pub fn set_len(&mut self, len: usize) -> Result<()> {
        if self.wtr.is_some() {
            return Err(Error::NotFinish);
        }

        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let tx_handle = TxMgr::begin_trans(&self.handle.txmgr)?;
        tx_handle.run_all(|| {
            Fnode::set_len(self.handle.clone(), len, tx_handle.txid)
        })?;
        Ok(())
    }
}

impl Read for File {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
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
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
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

    fn flush(&mut self) -> IoResult<()> {
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
                Error::ReadOnly.description(),
            )),
        }
    }
}

impl Seek for File {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
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
            .field("ver", &self.ver)
            .field("pos", &self.pos)
            .field("rdr", &self.rdr)
            .field("wtr", &self.wtr)
            .field("read_only", &self.read_only)
            .finish()
    }
}
