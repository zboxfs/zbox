use std::cmp::min;
use std::error::Error as StdError;
use std::fmt::Debug;
use std::io::{Error as IoError, ErrorKind, Read, Result as IoResult, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use super::vio;
use base::crypto::{Crypto, Key};
use base::utils::{ensure_parents_dir, remove_empty_parent_dir};
use error::{Error, Result};
use trans::Eid;
use trans::Finish;
use volume::{ArmAccess, Armor};

// read/write frame size
const FRAME_SIZE: usize = 16 * 1024;

// File crypto reader
pub struct CryptoReader {
    file: vio::File,

    // encrypted frame, read from file
    enc_frame: Vec<u8>,
    enc_frame_len: usize,

    // decrypted frame
    frame: Vec<u8>,
    frame_len: usize,
    read: usize,

    crypto: Crypto,
    key: Key,
}

impl CryptoReader {
    fn new(file: vio::File, crypto: &Crypto, key: &Key) -> Self {
        CryptoReader {
            file,
            enc_frame: vec![0u8; FRAME_SIZE],
            enc_frame_len: 0,
            frame: vec![0u8; FRAME_SIZE],
            frame_len: 0,
            read: 0,
            crypto: crypto.clone(),
            key: key.clone(),
        }
    }
}

impl Read for CryptoReader {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        if self.read >= self.frame_len {
            // frame has been exhausted, read in a new frame
            loop {
                match self.file.read(&mut self.enc_frame[self.enc_frame_len..])
                {
                    Ok(0) => break,
                    Ok(len) => self.enc_frame_len += len,
                    Err(ref err) if err.kind() == ErrorKind::Interrupted => {}
                    Err(err) => return Err(err),
                }
            }

            if self.enc_frame_len == 0 {
                return Ok(0);
            }

            // decrypt frame
            self.frame_len = map_io_err!(self.crypto.decrypt_to(
                &mut self.frame,
                &self.enc_frame[..self.enc_frame_len],
                &self.key
            ))?;
            self.enc_frame_len = 0;
        }

        // copy decrypted to destination
        assert!(self.frame_len > self.read);
        let copy_len = min(buf.len(), self.frame_len - self.read);
        buf[..copy_len]
            .copy_from_slice(&self.frame[self.read..self.read + copy_len]);
        self.read += copy_len;

        Ok(copy_len)
    }
}

// File crypto writer
pub struct CryptoWriter {
    file: vio::File,

    // stage frame, read from input
    stg: Vec<u8>,
    stg_len: usize,

    // encrypted frame
    frame: Vec<u8>,
    frame_len: usize,

    // total bytes written to file
    written: usize,

    crypto: Crypto,
    key: Key,
}

impl CryptoWriter {
    fn new(file: vio::File, crypto: &Crypto, key: &Key) -> Self {
        CryptoWriter {
            file,
            stg: vec![0u8; crypto.decrypted_len(FRAME_SIZE)],
            stg_len: 0,
            frame: vec![0u8; FRAME_SIZE],
            frame_len: 0,
            written: 0,
            crypto: crypto.clone(),
            key: key.clone(),
        }
    }

    // encrypt to frame and write it to file
    fn write_frame(&mut self) -> Result<()> {
        if self.stg_len == 0 {
            return Ok(());
        }

        // encrypt stage to frame
        self.frame_len = self.crypto.encrypt_to(
            &mut self.frame,
            &self.stg[..self.stg_len],
            &self.key,
        )?;

        // write encrypted frame to file
        self.file.write_all(&self.frame[..self.frame_len])?;
        self.written += self.frame_len;
        self.stg_len = 0;
        Ok(())
    }
}

impl Write for CryptoWriter {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        let copy_len = min(buf.len(), self.stg.len() - self.stg_len);
        self.stg[self.stg_len..self.stg_len + copy_len]
            .copy_from_slice(&buf[..copy_len]);
        self.stg_len += copy_len;

        if self.stg_len >= self.stg.len() {
            // stage is full, write frame
            map_io_err!(self.write_frame())?;
        }

        Ok(copy_len)
    }

    fn flush(&mut self) -> IoResult<()> {
        // do nothing, use finish() to finalise writing
        Ok(())
    }
}

impl Finish for CryptoWriter {
    fn finish(mut self) -> Result<usize> {
        // flush frame
        self.write_frame()?;
        Ok(self.written)
    }
}

// file armor
#[derive(Debug)]
pub struct FileArmor<T> {
    base: PathBuf,
    crypto: Crypto,
    key: Key,
    _t: PhantomData<T>,
}

impl<T> FileArmor<T> {
    pub fn new(base: &Path) -> Self {
        FileArmor {
            base: base.to_path_buf(),
            crypto: Crypto::default(),
            key: Key::new_empty(),
            _t: PhantomData,
        }
    }

    #[inline]
    pub fn set_crypto_ctx(&mut self, crypto: Crypto, key: Key) {
        self.crypto = crypto;
        self.key = key;
    }
}

impl<'de, T: ArmAccess<'de> + Debug> Armor<'de> for FileArmor<T> {
    type Item = T;
    type ItemReader = CryptoReader;
    type ItemWriter = CryptoWriter;

    fn get_item_reader(&self, arm_id: &Eid) -> Result<Self::ItemReader> {
        let path = arm_id.to_path_buf(&self.base);
        match vio::OpenOptions::new().read(true).open(&path) {
            Ok(file) => Ok(CryptoReader::new(file, &self.crypto, &self.key)),
            Err(ref err) if err.kind() == ErrorKind::NotFound => {
                Err(Error::NotFound)
            }
            Err(err) => Err(Error::from(err)),
        }
    }

    fn get_item_writer(&self, arm_id: &Eid) -> Result<Self::ItemWriter> {
        let path = arm_id.to_path_buf(&self.base);
        ensure_parents_dir(&path)?;
        let file = vio::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        Ok(CryptoWriter::new(file, &self.crypto, &self.key))
    }

    fn del_arm(&self, arm_id: &Eid) -> Result<()> {
        let path = arm_id.to_path_buf(&self.base);
        vio::remove_file(&path)?;
        remove_empty_parent_dir(&path)?;
        Ok(())
    }
}
