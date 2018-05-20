mod file;
mod mem;
//#[cfg(feature = "zbox-cloud")]
//mod zbox;

use std::fmt::Debug;
use std::sync::{Arc, RwLock};

use error::Result;
use trans::Eid;

pub use self::file::FileStorage;
pub use self::mem::MemStorage;
//#[cfg(feature = "zbox-cloud")]
//pub use self::zbox::ZboxStorage;

/// Storage trait
pub trait Storage: Debug + Send + Sync {
    fn get(&mut self, dst: &mut [u8], id: &Eid, offset: u64) -> Result<usize>;
    fn put(&mut self, id: &Eid, buf: &[u8], offset: u64) -> Result<usize>;
    fn del(&mut self, id: &Eid) -> Result<()>;

    fn get_all(&mut self, dst: &mut Vec<u8>, id: &Eid) -> Result<usize> {
        let mut offset = 0;
        dst.clear();
        loop {
            if offset >= dst.len() {
                let new_len = dst.len() + 4096;
                dst.resize(new_len, 0);
            }
            let got = self.get(&mut dst[offset..], id, offset as u64)?;
            if got == 0 {
                break;
            }
            offset += got
        }
        dst.truncate(offset);
        Ok(offset)
    }

    fn put_all(&mut self, id: &Eid, buf: &[u8]) -> Result<()> {
        let mut offset = 0;
        while offset < buf.len() {
            let written = self.put(id, &buf[offset..], offset as u64)?;
            offset += written;
        }
        Ok(())
    }
}

/// Storage reference type
pub type StorageRef = Arc<RwLock<Storage>>;
