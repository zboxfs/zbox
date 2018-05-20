use std::path::Path;

use error::Result;
use base::IntoRef;
use trans::Eid;
use volume::storage::Storage;

/// File Storage
#[derive(Debug)]
pub struct FileStorage {}

impl FileStorage {
    pub fn new(_path: &Path) -> Self {
        FileStorage {}
    }
}

impl Storage for FileStorage {
    fn get(
        &mut self,
        _buf: &mut [u8],
        _id: &Eid,
        _offset: u64,
    ) -> Result<usize> {
        // TODO
        Ok(0)
    }

    fn put(&mut self, _id: &Eid, _buf: &[u8], _offset: u64) -> Result<usize> {
        // TODO
        Ok(_buf.len())
    }

    fn del(&mut self, _id: &Eid) -> Result<()> {
        // TODO
        Ok(())
    }
}

impl IntoRef for FileStorage {}
