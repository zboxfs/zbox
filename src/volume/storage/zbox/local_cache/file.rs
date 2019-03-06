use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use super::CacheBackend;
use base::utils;
use base::vio;
use error::Result;

pub struct FileBackend {
    base: PathBuf,
}

impl FileBackend {
    pub fn new(base: &Path) -> Self {
        FileBackend {
            base: base.to_path_buf(),
        }
    }
}

impl CacheBackend for FileBackend {
    #[inline]
    fn contains(&mut self, rel_path: &Path) -> bool {
        let path = self.base.join(rel_path);
        path.exists()
    }

    fn get_exact(
        &mut self,
        rel_path: &Path,
        offset: usize,
        dst: &mut [u8],
    ) -> Result<()> {
        let path = self.base.join(rel_path);
        let mut file = vio::OpenOptions::new().read(true).open(&path)?;
        file.seek(SeekFrom::Start(offset as u64))?;
        file.read_exact(dst)?;
        Ok(())
    }

    fn get(&mut self, rel_path: &Path) -> Result<Vec<u8>> {
        let path = self.base.join(rel_path);
        let mut ret = Vec::new();
        let mut file = vio::OpenOptions::new().read(true).open(&path)?;
        file.read_to_end(&mut ret)?;
        Ok(ret)
    }

    fn insert(&mut self, rel_path: &Path, obj: &[u8]) -> Result<()> {
        let path = self.base.join(rel_path);
        utils::ensure_parents_dir(&path)?;
        let mut file = vio::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        file.write_all(obj)?;
        Ok(())
    }

    fn remove(&mut self, rel_path: &Path) -> Result<()> {
        let path = self.base.join(rel_path);
        if path.exists() {
            vio::remove_file(&path)?;
            // ignore error when removing empty parent dir
            let _ = utils::remove_empty_parent_dir(&path);
        }
        Ok(())
    }

    #[inline]
    fn clear(&mut self) -> Result<()> {
        if self.base.is_dir() {
            for entry in vio::read_dir(&self.base)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    vio::remove_dir_all(path)?;
                } else {
                    vio::remove_file(path)?;
                }
            }
        }
        Ok(())
    }
}
