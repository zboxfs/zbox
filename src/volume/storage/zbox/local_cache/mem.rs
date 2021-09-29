use std::collections::HashMap;
use std::path::{Path, PathBuf};

use super::CacheBackend;
use crate::error::Result;

pub struct MemBackend {
    map: HashMap<PathBuf, Vec<u8>>,
}

impl MemBackend {
    #[inline]
    pub fn new() -> Self {
        MemBackend {
            map: HashMap::new(),
        }
    }
}

impl CacheBackend for MemBackend {
    #[inline]
    fn contains(&mut self, rel_path: &Path) -> bool {
        self.map.contains_key(rel_path)
    }

    fn get_exact(
        &mut self,
        rel_path: &Path,
        offset: usize,
        dst: &mut [u8],
    ) -> Result<()> {
        let obj = &self.map[rel_path];
        let len = dst.len();
        dst.copy_from_slice(&obj[offset..offset + len]);
        Ok(())
    }

    #[inline]
    fn get(&mut self, rel_path: &Path) -> Result<Vec<u8>> {
        Ok(self.map[rel_path].to_owned())
    }

    #[inline]
    fn insert(&mut self, rel_path: &Path, obj: &[u8]) -> Result<()> {
        self.map.insert(rel_path.to_path_buf(), obj.to_owned());
        Ok(())
    }

    #[inline]
    fn remove(&mut self, rel_path: &Path) -> Result<()> {
        self.map.remove(rel_path);
        Ok(())
    }

    #[inline]
    fn clear(&mut self) -> Result<()> {
        self.map.clear();
        Ok(())
    }
}
