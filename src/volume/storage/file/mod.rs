mod file;
mod file_armor;
mod index;
mod sector;
mod vio;

pub use self::file::FileStorage;

use std::path::Path;

use self::vio::imp as vio_imp;
use error::Result;

// ensure all parents dir are created along the path
fn ensure_parents_dir(path: &Path) -> Result<()> {
    let parent = path.parent().unwrap();
    if !parent.exists() {
        vio_imp::create_dir_all(parent)?;
    }
    Ok(())
}

// remove parent dir if it is empty
fn remove_empty_parent_dir(path: &Path) -> Result<()> {
    let parent_dir = path.parent().unwrap();
    if vio_imp::read_dir(&parent_dir)?.count() == 0 {
        vio_imp::remove_dir(&parent_dir)?;
    }
    Ok(())
}
