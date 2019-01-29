use std::time::Duration;

#[cfg(any(feature = "storage-file", feature = "storage-zbox"))]
use error::Result;

/// Calculate usize align offset, size must be 2^n integer
#[inline]
pub fn align_offset(x: usize, size: usize) -> usize {
    x & (size - 1)
}

/// Align usize to floor, size must be 2^n integer
#[allow(dead_code)]
#[inline]
pub fn align_floor(x: usize, size: usize) -> usize {
    x - (x & (size - 1))
}

/// Align u64 to floor, size must be 2^n integer
#[allow(dead_code)]
#[inline]
pub fn align_floor_u64(x: u64, size: u64) -> u64 {
    x - (x & (size - 1))
}

/// Align usize to floor and convert to chunk index, size must be 2^n integer
#[allow(dead_code)]
#[inline]
pub fn align_floor_chunk(x: usize, size: usize) -> usize {
    align_floor(x, size) / size
}

/// Align usize integer to ceil, size must be 2^n integer
#[inline]
pub fn align_ceil(x: usize, size: usize) -> usize {
    if x == 0 {
        return size;
    }
    x + (-(x as isize) & (size as isize - 1)) as usize
}

/// Align u64 integer to ceil, size must be 2^n integer
/// Note: when x is on size boundary, it will align to next ceil
#[allow(dead_code)]
#[inline]
pub fn align_ceil_u64(x: u64, size: u64) -> u64 {
    if x == 0 {
        return size;
    }
    x + (-(x as i64) & (size as i64 - 1)) as u64
}

/// Align usize to ceil and convert to chunk index, size must be 2^n integer
#[inline]
pub fn align_ceil_chunk(x: usize, size: usize) -> usize {
    align_ceil(x, size) / size
}

/// Output human friendly speed string
#[allow(dead_code)]
pub fn speed_str(duration: &Duration, data_len: usize) -> String {
    let secs = duration.as_secs() as f32
        + duration.subsec_nanos() as f32 / 1_000_000_000.0;
    format!("{} MB/s", data_len as f32 / (1024.0 * 1024.0) / secs)
}

/// Ensure all parents dir are created along the path
#[cfg(any(feature = "storage-file", feature = "storage-zbox"))]
pub fn ensure_parents_dir(path: &std::path::Path) -> Result<()> {
    let parent = path.parent().unwrap();
    if !parent.exists() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}

/// Remove parent dir if it is empty
#[cfg(any(feature = "storage-file", feature = "storage-zbox"))]
pub fn remove_empty_parent_dir(path: &std::path::Path) -> Result<()> {
    for parent in path.ancestors().skip(1) {
        if std::fs::read_dir(parent)?.count() > 0 {
            break;
        }
        std::fs::remove_dir(&parent)?;
    }
    Ok(())
}
