/// Calculate usize align offset, size must be 2^n integer
#[inline]
pub fn align_offset(x: usize, size: usize) -> usize {
    x & (size - 1)
}

/// Calculate u64 align offset, size must be 2^n integer
#[inline]
pub fn align_offset_u64(x: u64, size: u64) -> u64 {
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

/// Align usize integer to ceil, size must be 2^n integer
#[inline]
pub fn align_ceil(x: usize, size: usize) -> usize {
    if x == 0 {
        return size;
    }
    x + (-(x as isize) & (size as isize - 1)) as usize
}

/// Align u64 integer to ceil, size must be 2^n integer
#[allow(dead_code)]
#[inline]
pub fn align_ceil_u64(x: u64, size: u64) -> u64 {
    if x == 0 {
        return size;
    }
    x + (-(x as i64) & (size as i64 - 1)) as u64
}
