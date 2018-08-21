//! volume module document
//!

mod address;
mod allocator;
mod storage;
mod super_block;
mod volume;

pub use self::allocator::{Allocator, AllocatorRef};
pub use self::storage::StorageRef;
pub use self::volume::{Info, Reader, Volume, VolumeRef, Writer};

// block and frame size
pub const BLK_SIZE: usize = 8 * 1024;
pub const BLKS_PER_FRAME: usize = 16;
pub const FRAME_SIZE: usize = BLKS_PER_FRAME * BLK_SIZE;
