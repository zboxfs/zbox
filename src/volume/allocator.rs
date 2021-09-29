use std::sync::{Arc, RwLock};

use super::address::Span;
use crate::base::IntoRef;

/// Block allocator
#[derive(Debug, Default)]
pub struct Allocator {
    blk_wmark: usize,
}

impl Allocator {
    #[inline]
    pub fn new() -> Allocator {
        Allocator::default()
    }

    #[inline]
    pub fn block_wmark(&self) -> usize {
        self.blk_wmark
    }

    #[inline]
    pub fn set_block_wmark(&mut self, blk_wmark: usize) {
        self.blk_wmark = blk_wmark;
    }

    // allocate continuous blocks
    #[inline]
    pub fn allocate(&mut self, blk_cnt: usize) -> Span {
        let begin = self.blk_wmark;
        self.blk_wmark += blk_cnt;
        Span::new(begin, blk_cnt)
    }
}

impl IntoRef for Allocator {}

/// Block allocator reference type
pub type AllocatorRef = Arc<RwLock<Allocator>>;
