use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::thread::{self, ThreadId};

use super::address::Span;
use base::IntoRef;

/// Block allocator
#[derive(Debug, Default)]
pub struct Allocator {
    blk_wmark: u64,
    reserved: HashMap<ThreadId, Span>,
}

impl Allocator {
    #[inline]
    pub fn new() -> Allocator {
        Allocator::default()
    }

    #[inline]
    pub fn block_wmark(&self) -> u64 {
        self.blk_wmark
    }

    #[inline]
    pub fn set_block_wmark(&mut self, blk_wmark: u64) {
        self.blk_wmark = blk_wmark;
    }

    // reserve some continuous blocks, return the new block watermark
    // one thread can reserve only once and must be in
    // whole-in-and-whole-out manner
    pub fn reserve(&mut self, blk_cnt: usize) -> u64 {
        let thread_id = thread::current().id();
        let exists = self.reserved.insert(
            thread_id,
            Span::new(self.blk_wmark, self.blk_wmark + blk_cnt as u64, 0),
        );
        assert_eq!(exists, None);
        self.blk_wmark += blk_cnt as u64;
        self.blk_wmark
    }

    // allocate continuous blocks, return the start block index
    pub fn allocate(&mut self, blk_cnt: usize) -> u64 {
        let thread_id = thread::current().id();

        // if the thread has reservation, take it
        if let Some(span) = self.reserved.remove(&thread_id) {
            assert_eq!(blk_cnt, span.block_count());
            return span.begin;
        }

        let begin_idx = self.blk_wmark;
        self.blk_wmark += blk_cnt as u64;
        begin_idx
    }
}

impl IntoRef for Allocator {}

/// Block allocator reference type
pub type AllocatorRef = Arc<RwLock<Allocator>>;
