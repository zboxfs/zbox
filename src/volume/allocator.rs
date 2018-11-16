use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::thread::{self, ThreadId};

use super::address::Span;
use base::IntoRef;

/// Block allocator
#[derive(Debug, Default)]
pub struct Allocator {
    blk_wmark: usize,
    reserved: HashMap<ThreadId, Span>,
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

    // reserve some continuous blocks, return the new block watermark
    // one thread can reserve only once and must be in
    // whole-in-and-whole-out manner
    pub fn reserve(&mut self, blk_cnt: usize) -> usize {
        let thread_id = thread::current().id();
        let exists = self
            .reserved
            .insert(thread_id, Span::new(self.blk_wmark, blk_cnt));
        assert_eq!(exists, None);
        self.blk_wmark += blk_cnt;
        self.blk_wmark
    }

    // allocate continuous blocks, return the start block index
    pub fn allocate(&mut self, blk_cnt: usize) -> Span {
        let thread_id = thread::current().id();

        // if the thread has reservation, take it
        if let Some(span) = self.reserved.remove(&thread_id) {
            assert_eq!(blk_cnt, span.cnt);
            return span;
        }

        let begin = self.blk_wmark;
        self.blk_wmark += blk_cnt;
        Span::new(begin, blk_cnt)
    }
}

impl IntoRef for Allocator {}

/// Block allocator reference type
pub type AllocatorRef = Arc<RwLock<Allocator>>;
