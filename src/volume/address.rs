use std::ops::Index;
use std::slice::Iter;

use super::{BLKS_PER_FRAME, BLK_SIZE, FRAME_SIZE};

/// Span
#[derive(Debug, Clone, Copy, Default, PartialEq, Deserialize, Serialize)]
pub struct Span {
    pub begin: u64,  // begin block index
    pub end: u64,    // end block index, exclusive
    pub offset: u64, // offset in span list
}

impl Span {
    #[inline]
    pub fn new(begin: u64, end: u64, offset: u64) -> Self {
        Span { begin, end, offset }
    }

    #[inline]
    pub fn block_count(&self) -> usize {
        (self.end - self.begin) as usize
    }

    #[inline]
    pub fn block_len(&self) -> usize {
        self.block_count() * BLK_SIZE
    }

    pub fn split_to(&mut self, at: u64) -> Span {
        assert!(self.begin <= at && at < self.end);
        let ret = Span {
            begin: self.begin,
            end: at,
            offset: self.offset,
        };
        self.begin = at;
        self.offset = self.offset + ret.block_len() as u64;
        ret
    }
}

/// Entity address
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Addr {
    pub len: usize,
    pub list: Vec<Span>,
}

impl Addr {
    #[inline]
    pub fn iter(&self) -> Iter<Span> {
        self.list.iter()
    }

    // append a span to address
    pub fn append(&mut self, begin_idx: u64, blk_cnt: usize, len: usize) {
        let end_idx = begin_idx + blk_cnt as u64;

        if self.list.is_empty() {
            self.list.push(Span::new(begin_idx, end_idx, 0));
            self.len = len;
            return;
        }

        if begin_idx == self.list.last_mut().unwrap().end {
            // merge to the last span
            let last = self.list.last_mut().unwrap();
            last.end += blk_cnt as u64;
        } else {
            self.list
                .push(Span::new(begin_idx, end_idx, self.len as u64));
        }
        self.len += len;
    }

    // split address to frames
    pub fn split_to_frames(&self) -> Vec<Addr> {
        let mut frames = vec![Addr::default()];
        let mut frm_idx: usize = 0;
        let mut blks_cnt: usize = 0;

        for span in self.list.iter() {
            let mut span = span.clone();
            span.offset = (frm_idx * FRAME_SIZE + blks_cnt * BLK_SIZE) as u64;

            loop {
                let blks_left = BLKS_PER_FRAME - blks_cnt;

                if span.block_count() <= blks_left {
                    // span can fit into frame
                    frames[frm_idx].list.push(span);
                    blks_cnt += span.block_count();
                    break;
                }

                // span cannot fit into frame, must split span first
                let at = span.begin + blks_left as u64;
                let split = span.split_to(at);

                // finish current frame and start a new frame
                frames[frm_idx].list.push(split);
                frames[frm_idx].len = FRAME_SIZE;
                frames.push(Addr::default());
                frm_idx += 1;
                blks_cnt = 0;
            }
        }

        // fix the last frame's length
        frames.last_mut().unwrap().len = self.len - frm_idx * FRAME_SIZE;
        assert_eq!(self.len, frames.iter().map(|a| a.len).sum::<usize>());

        frames
    }
}

impl IntoIterator for Addr {
    type Item = Span;
    type IntoIter = ::std::vec::IntoIter<Span>;

    fn into_iter(self) -> Self::IntoIter {
        self.list.into_iter()
    }
}

impl Index<usize> for Addr {
    type Output = Span;

    fn index(&self, index: usize) -> &Span {
        &self.list[index]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_addr() {
        // #1, a address is smaller than a frame
        let span = Span {
            begin: 0,
            end: 1,
            offset: 0,
        };
        let addr = Addr {
            len: 3,
            list: vec![span.clone()],
        };
        let frms = addr.split_to_frames();
        assert_eq!(frms.len(), 1);
        assert_eq!(frms[0].len, addr.len);
        assert_eq!(frms[0].list[0], span);

        // #2, a address is equal a frame
        let span = Span {
            begin: 0,
            end: BLKS_PER_FRAME as u64,
            offset: 0,
        };
        let addr = Addr {
            len: FRAME_SIZE,
            list: vec![span.clone()],
        };
        let frms = addr.split_to_frames();
        assert_eq!(frms.len(), 1);
        assert_eq!(frms[0].len, addr.len);
        assert_eq!(frms[0].list[0], span);

        // #3, a address is greater a frame
        let span = Span {
            begin: 0,
            end: BLKS_PER_FRAME as u64 + 1,
            offset: 0,
        };
        let addr = Addr {
            len: FRAME_SIZE + 3,
            list: vec![span.clone()],
        };
        let frms = addr.split_to_frames();
        assert_eq!(frms.len(), 2);
        assert_eq!(frms[0].len, FRAME_SIZE);
        assert_eq!(
            frms[0].list[0],
            Span {
                begin: 0,
                end: BLKS_PER_FRAME as u64,
                offset: 0
            }
        );
        assert_eq!(frms[1].len, 3);
        assert_eq!(
            frms[1].list[0],
            Span {
                begin: BLKS_PER_FRAME as u64,
                end: BLKS_PER_FRAME as u64 + 1,
                offset: FRAME_SIZE as u64,
            }
        );

        // #4, 2 address are smaller than a frame
        let span = Span {
            begin: 0,
            end: 1,
            offset: 0,
        };
        let span2 = Span {
            begin: 3,
            end: 4,
            offset: BLK_SIZE as u64,
        };
        let addr = Addr {
            len: BLK_SIZE + 3,
            list: vec![span.clone(), span2.clone()],
        };
        let frms = addr.split_to_frames();
        assert_eq!(frms.len(), 1);
        assert_eq!(frms[0].len, addr.len);
        assert_eq!(frms[0].list.len(), 2);
        assert_eq!(frms[0].list[0], span);
        assert_eq!(frms[0].list[1], span2);

        // #5, 2 address is greater than a frame
        let span = Span {
            begin: 0,
            end: 1,
            offset: 0,
        };
        let span2 = Span {
            begin: 3,
            end: BLKS_PER_FRAME as u64 + 3,
            offset: BLK_SIZE as u64,
        };
        let addr = Addr {
            len: BLK_SIZE + FRAME_SIZE,
            list: vec![span.clone(), span2.clone()],
        };
        let frms = addr.split_to_frames();
        assert_eq!(frms.len(), 2);
        assert_eq!(frms[0].len, FRAME_SIZE);
        assert_eq!(frms[0].list.len(), 2);
        assert_eq!(frms[0].list[0], span);
        let end = span2.begin + BLKS_PER_FRAME as u64 - 1;
        assert_eq!(
            frms[0].list[1],
            Span {
                begin: span2.begin,
                end,
                offset: BLK_SIZE as u64,
            }
        );
        assert_eq!(frms[1].len, BLK_SIZE);
        assert_eq!(frms[1].list.len(), 1);
        assert_eq!(
            frms[1].list[0],
            Span {
                begin: end,
                end: end + 1,
                offset: FRAME_SIZE as u64,
            }
        );

        // #6, 1 address is greater than 2 frame
        let span = Span {
            begin: 0,
            end: BLKS_PER_FRAME as u64 * 2 + 1,
            offset: 0,
        };
        let addr = Addr {
            len: FRAME_SIZE * 2 + 3,
            list: vec![span.clone()],
        };
        let frms = addr.split_to_frames();
        assert_eq!(frms.len(), 3);
        assert_eq!(frms[0].len, FRAME_SIZE);
        assert_eq!(frms[0].list.len(), 1);
        assert_eq!(
            frms[0].list[0],
            Span {
                begin: 0,
                end: BLKS_PER_FRAME as u64,
                offset: 0,
            }
        );
        assert_eq!(frms[1].len, FRAME_SIZE);
        assert_eq!(frms[1].list.len(), 1);
        assert_eq!(
            frms[1].list[0],
            Span {
                begin: BLKS_PER_FRAME as u64,
                end: BLKS_PER_FRAME as u64 * 2,
                offset: FRAME_SIZE as u64,
            }
        );
        assert_eq!(frms[2].len, 3);
        assert_eq!(frms[2].list.len(), 1);
        assert_eq!(
            frms[2].list[0],
            Span {
                begin: BLKS_PER_FRAME as u64 * 2,
                end: BLKS_PER_FRAME as u64 * 2 + 1,
                offset: FRAME_SIZE as u64 * 2,
            }
        );
    }
}
