use std::cmp::min;
use std::fmt::{self, Debug};
use std::io::{Result as IoResult, Seek, SeekFrom, Write};
use std::ptr;

// taken from pcompress implementation
// https://github.com/moinakg/pcompress
const PRIME: u64 = 153191u64;
const MASK: u64 = 0xffffffffffu64;
const MIN_SIZE: usize = 16 * 1024; // minimal chunk size, 16k
const AVG_SIZE: usize = 32 * 1024; // average chunk size, 32k
const MAX_SIZE: usize = 64 * 1024; // maximum chunk size, 64k

// Irreducible polynomial for Rabin modulus, from pcompress
const FP_POLY: u64 = 0xbfe6b8a5bf378d83u64;

// since we will skip MIN_SIZE when sliding window, it only
// needs to target (AVG_SIZE - MIN_SIZE) cut length,
// note the (AVG_SIZE - MIN_SIZE) must be 2^n
const CUT_MASK: u64 = (AVG_SIZE - MIN_SIZE - 1) as u64;

// rolling hash window constants
const WIN_SIZE: usize = 16; // must be 2^n
const WIN_MASK: usize = WIN_SIZE - 1;
const WIN_SLIDE_OFFSET: usize = 64;
const WIN_SLIDE_POS: usize = MIN_SIZE - WIN_SLIDE_OFFSET;

// writer buffer length
const WTR_BUF_LEN: usize = 8 * MAX_SIZE;

/// Pre-calculated chunker parameters
#[derive(Clone, Deserialize, Serialize)]
pub struct ChunkerParams {
    poly_pow: u64,     // poly power
    out_map: Vec<u64>, // pre-computed out byte map, length is 256
    ir: Vec<u64>,      // irreducible polynomial, length is 256
}

impl ChunkerParams {
    pub fn new() -> Self {
        let mut cp = ChunkerParams::default();

        // calculate poly power, it is actually PRIME ^ WIN_SIZE
        for _ in 0..WIN_SIZE {
            cp.poly_pow = (cp.poly_pow * PRIME) & MASK;
        }

        // pre-calculate out map table and irreducible polynomial
        // for each possible byte, copy from PCompress implementation
        for i in 0..256 {
            cp.out_map[i] = (i as u64 * cp.poly_pow) & MASK;

            let (mut term, mut pow, mut val) = (1u64, 1u64, 1u64);
            for _ in 0..WIN_SIZE {
                if (term & FP_POLY) != 0 {
                    val += (pow * i as u64) & MASK;
                }
                pow = (pow * PRIME) & MASK;
                term *= 2;
            }
            cp.ir[i] = val;
        }

        cp
    }
}

impl Debug for ChunkerParams {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ChunkerParams()")
    }
}

impl Default for ChunkerParams {
    fn default() -> Self {
        let mut ret = ChunkerParams {
            poly_pow: 1,
            out_map: vec![0u64; 256],
            ir: vec![0u64; 256],
        };
        ret.out_map.shrink_to_fit();
        ret.ir.shrink_to_fit();
        ret
    }
}

/// Chunker
pub struct Chunker<W: Write + Seek> {
    dst: W,                // destination writer
    params: ChunkerParams, // chunker parameters
    pos: usize,
    chunk_len: usize,
    buf_clen: usize,
    win_idx: usize,
    roll_hash: u64,
    win: [u8; WIN_SIZE], // rolling hash circle window
    buf: Vec<u8>,        // chunker buffer, fixed size: WTR_BUF_LEN
}

impl<W: Write + Seek> Chunker<W> {
    pub fn new(params: ChunkerParams, dst: W) -> Self {
        let mut buf = vec![0u8; WTR_BUF_LEN];
        buf.shrink_to_fit();

        Chunker {
            dst,
            params,
            pos: WIN_SLIDE_POS,
            chunk_len: WIN_SLIDE_POS,
            buf_clen: 0,
            win_idx: 0,
            roll_hash: 0,
            win: [0u8; WIN_SIZE],
            buf,
        }
    }

    pub fn into_inner(mut self) -> IoResult<W> {
        self.flush()?;
        Ok(self.dst)
    }
}

impl<W: Write + Seek> Write for Chunker<W> {
    // consume bytes stream, output chunks
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        // copy source data into chunker buffer
        let in_len = min(WTR_BUF_LEN - self.buf_clen, buf.len());
        assert!(in_len > 0);
        self.buf[self.buf_clen..self.buf_clen + in_len]
            .copy_from_slice(&buf[..in_len]);
        self.buf_clen += in_len;

        while self.pos < self.buf_clen {
            // get current byte and pushed out byte
            let ch = self.buf[self.pos];
            let out = self.win[self.win_idx] as usize;
            let pushed_out = self.params.out_map[out];

            // calculate Rabin rolling hash
            self.roll_hash = (self.roll_hash * PRIME) & MASK;
            self.roll_hash += ch as u64;
            self.roll_hash = self.roll_hash.wrapping_sub(pushed_out) & MASK;

            // forward circle window
            self.win[self.win_idx] = ch;
            self.win_idx = (self.win_idx + 1) & WIN_MASK;

            self.chunk_len += 1;
            self.pos += 1;

            if self.chunk_len >= MIN_SIZE {
                let chksum = self.roll_hash ^ self.params.ir[out];

                // reached cut point, chunk can be produced now
                if (chksum & CUT_MASK) == 0 || self.chunk_len >= MAX_SIZE {
                    // write the chunk to destination writer,
                    // ensure it is consumed in whole
                    let p = self.pos - self.chunk_len;
                    let written = self.dst.write(&self.buf[p..self.pos])?;
                    assert_eq!(written, self.chunk_len);

                    // not enough space in buffer, copy remaining to
                    // the head of buffer and reset buf position
                    if self.pos + MAX_SIZE >= WTR_BUF_LEN {
                        let left_len = self.buf_clen - self.pos;
                        unsafe {
                            ptr::copy::<u8>(
                                self.buf[self.pos..].as_ptr(),
                                self.buf.as_mut_ptr(),
                                left_len,
                            );
                        }
                        self.buf_clen = left_len;
                        self.pos = 0;
                    }

                    // jump to next start sliding position
                    self.pos += WIN_SLIDE_POS;
                    self.chunk_len = WIN_SLIDE_POS;
                }
            }
        }

        Ok(in_len)
    }

    fn flush(&mut self) -> IoResult<()> {
        // flush remaining data to destination
        let p = self.pos - self.chunk_len;
        if p < self.buf_clen {
            self.chunk_len = self.buf_clen - p;
            self.dst.write(&self.buf[p..(p + self.chunk_len)])?;
        }

        // reset chunker
        self.pos = WIN_SLIDE_POS;
        self.chunk_len = WIN_SLIDE_POS;
        self.buf_clen = 0;
        self.win_idx = 0;
        self.roll_hash = 0;
        self.win = [0u8; WIN_SIZE];

        self.dst.flush()
    }
}

impl<W: Write + Seek> Debug for Chunker<W> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Chunker()")
    }
}

impl<W: Write + Seek> Seek for Chunker<W> {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        self.dst.seek(pos)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{copy, Cursor, Result as IoResult, Seek, SeekFrom, Write};
    use std::time::Instant;

    use super::*;
    use base::crypto::{Crypto, RandomSeed, RANDOM_SEED_SIZE};
    use base::init_env;
    use base::utils::speed_str;
    use content::chunk::Chunk;

    #[derive(Debug)]
    struct Sinker {
        len: usize,
        chks: Vec<Chunk>,
    }

    impl Write for Sinker {
        fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
            self.chks.push(Chunk::new(self.len, buf.len()));
            self.len += buf.len();
            Ok(buf.len())
        }

        fn flush(&mut self) -> IoResult<()> {
            // verify
            let sum = self.chks.iter().fold(0, |sum, ref t| sum + t.len);
            assert_eq!(sum, self.len);
            for i in 0..(self.chks.len() - 2) {
                assert_eq!(
                    self.chks[i].pos + self.chks[i].len,
                    self.chks[i + 1].pos
                );
            }

            Ok(())
        }
    }

    impl Seek for Sinker {
        fn seek(&mut self, _: SeekFrom) -> IoResult<u64> {
            Ok(0)
        }
    }

    #[derive(Debug)]
    struct VoidSinker {}

    impl Write for VoidSinker {
        fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
            Ok(buf.len())
        }

        fn flush(&mut self) -> IoResult<()> {
            Ok(())
        }
    }

    impl Seek for VoidSinker {
        fn seek(&mut self, _: SeekFrom) -> IoResult<u64> {
            Ok(0)
        }
    }

    #[test]
    fn chunker() {
        init_env();

        // perpare test data
        const DATA_LEN: usize = 765 * 1024;
        let params = ChunkerParams::new();
        let mut data = vec![0u8; DATA_LEN];
        Crypto::random_buf(&mut data);
        let mut cur = Cursor::new(data);
        let sinker = Sinker {
            len: 0,
            chks: Vec::new(),
        };

        // test chunker
        let mut ckr = Chunker::new(params, sinker);
        let result = copy(&mut cur, &mut ckr);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), DATA_LEN as u64);
        ckr.flush().unwrap();
    }

    #[test]
    fn chunker_perf() {
        init_env();

        // perpare test data
        const DATA_LEN: usize = 10 * 1024 * 1024;
        let params = ChunkerParams::new();
        let mut data = vec![0u8; DATA_LEN];
        let seed = RandomSeed::from(&[0u8; RANDOM_SEED_SIZE]);
        Crypto::random_buf_deterministic(&mut data, &seed);
        let mut cur = Cursor::new(data);
        let sinker = VoidSinker {};

        // test chunker performance
        let mut ckr = Chunker::new(params, sinker);
        let now = Instant::now();
        copy(&mut cur, &mut ckr).unwrap();
        ckr.flush().unwrap();
        let time = now.elapsed();

        println!("Chunker perf: {}", speed_str(&time, DATA_LEN));
    }
}
