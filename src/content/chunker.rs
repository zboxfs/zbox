use std::cmp::min;
use std::fmt::{self, Debug};
use std::io::{Result as IoResult, Seek, SeekFrom, Write};
use std::ptr;
use rand::prelude::{Distribution, ThreadRng};
use rand_distr::Normal;

use serde::{Deserialize, Serialize};

// writer buffer length
const BUFFER_SIZE: usize = 8 * MAX_CHUNK_SIZE;

// leap-based cdc constants
const MIN_CHUNK_SIZE: usize = 1024 * 16;
const MAX_CHUNK_SIZE: usize = 1024 * 64;

const WINDOW_PRIMARY_COUNT: usize = 22;
const WINDOW_SECONDARY_COUNT: usize = 2;
const WINDOW_COUNT: usize = WINDOW_PRIMARY_COUNT + WINDOW_SECONDARY_COUNT;

const WINDOW_SIZE: usize = 180;
const WINDOW_MATRIX_SHIFT: usize = 42; // WINDOW_MATRIX_SHIFT * 4 < WINDOW_SIZE - 5
const MATRIX_WIDTH: usize = 8;
const MATRIX_HEIGHT: usize = 255;

enum PointStatus {
    Satisfied,
    Unsatisfied(usize),
}

/// Chunker
pub struct Chunker<W: Write + Seek> {
    dst: W,                // destination writer
    pos: usize,
    chunk_len: usize,
    buf_clen: usize,
    ef_matrix: Vec<Vec<u8>>,
    buf: Vec<u8>,        // chunker buffer, fixed size: BUFFER_SIZE
}

impl<W: Write + Seek> Chunker<W> {
    pub fn new(dst: W) -> Self {
        let mut buf = vec![0u8; BUFFER_SIZE];
        buf.shrink_to_fit();
        let ef_matrix = generate_ef_matrix();

        Chunker {
            dst,
            pos: MIN_CHUNK_SIZE,
            chunk_len: MIN_CHUNK_SIZE,
            buf_clen: 0,
            ef_matrix,
            buf,
        }
    }

    pub fn into_inner(mut self) -> IoResult<W> {
        self.flush()?;
        Ok(self.dst)
    }

    fn is_point_satisfied(&self) -> PointStatus {
        // primary check, T<=x<M where T is WINDOW_SECONDARY_COUNT and M is WINDOW_COUNT
        for i in WINDOW_SECONDARY_COUNT..WINDOW_COUNT {
            if !self.is_window_qualified(&self.buf[self.pos - i - WINDOW_SIZE..self.pos - i]) { // window is WINDOW_SIZE bytes long and moves to the left
                let leap = WINDOW_COUNT - i;
                return PointStatus::Unsatisfied(leap);
            }
        }

        //secondary check, 0<=x<T bytes
        for i in 0..WINDOW_SECONDARY_COUNT {
            if !self.is_window_qualified(&self.buf[self.pos - i - WINDOW_SIZE..self.pos - i]) {
                let leap = WINDOW_COUNT - WINDOW_SECONDARY_COUNT - i;
                return PointStatus::Unsatisfied(leap);
            }
        }

        PointStatus::Satisfied
    }

    fn is_window_qualified(&self, window: &[u8]) -> bool {
        (0..5)
            .map(|index| window[WINDOW_SIZE - 1 - index * WINDOW_MATRIX_SHIFT]) // init array
            .enumerate()
            .map(|(index, byte)| self.ef_matrix[byte as usize][index]) // get elements from ef_matrix
            .fold(0, |acc, value| acc ^ (value as usize)) // why is acc of type usize?
            != 0
    }

    fn write_to_dst(&mut self) -> IoResult<usize> {
        let p = self.pos - self.chunk_len;
        let written = self.dst.write(&self.buf[p..self.pos])?;
        assert_eq!(written, self.chunk_len);

        if self.pos + MAX_CHUNK_SIZE >= BUFFER_SIZE {
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

        self.pos += MIN_CHUNK_SIZE;
        self.chunk_len = MIN_CHUNK_SIZE;
        Ok(written)
    }
}

impl<W: Write + Seek> Write for Chunker<W> {
    // consume bytes stream, output chunks
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        // copy source data into chunker buffer
        let in_len = min(BUFFER_SIZE - self.buf_clen, buf.len());
        assert!(in_len > 0);
        self.buf[self.buf_clen..self.buf_clen + in_len]
            .copy_from_slice(&buf[..in_len]);
        self.buf_clen += in_len;

        while self.pos < self.buf_clen {
            if self.chunk_len >= MAX_CHUNK_SIZE {
                self.write_to_dst()?;
            } else {
                match self.is_point_satisfied() {
                    PointStatus::Satisfied => {
                        self.write_to_dst()?;
                    }
                    PointStatus::Unsatisfied(leap) => {
                        self.pos += leap;
                        self.chunk_len += leap;
                    },
                };
            }
        }
        Ok(in_len)
    }

    fn flush(&mut self) -> IoResult<()> {
        // flush remaining data to destination
        let p = self.pos - self.chunk_len;
        if p < self.buf_clen {
            self.chunk_len = self.buf_clen - p;
            let _ = self.dst.write(&self.buf[p..(p + self.chunk_len)])?;
        }

        // reset chunker
        self.pos = MIN_CHUNK_SIZE;
        self.chunk_len = MIN_CHUNK_SIZE;
        self.buf_clen = 0;

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

fn generate_ef_matrix() -> Vec<Vec<u8>> {
    let base_matrix = (0..=255)
        .map(|index| vec![index; 5])
        .collect::<Vec<Vec<u8>>>(); // 256x5 matrix that looks like ((0,0,0,0,0), (1,1,1,1,1)..)

    let matrix_h = generate_matrix();
    let matrix_g = generate_matrix();

    let e_matrix = transform_base_matrix(&base_matrix, &matrix_h);
    let f_matrix = transform_base_matrix(&base_matrix, &matrix_g);

    let ef_matrix = e_matrix.iter().zip(f_matrix.iter())
        .map(concatenate_bits_in_rows)
        .collect();
    ef_matrix
}

fn transform_base_matrix(base_matrix: &[Vec<u8>], additional_matrix: &[Vec<f64>]) -> Vec<Vec<bool>> {
    base_matrix.iter()
        .map(|row| transform_byte_row(row[0], additional_matrix))
        .collect::<Vec<Vec<bool>>>()
}

fn concatenate_bits_in_rows((row_x, row_y): (&Vec<bool>, &Vec<bool>)) -> Vec<u8> {
    row_x.iter().zip(row_y.iter())
        .map(concatenate_bits)
        .collect()
}

fn concatenate_bits((x, y): (&bool, &bool)) -> u8 {
    match (*x, *y) {
        (true, true) => 3,
        (true, false) => 2,
        (false, true) => 1,
        (false, false) => 0,
    }
}

fn transform_byte_row(byte: u8, matrix: &[Vec<f64>]) -> Vec<bool> {
    let mut new_row = vec![0u8; 5];
    (0..255)
        .map(|index| multiply_rows(byte, &matrix[index]))
        .enumerate()
        .for_each(|(index, value)| if value > 0.0 { new_row[index / 51] += 1; });

    new_row.iter()
        .map(|&number| if number % 2 == 0 {false} else {true})
        .collect::<Vec<bool>>()
}

fn multiply_rows(byte: u8, numbers: &[f64]) -> f64 {
    numbers.iter().enumerate()
        .map(|(index, number)| if (byte >> index) & 1 == 1 {*number} else {-(*number)})
        .sum()
}

fn generate_matrix() -> Vec<Vec<f64>> {
    let normal = Normal::new(0.0, 1.0).unwrap();
    let mut rng = rand::thread_rng();

    (0..MATRIX_HEIGHT)
        .map(|_| generate_row(&normal, &mut rng))
        .collect()
}

fn generate_row(normal: &Normal<f64>, rng: &mut ThreadRng) -> Vec<f64> {
    (0..MATRIX_WIDTH)
        .map(|_| normal.sample(rng))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::io::{copy, Cursor, Result as IoResult, Seek, SeekFrom, Write};
    use std::time::Instant;

    use super::*;
    use crate::base::crypto::{Crypto, RandomSeed, RANDOM_SEED_SIZE};
    use crate::base::init_env;
    use crate::base::utils::speed_str;
    use crate::content::chunk::Chunk;

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
        let mut data = vec![0u8; DATA_LEN];
        Crypto::random_buf(&mut data);
        let mut cur = Cursor::new(data);
        let sinker = Sinker {
            len: 0,
            chks: Vec::new(),
        };

        // test chunker
        let mut ckr = Chunker::new(sinker);
        let result = copy(&mut cur, &mut ckr);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), DATA_LEN as u64);
        ckr.flush().unwrap();
    }

    #[test]
    fn chunker_perf() {
        init_env();

        // prepare test data
        const DATA_LEN: usize = 800 * 1024 * 1024;
        let mut data = vec![0u8; DATA_LEN];
        let seed = RandomSeed::from(&[0u8; RANDOM_SEED_SIZE]);
        Crypto::random_buf_deterministic(&mut data, &seed);
        let mut cur = Cursor::new(data);
        let sinker = VoidSinker {};

        // test chunker performance
        let mut ckr = Chunker::new(sinker);
        let now = Instant::now();
        copy(&mut cur, &mut ckr).unwrap();
        ckr.flush().unwrap();
        let time = now.elapsed();

        println!("Chunker perf: {}", speed_str(&time, DATA_LEN));
    }
}
