#![allow(dead_code)]

use std::fmt::{self, Debug};
use std::ptr;

// libsodium ffi
extern "C" {
    fn randombytes_buf(buf: *mut u8, size: usize);
    fn randombytes_uniform(upper_bound: u32) -> u32;
    fn randombytes_buf_deterministic(
        buf: *mut u8,
        size: usize,
        seed: *const u8,
    );

    fn crypto_generichash(
        out: *mut u8,
        outlen: usize,
        inbuf: *const u8,
        inlen: u64,
        key: *const u8,
        keylen: usize,
    ) -> i32;
}

pub fn random_buf(buf: &mut [u8]) {
    unsafe {
        randombytes_buf(buf.as_mut_ptr(), buf.len());
    }
}

pub fn random_buf_deterministic(buf: &mut [u8], seed: &RandomSeed) {
    unsafe {
        randombytes_buf_deterministic(
            buf.as_mut_ptr(),
            buf.len(),
            seed.as_ptr(),
        );
    }
}

pub fn random_usize(upper_bound: usize) -> usize {
    unsafe { randombytes_uniform(upper_bound as u32) as usize }
}

pub fn random_u32(upper_bound: u32) -> u32 {
    unsafe { randombytes_uniform(upper_bound) }
}

pub fn random_slice(buf: &[u8]) -> (usize, &[u8]) {
    let pos = random_usize(buf.len());
    let len = random_usize(buf.len() - pos);
    (pos, &buf[pos..(pos + len)])
}

pub fn random_slice_with_len(buf: &[u8], len: usize) -> &[u8] {
    let pos = random_usize(buf.len() - len);
    &buf[pos..(pos + len)]
}

pub const RANDOM_SEED_SIZE: usize = 32;

#[derive(Debug, Default)]
pub struct RandomSeed(pub [u8; RANDOM_SEED_SIZE]);

impl RandomSeed {
    pub fn new() -> Self {
        let mut seed = Self::default();
        random_buf(&mut seed.0);
        seed
    }

    pub fn from(seed: &[u8]) -> Self {
        assert_eq!(seed.len(), RANDOM_SEED_SIZE);
        let mut ret = RandomSeed([0u8; RANDOM_SEED_SIZE]);
        &ret.0[..].copy_from_slice(seed);
        ret
    }

    pub fn as_ptr(&self) -> *const u8 {
        (&self.0).as_ptr()
    }
}

pub const HASH_SIZE: usize = 32;

#[derive(Clone, Eq, PartialEq)]
pub struct Hash([u8; HASH_SIZE]);

impl Hash {
    pub fn new() -> Self {
        Hash([0; HASH_SIZE])
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.0.as_mut_ptr()
    }
}

pub fn hash(inbuf: &[u8]) -> Hash {
    let mut ret = Hash::new();
    unsafe {
        match crypto_generichash(
            ret.as_mut_ptr(),
            HASH_SIZE,
            inbuf.as_ptr(),
            inbuf.len() as u64,
            ptr::null(),
            0,
        ) {
            0 => ret,
            _ => unreachable!(),
        }
    }
}

impl Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Hash({}..)", &self.to_string()[..6])
    }
}

impl ToString for Hash {
    fn to_string(&self) -> String {
        let strs: Vec<String> =
            self.0.iter().map(|b| format!("{:x}", b)).collect();
        strs.join("")
    }
}
