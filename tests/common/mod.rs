use std::ptr;
use std::fmt::{self, Debug};
use std::cmp::min;
use tempdir::TempDir;
use zbox::{zbox_init, Repo, RepoOpener};

#[derive(Debug)]
#[allow(dead_code)]
pub struct TestEnv {
    pub repo: Repo,
    pub tmpdir: TempDir,
}

impl TestEnv {
    #[allow(dead_code)]
    pub fn reopen(&mut self) {
        let tmpdir = TempDir::new("zbox_test").expect("Create temp dir failed");
        let dir = tmpdir.path().join("repo");
        let path = "file://".to_string() + dir.to_str().unwrap();
        let dummy_repo =
            RepoOpener::new().create(true).open(&path, "pwd").unwrap();

        let uri = self.repo.info().uri.clone();
        self.repo = dummy_repo;
        self.repo = RepoOpener::new().open(&uri, "pwd").unwrap();
    }
}

pub fn setup() -> TestEnv {
    zbox_init();
    let tmpdir = TempDir::new("zbox_test").expect("Create temp dir failed");
    let dir = tmpdir.path().join("repo");
    let path = "file://".to_string() + dir.to_str().unwrap();
    let repo = RepoOpener::new().create(true).open(&path, "pwd").unwrap();
    TestEnv { repo, tmpdir }
}

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

#[allow(dead_code)]
pub fn random_buf(buf: &mut [u8]) {
    unsafe {
        randombytes_buf(buf.as_mut_ptr(), buf.len());
    }
}

#[allow(dead_code)]
pub fn random_buf_deterministic(buf: &mut [u8], seed: &RandomSeed) {
    unsafe {
        randombytes_buf_deterministic(
            buf.as_mut_ptr(),
            buf.len(),
            seed.as_ptr(),
        );
    }
}

#[allow(dead_code)]
pub fn random_usize(upper_bound: usize) -> usize {
    unsafe { randombytes_uniform(upper_bound as u32) as usize }
}

#[allow(dead_code)]
pub fn random_u32(upper_bound: u32) -> u32 {
    unsafe { randombytes_uniform(upper_bound) }
}

#[allow(dead_code)]
pub fn random_slice(buf: &[u8]) -> (usize, &[u8]) {
    let pos = random_usize(buf.len());
    let len = random_usize(buf.len() - pos);
    (pos, &buf[pos..(pos + len)])
}

#[allow(dead_code)]
pub fn random_slice_with_len(buf: &[u8], len: usize) -> &[u8] {
    let pos = random_usize(buf.len() - len);
    &buf[pos..(pos + len)]
}

pub const RANDOM_SEED_SIZE: usize = 32;

#[derive(Debug, Default)]
pub struct RandomSeed(pub [u8; RANDOM_SEED_SIZE]);

impl RandomSeed {
    #[allow(dead_code)]
    pub fn new() -> Self {
        let mut seed = Self::default();
        random_buf(&mut seed.0);
        seed
    }

    #[allow(dead_code)]
    pub fn from(seed: &[u8; RANDOM_SEED_SIZE]) -> Self {
        RandomSeed(seed.clone())
    }

    pub fn as_ptr(&self) -> *const u8 {
        (&self.0).as_ptr()
    }
}

pub const HASH_SIZE: usize = 32;

#[allow(dead_code)]
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

#[allow(dead_code)]
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

#[derive(Debug, Clone)]
pub struct Span {
    pub pos: usize,
    pub len: usize,
}

// make random test data
// return: (random seed, permutation sequence, data buffer)
// item in permutation sequence:
//   (span in random data buffer, position in data buffer)
#[allow(dead_code)]
pub fn make_test_data(
    rnd_data_len: usize,
    data_len: usize,
) -> (RandomSeed, Vec<(Span, usize)>, Vec<u8>) {
    let mut rnd_data = vec![0u8; rnd_data_len];
    let seed = RandomSeed::new();
    random_buf_deterministic(&mut rnd_data, &seed);

    // init data buffer
    let mut data = vec![0u8; data_len];
    let mut permu = Vec::new();
    for _ in 0..5 {
        let pos = random_usize(data_len);
        let rnd_pos = random_usize(rnd_data_len);
        let max_len = min(data_len - pos, rnd_data_len - rnd_pos);
        let len = random_u32(max_len as u32) as usize;
        permu.push((Span { pos: rnd_pos, len }, pos));
        &mut data[pos..pos + len].copy_from_slice(
            &rnd_data[rnd_pos..rnd_pos + len],
        );
    }

    (seed, permu, data)
}

// reproduce test data
#[allow(dead_code)]
pub fn reprod_test_data(
    seed: RandomSeed,
    permu: Vec<(Span, usize)>,
    rnd_data_len: usize,
    data_len: usize,
) -> Vec<u8> {
    // init random data buffer
    let mut rnd_data = vec![0u8; rnd_data_len];
    random_buf_deterministic(&mut rnd_data, &seed);

    // init data buffer
    let mut data = vec![0u8; data_len];
    for opr in permu {
        let pos = opr.1;
        let rnd_pos = opr.0.pos;
        let len = opr.0.len;
        &mut data[pos..pos + len].copy_from_slice(
            &rnd_data[rnd_pos..rnd_pos + len],
        );
    }

    data
}
