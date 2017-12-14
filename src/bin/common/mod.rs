extern crate bytes;
extern crate zbox;

use std::ptr;
use std::fmt::{self, Debug};
use std::cmp::min;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::io::{Read, Write, Seek, SeekFrom, Cursor};
use std::sync::{Arc, RwLock};

use self::bytes::{Buf, BufMut, LittleEndian};
use zbox::{init_env, Repo, RepoOpener};

const RND_DATA_LEN: usize = 2 * 1024 * 1024;
const DATA_LEN: usize = 2 * RND_DATA_LEN;

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

// permutation
// item in permutation sequence:
//   (span in random data buffer, position in data buffer)
type Permu = Vec<(Span, usize)>;

// make random test data
// return: (random seed, permutation sequence, data buffer)
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
    permu: Permu,
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

#[derive(Debug)]
pub struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> Self {
        let mut buf = [0; 8];
        random_buf(&mut buf);
        let dirnames: Vec<String> =
            buf.iter().map(|b| format!("{:x}", b)).collect();
        let dirname = format!("zbox_test_{}", dirnames.join(""));

        let mut dir = env::temp_dir();
        dir.push(&dirname);

        fs::create_dir(&dir).expect("Create temp dir failed");
        TempDir { path: dir }
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.path).unwrap();
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct TestEnv {
    pub repo: Repo,
    pub tmpdir: TempDir,
}

impl TestEnv {
    #[allow(dead_code)]
    pub fn reopen(&mut self) {
        let tmpdir = TempDir::new();
        let dir = tmpdir.path.join("repo");
        let path = "file://".to_string() + dir.to_str().unwrap();
        let dummy_repo =
            RepoOpener::new().create(true).open(&path, "pwd").unwrap();

        let info = self.repo.info();
        self.repo = dummy_repo;
        self.repo = RepoOpener::new().open(info.uri(), "pwd").unwrap();
    }
}

pub fn setup() -> TestEnv {
    init_env();
    let tmpdir = TempDir::new();
    let dir = tmpdir.path.join("repo");
    let path = "file://".to_string() + dir.to_str().unwrap();
    let repo = RepoOpener::new().create(true).open(&path, "pwd").unwrap();
    TestEnv { repo, tmpdir }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct TestEnv2 {
    pub path: PathBuf,
    pub repo: Repo,
    pub data: Vec<u8>,
}

impl TestEnv2 {
    pub fn new(name: &str) -> Self {
        init_env();
        let base = PathBuf::from("./fuzz_test/");
        let tm = format!(
            "{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );
        let path = base.join(name.to_string() + "-" + &tm);
        fs::create_dir_all(&path).unwrap();

        // open repo
        println!("Start fuzz test at {:?}", path);
        let repo_path = "file://".to_string() +
            path.join("repo").to_str().unwrap();
        let repo = RepoOpener::new()
            .create(true)
            .open(&repo_path, "pwd")
            .unwrap();

        // create test environment
        let mut ret = TestEnv2 {
            path,
            repo,
            data: vec![0; DATA_LEN],
        };

        // make test data
        ret.make_test_data();

        ret
    }

    pub fn into_ref(self) -> TestEnv2Ref {
        Arc::new(RwLock::new(self))
    }

    fn make_test_data(&mut self) {
        let mut rnd_data = vec![0u8; RND_DATA_LEN];
        let seed = RandomSeed::new();
        random_buf_deterministic(&mut rnd_data, &seed);

        // fill data buffer with random data
        let mut permu: Permu = Vec::new();
        for _ in 0..5 {
            let pos = random_usize(DATA_LEN);
            let rnd_pos = random_usize(RND_DATA_LEN);
            let max_len = min(DATA_LEN - pos, RND_DATA_LEN - rnd_pos);
            let len = random_u32(max_len as u32) as usize;
            permu.push((Span { pos: rnd_pos, len }, pos));
            &mut self.data[pos..pos + len].copy_from_slice(
                &rnd_data[rnd_pos..rnd_pos + len],
            );
        }

        // save seed
        let seed_path = self.path.join("seed");
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&seed_path)
            .unwrap();
        file.write_all(&seed.0).unwrap();

        // save permutation
        let mut buf = Vec::new();
        let permu_path = self.path.join("permu");
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&permu_path)
            .unwrap();
        for &(ref span, pos) in permu.iter() {
            buf.clear();
            buf.put_u64::<LittleEndian>(span.pos as u64);
            buf.put_u64::<LittleEndian>(span.len as u64);
            buf.put_u64::<LittleEndian>(pos as u64);
            file.write_all(&buf).unwrap();
        }
    }

    pub fn load(path: &str) -> Self {
        init_env();
        let base = PathBuf::from("./fuzz_test/");
        let path = base.join(path);

        // load seed
        let mut buf = Vec::new();
        let seed_path = path.join("seed");
        let mut file = fs::File::open(&seed_path).unwrap();
        file.read_to_end(&mut buf).unwrap();
        let seed = RandomSeed::from(&buf[..32]);

        // load permutation
        let mut buf = Vec::new();
        let permu_path = path.join("permu");
        let mut file = fs::File::open(&permu_path).unwrap();
        let read = file.read_to_end(&mut buf).unwrap();
        let mut permu: Permu = Vec::new();
        for chunk in buf.chunks(3 * 8) {
            // chunk is 3 * u64 integers
            let mut cur = Cursor::new(chunk);
            let pos = cur.get_u64::<LittleEndian>() as usize;
            let len = cur.get_u64::<LittleEndian>() as usize;
            let span = Span { pos, len };
            let pos = cur.get_u64::<LittleEndian>() as usize;
            permu.push((span, pos));
        }

        // reproduce test data
        let mut rnd_data = vec![0u8; RND_DATA_LEN];
        random_buf_deterministic(&mut rnd_data, &seed);
        let mut data = vec![0u8; DATA_LEN];
        for opr in permu {
            let pos = opr.1;
            let rnd_pos = opr.0.pos;
            let len = opr.0.len;
            &mut data[pos..pos + len].copy_from_slice(
                &rnd_data[rnd_pos..rnd_pos + len],
            );
        }

        // open repo
        let repo_path = path.join("repo");
        fs::remove_dir_all(&repo_path).unwrap();
        let repo_path = "file://".to_string() + repo_path.to_str().unwrap();
        let repo = RepoOpener::new()
            .create(true)
            .open(&repo_path, "pwd")
            .unwrap();

        println!("Fuzz test loaded {:?}", path);

        // create test environment
        TestEnv2 { path, repo, data }
    }
}

pub type TestEnv2Ref = Arc<RwLock<TestEnv2>>;
