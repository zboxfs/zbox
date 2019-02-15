#![allow(dead_code)]

extern crate bytes;

use std::cmp::min;
use std::fmt::{self, Debug};
use std::fs;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::ops::Index;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use super::controller::imp::Controller;
use super::crypto;

use self::bytes::{Buf, BufMut};
use zbox::{init_env, File, Repo, RepoOpener, Result};

#[derive(Debug, Clone, Copy)]
pub enum FileType {
    File,
    Dir,
}

impl FileType {
    fn random() -> Self {
        match crypto::random_u32(2) {
            0 => FileType::File,
            1 => FileType::Dir,
            _ => unreachable!(),
        }
    }

    fn is_dir(&self) -> bool {
        match *self {
            FileType::Dir => true,
            _ => false,
        }
    }

    fn to_u64(&self) -> u64 {
        match *self {
            FileType::File => 0,
            FileType::Dir => 1,
        }
    }

    fn from_u64(val: u64) -> Self {
        match val {
            0 => FileType::File,
            1 => FileType::Dir,
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Action {
    New,
    Read,
    Update,
    Truncate,
    Delete,
    DeleteAll,
    Rename,
    Move,
    Copy,
    Reopen,
}

impl Action {
    fn new_random() -> Self {
        // give each action different probability, 10 is base line
        let weight = [
            60, // New,
            15, // Read,
            15, // Update,
            10, // Truncate,
            5,  // Delete,
            2,  // DeleteAll,
            10, // Rename,
            10, // Move,
            10, // Copy,
            15, // Reopen,
        ];
        let rnd = crypto::random_u32(weight.iter().sum());
        let (mut idx, mut last) = (0, 0);
        for (i, w) in weight.iter().enumerate() {
            if last <= rnd && rnd < last + *w {
                idx = i;
                break;
            }
            last += *w;
        }
        match idx {
            0 => Action::New,
            1 => Action::Read,
            2 => Action::Update,
            3 => Action::Truncate,
            4 => Action::Delete,
            5 => Action::DeleteAll,
            6 => Action::Rename,
            7 => Action::Move,
            8 => Action::Copy,
            9 => Action::Reopen,
            _ => unreachable!(),
        }
    }

    fn to_u64(&self) -> u64 {
        match *self {
            Action::New => 0,
            Action::Read => 1,
            Action::Update => 2,
            Action::Truncate => 3,
            Action::Delete => 4,
            Action::DeleteAll => 5,
            Action::Rename => 6,
            Action::Move => 7,
            Action::Copy => 8,
            Action::Reopen => 9,
        }
    }

    fn from_u64(val: u64) -> Self {
        match val {
            0 => Action::New,
            1 => Action::Read,
            2 => Action::Update,
            3 => Action::Truncate,
            4 => Action::Delete,
            5 => Action::DeleteAll,
            6 => Action::Rename,
            7 => Action::Move,
            8 => Action::Copy,
            9 => Action::Reopen,
            _ => unreachable!(),
        }
    }
}

// control group node
#[derive(Clone)]
pub struct Node {
    pub path: PathBuf,
    pub ftype: FileType,
    pub data: Vec<u8>,
}

impl Node {
    pub fn new_file<P: AsRef<Path>>(path: P, data: &[u8]) -> Self {
        Node {
            path: path.as_ref().to_path_buf(),
            ftype: FileType::File,
            data: data.to_vec(),
        }
    }

    pub fn new_dir<P: AsRef<Path>>(path: P) -> Self {
        Node {
            path: path.as_ref().to_path_buf(),
            ftype: FileType::Dir,
            data: Vec::new(),
        }
    }

    pub fn is_file(&self) -> bool {
        !self.is_dir()
    }

    pub fn is_dir(&self) -> bool {
        self.ftype.is_dir()
    }

    pub fn is_root(&self) -> bool {
        self.path.to_str().unwrap() == "/"
    }

    pub fn compare_file_content(&self, repo: &mut Repo) -> Result<()> {
        let mut f = repo.open_file(&self.path)?;
        let meta = f.metadata()?;
        assert_eq!(meta.len(), self.data.len());
        let mut dst = Vec::new();
        f.read_to_end(&mut dst)?;
        assert_eq!(&dst[..], &self.data[..]);
        Ok(())
    }
}

impl Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.is_dir() {
            f.debug_struct("Dir").field("path", &self.path).finish()
        } else {
            f.debug_struct("File")
                .field("path", &self.path)
                .field("len", &self.data.len())
                .finish()
        }
    }
}

// control group
#[derive(Debug, Clone)]
pub struct ControlGroup(pub Vec<Node>);

impl ControlGroup {
    fn new() -> Self {
        ControlGroup(vec![Node::new_dir("/")])
    }

    #[inline]
    pub fn add_file(&mut self, path: &Path, data: &[u8]) {
        self.0.push(Node::new_file(path, data));
    }

    #[inline]
    pub fn add_dir(&mut self, path: &Path) {
        self.0.push(Node::new_dir(path));
    }

    #[inline]
    pub fn find_node(&self, path: &Path) -> Option<&Node> {
        self.0.iter().find(|&p| &p.path == path)
    }

    #[inline]
    pub fn find_node_mut(&mut self, path: &Path) -> Option<&mut Node> {
        self.0.iter_mut().find(|ref p| p.path == path)
    }

    #[inline]
    pub fn has_node(&self, path: &Path) -> bool {
        self.find_node(path).is_some()
    }

    // get immediate children
    pub fn get_children(&self, path: &Path) -> Vec<&PathBuf> {
        let mut dirs: Vec<&PathBuf> = self
            .0
            .iter()
            .skip(1) // skip root
            .map(|n| &n.path)
            .filter(|p| p.parent().unwrap() == path)
            .collect();
        dirs.sort();
        dirs
    }

    pub fn has_child(&self, path: &Path) -> bool {
        self.0
            .iter()
            .skip(1) // skip root
            .any(|n| n.path.parent().unwrap() == path)
    }

    pub fn del(&mut self, path: &Path) {
        self.0.retain(|n| &n.path != path);
    }

    pub fn del_all_children(&mut self, path: &Path) {
        self.0.retain(|n| !n.path.starts_with(&path));
    }
}

impl Index<usize> for ControlGroup {
    type Output = Node;

    fn index(&self, index: usize) -> &Node {
        self.0.index(index)
    }
}

// test round step
#[derive(Clone)]
pub struct Step {
    pub round: usize,
    pub action: Action,
    pub node_idx: usize,
    pub tgt_idx: usize,
    pub ftype: FileType,
    pub name: String,
    pub file_pos: usize,
    pub data_pos: usize,
    pub data_len: usize,
}

impl Step {
    // byte length
    const BYTES_LEN: usize = 8 * 8 + 32;

    // file name to save the steps
    const STEPS_FILE: &'static str = "steps";

    fn new_random(round: usize, ctlgrp: &ControlGroup, data: &[u8]) -> Self {
        let ctlgrp_len = ctlgrp.0.len();
        let node_idx = crypto::random_usize(ctlgrp_len);
        let (data_pos, buf) = crypto::random_slice(&data);
        let file_pos = crypto::random_usize(ctlgrp.0[node_idx].data.len());
        Step {
            round,
            action: Action::new_random(),
            node_idx,
            tgt_idx: crypto::random_usize(ctlgrp_len),
            ftype: FileType::random(),
            name: format!("{}", crypto::random_usize(round)),
            file_pos,
            data_pos,
            data_len: buf.len(),
        }
    }

    // append step to file
    fn save(&self, path: &Path) {
        let mut buf = Vec::new();
        let path = path.join(Self::STEPS_FILE);
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(&path)
            .unwrap();
        buf.put_u64_le(self.round as u64);
        buf.put_u64_le(self.action.to_u64());
        buf.put_u64_le(self.node_idx as u64);
        buf.put_u64_le(self.tgt_idx as u64);
        buf.put_u64_le(self.ftype.to_u64());
        buf.put_u64_le(self.file_pos as u64);
        buf.put_u64_le(self.data_pos as u64);
        buf.put_u64_le(self.data_len as u64);
        let mut s = self.name.clone().into_bytes();
        s.resize(32, 0);
        buf.put(s);
        file.write_all(&buf).unwrap();
    }

    // load all steps
    fn load_all(path: &Path) -> Vec<Self> {
        let mut buf = Vec::new();
        let path = path.join(Self::STEPS_FILE);
        let mut file = fs::File::open(&path).unwrap();
        let read = file.read_to_end(&mut buf).unwrap();
        let mut ret = Vec::new();
        let rounds = read / Self::BYTES_LEN;

        let mut cur = Cursor::new(buf);
        for _ in 0..rounds {
            let round = cur.get_u64_le() as usize;
            let action = Action::from_u64(cur.get_u64_le());
            let node_idx = cur.get_u64_le() as usize;
            let tgt_idx = cur.get_u64_le() as usize;
            let ftype = FileType::from_u64(cur.get_u64_le());
            let file_pos = cur.get_u64_le() as usize;
            let data_pos = cur.get_u64_le() as usize;
            let data_len = cur.get_u64_le() as usize;
            let mut s = vec![0u8; 32];
            cur.copy_to_slice(&mut s);
            let p = s.iter().position(|c| *c == 0).unwrap();
            let name = String::from_utf8(s[..p].to_vec()).unwrap();
            let step = Step {
                round,
                action,
                node_idx,
                tgt_idx,
                ftype,
                name,
                file_pos,
                data_pos,
                data_len,
            };
            ret.push(step);
        }

        println!("Loaded {} steps", rounds);

        ret
    }

    // write data to file at random position
    pub fn write_to_file(&self, f: &mut File, data: &[u8]) -> Result<()> {
        let meta = f.metadata()?;
        let file_size = meta.len();
        assert!(self.file_pos <= file_size);
        f.seek(SeekFrom::Start(self.file_pos as u64))?;
        f.write_all(&data[..])?;
        f.finish()
    }
}

impl Debug for Step {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Step {{ round: {}, action: Action::{:?}, node_idx: {}, \
             tgt_idx: {}, ftype: FileType::{:?}, name: String::from({:?}), \
             file_pos: {}, data_pos: {}, data_len: {}}}",
            self.round,
            self.action,
            self.node_idx,
            self.tgt_idx,
            self.ftype,
            self.name,
            self.file_pos,
            self.data_pos,
            self.data_len,
        )
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

// repository handle
#[derive(Debug)]
pub struct RepoHandle {
    pub repo: Repo,
}

impl RepoHandle {
    fn new(repo: Repo) -> Self {
        RepoHandle { repo }
    }
}

// fuzz tester trait
pub trait Testable: Debug + Send + Sync {
    fn test_round(
        &self,
        fuzzer: &mut Fuzzer,
        step: &Step,
        ctlgrp: &mut ControlGroup,
    );
}

// fuzzer
#[derive(Debug)]
pub struct Fuzzer {
    pub batch: String,
    pub path: PathBuf,
    pub uri: String,
    pub repo_handle: RepoHandle,
    pub seed: crypto::RandomSeed,
    pub ctlr: Controller,
    pub data: Vec<u8>,
}

impl Fuzzer {
    // fuzz test base dir path
    const BASE: &'static str = "./fuzz_test/";

    // storage, seed and permutation file name
    const STORAGE: &'static str = "storage";
    const SEED: &'static str = "seed";
    const PERMU: &'static str = "permu";

    // repository password
    pub const PWD: &'static str = "pwd";

    // repository dir name
    const REPO: &'static str = "repo";

    const RND_DATA_LEN: usize = 2 * 1024 * 1024;
    const DATA_LEN: usize = 2 * Self::RND_DATA_LEN;

    pub fn new(storage_type: &str) -> Self {
        init_env();

        // create fuzz test dir
        let base = PathBuf::from(Self::BASE);
        let batch = format!(
            "{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );
        let path = base.join(&batch);
        fs::create_dir_all(&path).unwrap();

        // open repo
        println!("Create fuzz test dir at {:?}.", path);
        let uri = storage_type.to_string()
            + "://"
            + path.join(Self::REPO).to_str().unwrap();
        let repo = RepoOpener::new()
            .create(true)
            .open(&uri, Self::PWD)
            .unwrap();

        // create test environment
        let mut ret = Fuzzer {
            batch,
            path,
            uri,
            repo_handle: RepoHandle::new(repo),
            seed: crypto::RandomSeed::new(),
            ctlr: Controller::new(),
            data: vec![0; Self::DATA_LEN],
        };

        // initial test
        ret.init();

        ret
    }

    pub fn into_ref(self) -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(self))
    }

    // get storage type from URI
    fn storage_type(&self) -> String {
        let at = self.uri.find("://").unwrap();
        self.uri[..at].to_string()
    }

    // initialise fuzz test and save it
    fn init(&mut self) {
        let mut rnd_data = vec![0u8; Self::RND_DATA_LEN];
        crypto::random_buf_deterministic(&mut rnd_data, &self.seed);

        // fill data buffer with random data
        let mut permu: Permu = Vec::new();
        for _ in 0..5 {
            let pos = crypto::random_usize(Self::DATA_LEN);
            let rnd_pos = crypto::random_usize(Self::RND_DATA_LEN);
            let max_len =
                min(Self::DATA_LEN - pos, Self::RND_DATA_LEN - rnd_pos);
            let len = crypto::random_u32(max_len as u32) as usize;
            permu.push((Span { pos: rnd_pos, len }, pos));
            &mut self.data[pos..pos + len]
                .copy_from_slice(&rnd_data[rnd_pos..rnd_pos + len]);
        }

        // save storage file
        let path = self.path.join(Self::STORAGE);
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        file.write_all(self.storage_type().as_bytes()).unwrap();

        // save seed file
        let path = self.path.join(Self::SEED);
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        file.write_all(&self.seed.0).unwrap();

        // save permutation file
        let mut buf = Vec::new();
        let path = self.path.join(Self::PERMU);
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        for &(ref span, pos) in permu.iter() {
            buf.clear();
            buf.put_u64_le(span.pos as u64);
            buf.put_u64_le(span.len as u64);
            buf.put_u64_le(pos as u64);
            file.write_all(&buf).unwrap();
        }
    }

    // load fuzz test
    fn load(batch: &str) -> Self {
        init_env();

        let base = PathBuf::from(Self::BASE);
        let base = base.join(batch);

        // load storage file
        let mut buf = Vec::new();
        let path = base.join(Self::STORAGE);
        let mut file = fs::File::open(&path).unwrap();
        file.read_to_end(&mut buf).unwrap();
        let storage = String::from_utf8(buf).unwrap();

        // load seed file
        let mut buf = Vec::new();
        let path = base.join(Self::SEED);
        let mut file = fs::File::open(&path).unwrap();
        file.read_to_end(&mut buf).unwrap();
        let seed = crypto::RandomSeed::from(&buf[..32]);

        // load permutation file
        let mut buf = Vec::new();
        let path = base.join(Self::PERMU);
        let mut file = fs::File::open(&path).unwrap();
        file.read_to_end(&mut buf).unwrap();
        let mut permu: Permu = Vec::new();
        for chunk in buf.chunks(3 * 8) {
            // chunk is 3 * u64 integers
            let mut cur = Cursor::new(chunk);
            let pos = cur.get_u64_le() as usize;
            let len = cur.get_u64_le() as usize;
            let span = Span { pos, len };
            let pos = cur.get_u64_le() as usize;
            permu.push((span, pos));
        }

        // reproduce test data
        let mut rnd_data = vec![0u8; Self::RND_DATA_LEN];
        crypto::random_buf_deterministic(&mut rnd_data, &seed);
        let mut data = vec![0u8; Self::DATA_LEN];
        for opr in permu {
            let pos = opr.1;
            let rnd_pos = opr.0.pos;
            let len = opr.0.len;
            &mut data[pos..pos + len]
                .copy_from_slice(&rnd_data[rnd_pos..rnd_pos + len]);
        }

        // create and open repo
        let repo_path = base.join(Self::REPO);
        let uri = storage.clone() + "://" + repo_path.to_str().unwrap();
        if storage == "file" {
            fs::remove_dir_all(&repo_path).unwrap();
        }
        let repo = RepoOpener::new()
            .create(true)
            .open(&uri, Self::PWD)
            .unwrap();

        println!("Fuzz test loaded {:?}.", base);

        Fuzzer {
            batch: batch.to_string(),
            path: base,
            uri: uri.clone(),
            repo_handle: RepoHandle::new(repo),
            seed,
            ctlr: Controller::new(),
            data,
        }
    }

    // run the fuzz test
    pub fn run(
        fuzzer: Arc<RwLock<Fuzzer>>,
        tester: Arc<RwLock<Testable>>,
        rounds: usize,
        worker_cnt: usize,
    ) {
        // create control group
        let ctlgrp = Arc::new(RwLock::new(ControlGroup::new()));

        {
            let fuzzer = fuzzer.read().unwrap();

            println!(
                "Start fuzz test, batch {}, {} rounds, {} worker.",
                fuzzer.batch, rounds, worker_cnt
            );

            // reset random error controller and turn it on
            fuzzer.ctlr.reset(&fuzzer.seed);
            fuzzer.ctlr.turn_on();
        }

        // start fuzz rounds
        // ------------------
        let mut workers = Vec::new();
        for i in 0..worker_cnt {
            let fuzzer = fuzzer.clone();
            let tester = tester.clone();
            let ctlgrp = ctlgrp.clone();
            let name = format!("worker-{}", i);
            let builder = thread::Builder::new().name(name);

            workers.push(
                builder
                    .spawn(move || {
                        let curr = thread::current();
                        let worker = curr.name().unwrap();

                        println!("[{}]: Started.", worker);
                        for round in 0..rounds {
                            let mut fuzzer = fuzzer.write().unwrap();
                            let tester = tester.read().unwrap();
                            let mut ctlgrp = ctlgrp.write().unwrap();
                            let step =
                                Step::new_random(round, &ctlgrp, &fuzzer.data);
                            step.save(&fuzzer.path);
                            tester.test_round(&mut fuzzer, &step, &mut ctlgrp);
                            if round % 10 == 0 {
                                println!(
                                    "[{}]: {}/{}...",
                                    worker, round, rounds
                                );
                            }
                        }
                        println!("[{}]: Finished.", worker);
                    })
                    .unwrap(),
            );
        }
        for w in workers {
            w.join().unwrap();
        }

        // verify
        // ------------------
        {
            let mut fuzzer = fuzzer.write().unwrap();
            let ctlgrp = ctlgrp.read().unwrap();
            fuzzer.verify(&ctlgrp);
        }
    }

    // load fuzz test and re-run it
    pub fn rerun(batch: &str, tester: Box<Testable>) {
        // load fuzzer
        let mut fuzzer = Fuzzer::load(batch);

        // load test steps
        let steps = Step::load_all(&fuzzer.path);
        let rounds = steps.len();

        // create control group
        let mut ctlgrp = ControlGroup::new();

        let curr = thread::current();
        let worker = curr.name().unwrap();
        println!(
            "[{}]: Rerun fuzz test, batch {}, {} rounds.",
            worker, fuzzer.batch, rounds
        );

        // reset random error controller and turn it on
        fuzzer.ctlr.reset(&fuzzer.seed);
        fuzzer.ctlr.turn_on();

        // start fuzz rounds
        // ------------------
        for round in 0..rounds {
            let step = &steps[round];
            //if round == 12 { fuzzer.ctlr.turn_off(); }
            tester.test_round(&mut fuzzer, &step, &mut ctlgrp);
            //if round == 12 { break; }
            if round % 10 == 0 {
                println!("[{}]: {}/{}...", worker, round, rounds);
            }
        }
        println!("[{}]: Finished.", worker);

        // verify
        // ------------------
        fuzzer.verify(&ctlgrp);
    }

    // verify fuzz test result
    fn verify(&mut self, ctlgrp: &ControlGroup) {
        println!("Start verifying...");

        // turn off random error controller
        self.ctlr.turn_off();

        // sort control group nodes by its path for fast search
        let mut ctlgrp = ctlgrp.clone();
        ctlgrp.0.sort_by(|a, b| a.path.cmp(&b.path));

        // loop all nodes to do the comparison
        for (idx, node) in ctlgrp.0.iter().enumerate() {
            if node.is_file() {
                // if node is file, compare its content
                node.compare_file_content(&mut self.repo_handle.repo)
                    .unwrap();
                continue;
            }

            // otherwise, compare directory
            // firstly, find the node's immediate children
            let mut children: Vec<Node> = Vec::new();
            let mut pos = idx + 1;
            while let Some(nd) = ctlgrp.0.get(pos) {
                match nd.path.strip_prefix(&node.path) {
                    Ok(p) => {
                        if p.components().count() == 1
                            && children
                                .binary_search_by(|c| c.path.cmp(&nd.path))
                                .is_err()
                        {
                            children.push(nd.clone());
                        }
                        pos += 1;
                    }
                    Err(_) => break,
                }
            }

            // get node's immediate children from repo
            let mut subdirs =
                self.repo_handle.repo.read_dir(&node.path).unwrap();
            subdirs.sort_by(|a, b| a.path().cmp(b.path()));

            // then compare them
            assert_eq!(subdirs.len(), children.len());
            for (i, child) in children.iter().enumerate() {
                let subdir = &subdirs[i];
                assert_eq!(&child.path, subdir.path());
                assert_eq!(child.is_dir(), subdir.metadata().is_dir());
            }
        }

        println!("Completed.");
    }
}
