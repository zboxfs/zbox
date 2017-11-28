extern crate tempdir;
extern crate zbox;

mod common;

use std::io::{Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::fmt::{self, Debug};
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use zbox::{Error, OpenOptions, Repo, File};

const RND_DATA_LEN: usize = 2 * 1024 * 1024;
const DATA_LEN: usize = 2 * RND_DATA_LEN;

#[derive(Debug, Clone, Copy)]
enum FileType {
    File,
    Dir,
}

impl FileType {
    fn random() -> Self {
        match common::random_u32(2) {
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
}

#[derive(Debug)]
enum Action {
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
            20, // New,
            10, // Read,
            15, // Update,
            10, // Truncate,
            5,  // Delete,
            2,  // DeleteAll,
            10, // Rename,
            10, // Move,
            10, // Copy,
            10, // Reopen,
        ];
        let rnd = common::random_u32(weight.iter().sum());
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
}

#[derive(Clone)]
struct Node {
    path: PathBuf,
    ftype: FileType,
    data: Vec<u8>,
}

impl Node {
    fn new_file<P: AsRef<Path>>(path: P, data: &[u8]) -> Self {
        Node {
            path: path.as_ref().to_path_buf(),
            ftype: FileType::File,
            data: data.to_vec(),
        }
    }

    fn new_dir<P: AsRef<Path>>(path: P) -> Self {
        Node {
            path: path.as_ref().to_path_buf(),
            ftype: FileType::Dir,
            data: Vec::new(),
        }
    }

    fn is_dir(&self) -> bool {
        self.ftype.is_dir()
    }

    fn is_root(&self) -> bool {
        self.path.to_str().unwrap() == "/"
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

// round step
struct Step<'a> {
    round: usize,
    action: Action,
    node_idx: usize,
    tgt_idx: usize,
    ftype: FileType,
    name: String,
    file_pos: usize,
    data_pos: usize,
    data_len: usize,
    data: &'a [u8],
}

impl<'a> Step<'a> {
    fn new(round: usize, nodes: &Vec<Node>, test_data: &'a [u8]) -> Self {
        let node_idx = common::random_usize(nodes.len());
        let (data_pos, data) = common::random_slice(&test_data);
        Step {
            round,
            action: Action::new_random(),
            node_idx,
            tgt_idx: common::random_usize(nodes.len()),
            ftype: FileType::random(),
            name: format!("{}", common::random_usize(round)),
            file_pos: common::random_usize(nodes[node_idx].data.len()),
            data_pos,
            data_len: data.len(),
            data,
        }
    }
}

impl<'a> Debug for Step<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Step {{ round: {}, action: Action::{:?}, node_idx: {}, \
                tgt_idx: {}, ftype: FileType::{:?}, name: String::from({:?}), \
                file_pos: {}, data_pos: {}, data_len: {}, \
                data: &test_data[{}..{}] }},",
            self.round,
            self.action,
            self.node_idx,
            self.tgt_idx,
            self.ftype,
            self.name,
            self.file_pos,
            self.data_pos,
            self.data_len,
            self.data_pos,
            self.data_pos + self.data_len,
        )
    }
}

fn compare_file_content(repo: &mut Repo, node: &Node) {
    let mut f = repo.open_file(&node.path).unwrap();
    let meta = f.metadata();
    assert_eq!(meta.len(), node.data.len());

    let mut dst = Vec::new();
    f.read_to_end(&mut dst).unwrap();
    assert_eq!(&dst[..], &node.data[..]);
}

// write data to file at random position
fn write_data_to_file(f: &mut File, step: &Step) {
    let meta = f.metadata();
    let file_size = meta.len();
    assert!(step.file_pos <= file_size);
    f.seek(SeekFrom::Start(step.file_pos as u64)).unwrap();
    f.write_all(&step.data[..]).unwrap();
    f.finish().unwrap();
}

fn test_round(
    round: usize,
    env: &mut common::TestEnv,
    step: &Step,
    nodes: &mut Vec<Node>,
) {
    let _ = round;

    //println!("nodes: {:#?}", nodes);
    let node = nodes[step.node_idx].clone();
    //println!("node: {:?}", node);

    match step.action {
        Action::New => {
            let path = node.path.join(&step.name);

            if nodes.iter().find(|&p| &p.path == &path).is_some() {
                match step.ftype {
                    FileType::File => {
                        assert_eq!(
                            OpenOptions::new()
                                .create_new(true)
                                .open(&mut env.repo, &path)
                                .unwrap_err(),
                            Error::AlreadyExists
                        );
                    }
                    FileType::Dir => {
                        assert_eq!(
                            env.repo.create_dir(path).unwrap_err(),
                            Error::AlreadyExists
                        );
                    }
                }
                return;
            }

            match step.ftype {
                FileType::File => {
                    let result = OpenOptions::new().create(true).open(
                        &mut env.repo,
                        &path,
                    );
                    if !node.is_dir() {
                        assert_eq!(result.unwrap_err(), Error::NotDir);
                        return;
                    }

                    // write initial data to the new file
                    let mut f = result.unwrap();
                    write_data_to_file(&mut f, &step);

                    // add new control node
                    nodes.push(Node::new_file(&path, &step.data[..]));
                }
                FileType::Dir => {
                    let result = env.repo.create_dir(&path);
                    if node.is_dir() {
                        result.unwrap();
                        nodes.push(Node::new_dir(&path));
                    } else {
                        assert_eq!(result.unwrap_err(), Error::NotDir);
                    }
                }
            }
        }

        Action::Read => {
            if node.is_dir() {
                // read dir
                let children = env.repo.read_dir(&node.path).unwrap();
                let mut dirs: Vec<&PathBuf> = nodes
                    .iter()
                    .skip(1)    // skip root
                    .map(|n| &n.path)
                    .filter(|p| p.parent().unwrap() == &node.path)
                    .collect();
                dirs.sort();
                let mut cmp_grp: Vec<&Path> =
                    children.iter().map(|c| c.path()).collect();
                cmp_grp.sort();
                assert_eq!(dirs, cmp_grp);

            } else {
                // compare file content
                compare_file_content(&mut env.repo, &node);
            }
        }

        Action::Update => {
            let result = OpenOptions::new().write(true).open(
                &mut env.repo,
                &node.path,
            );
            if node.is_dir() {
                assert_eq!(result.unwrap_err(), Error::IsDir);
                return;
            }
            let mut f = result.unwrap();
            write_data_to_file(&mut f, &step);

            // update control group node
            let nd = nodes.iter_mut().find(|n| &n.path == &node.path).unwrap();
            let old_len = nd.data.len();
            let pos = step.file_pos;
            let new_len = pos + step.data_len;
            if new_len > old_len {
                nd.data[pos..].copy_from_slice(&step.data[..old_len - pos]);
                nd.data.extend_from_slice(&step.data[old_len - pos..]);
            } else {
                nd.data[pos..pos + step.data_len].copy_from_slice(
                    &step.data[..],
                );
            }
        }

        Action::Truncate => {
            let result = OpenOptions::new().write(true).open(
                &mut env.repo,
                &node.path,
            );
            if node.is_dir() {
                assert_eq!(result.unwrap_err(), Error::IsDir);
                return;
            }
            let mut f = result.unwrap();
            f.set_len(step.data_len).unwrap();

            // update control group node
            let nd = nodes.iter_mut().find(|n| &n.path == &node.path).unwrap();
            let old_len = nd.data.len();
            let new_len = step.data_len;
            if new_len > old_len {
                let extra = vec![0u8; new_len - old_len];
                nd.data.extend_from_slice(&extra[..]);
            } else {
                nd.data.truncate(new_len);
            }
        }

        Action::Delete => {
            if node.is_dir() {
                let result = env.repo.remove_dir(&node.path);
                if node.is_root() {
                    assert_eq!(result.unwrap_err(), Error::IsRoot);
                } else {
                    if nodes
                        .iter()
                        .skip(1)    // skip root
                        .any(|n| n.path.parent().unwrap() == &node.path)
                    {
                        assert_eq!(result.unwrap_err(), Error::NotEmpty);
                    } else {
                        result.unwrap();
                        nodes.retain(|n| &n.path != &node.path);
                    }
                }
            } else {
                env.repo.remove_file(&node.path).unwrap();
                nodes.retain(|n| &n.path != &node.path);
            }
        }

        Action::DeleteAll => {
            let result = env.repo.remove_dir_all(&node.path);
            if node.is_root() {
                result.unwrap();
                nodes.retain(|n| n.is_root());
            } else if node.is_dir() {
                result.unwrap();
                nodes.retain(|n| !n.path.starts_with(&node.path));
            } else {
                assert_eq!(result.unwrap_err(), Error::NotDir);
            }
        }

        Action::Rename => {
            if node.is_root() {
                assert_eq!(
                    env.repo.rename(&node.path, "/xxx").unwrap_err(),
                    Error::InvalidArgument
                );
                return;
            }

            let new_path = node.path.parent().unwrap().join(&step.name);
            let result = env.repo.rename(&node.path, &new_path);
            if new_path == node.path {
                assert_eq!(result.unwrap_err(), Error::InvalidArgument);
            } else if nodes.iter().any(|n| &n.path == &new_path) {
                assert_eq!(result.unwrap_err(), Error::AlreadyExists);
            } else {
                result.unwrap();
                for nd in nodes.iter_mut().filter(
                    |n| n.path.starts_with(&node.path),
                )
                {
                    let child =
                        nd.path.strip_prefix(&node.path).unwrap().to_path_buf();
                    nd.path = new_path.join(child);
                }
            }
        }

        Action::Move => {
            if node.is_root() {
                return;
            }

            let tgt = &nodes[step.tgt_idx].clone();
            if tgt.is_root() {
                let result = env.repo.rename(&node.path, &tgt.path);
                assert_eq!(result.unwrap_err(), Error::IsRoot);
                return;
            }

            let new_path = if tgt.is_dir() {
                tgt.path.join(&step.name)
            } else {
                tgt.path.clone()
            };
            let result = env.repo.rename(&node.path, &new_path);
            if new_path.starts_with(&node.path) {
                assert_eq!(result.unwrap_err(), Error::InvalidArgument);
            } else if nodes.iter().any(|n| &n.path == &new_path) {
                assert_eq!(result.unwrap_err(), Error::AlreadyExists);
            } else {
                result.unwrap();
                for nd in nodes.iter_mut().filter(
                    |n| n.path.starts_with(&node.path),
                )
                {
                    let child =
                        nd.path.strip_prefix(&node.path).unwrap().to_path_buf();
                    nd.path = new_path.join(child);
                }
            }
        }

        Action::Copy => {
            let tgt = &nodes[step.tgt_idx].clone();
            let result = env.repo.copy(&node.path, &tgt.path);

            if node.is_dir() || tgt.is_dir() {
                assert_eq!(result.unwrap_err(), Error::NotFile);
                return;
            }

            result.unwrap();

            if nodes.iter().any(|n| &n.path == &tgt.path) {
                // copy to existing node
                let nd =
                    nodes.iter_mut().find(|n| &n.path == &tgt.path).unwrap();
                nd.data = node.data.clone();
            } else {
                // copy to new node
                nodes.push(Node::new_file(&tgt.path, &node.data[..]));
            }
        }

        Action::Reopen => {
            env.reopen();
        }
    }
}

fn verify(env: &mut common::TestEnv, nodes: &mut Vec<Node>) {
    //println!("===verify===");
    //println!("{:#?}", nodes);
    nodes.sort_by(|a, b| a.path.cmp(&b.path));
    for (idx, node) in nodes.iter().enumerate() {
        if node.is_dir() {
            // find node's immediate children
            let mut children: Vec<Node> = Vec::new();
            let mut pos = idx + 1;
            while let Some(nd) = nodes.get(pos) {
                match nd.path.strip_prefix(&node.path) {
                    Ok(p) => {
                        if p.components().count() == 1 &&
                            children
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
            let mut subdirs = env.repo.read_dir(&node.path).unwrap();
            subdirs.sort_by(|a, b| a.path().cmp(b.path()));

            // then compare them
            assert_eq!(subdirs.len(), children.len());
            for (i, child) in children.iter().enumerate() {
                let subdir = &subdirs[i];
                assert_eq!(&child.path, subdir.path());
                assert_eq!(child.is_dir(), subdir.metadata().is_dir());
            }

        } else {
            // if node is file, compare its content
            compare_file_content(&mut env.repo, &node);
        }
    }
}

#[test]
//#[ignore]
//#[cfg_attr(rustfmt, rustfmt_skip)]
fn fuzz_fs() {
    let mut env = common::setup();
    let (seed, permu, test_data) =
        common::make_test_data(RND_DATA_LEN, DATA_LEN);
    let mut nodes: Vec<Node> = vec![Node::new_dir("/")]; // control group
    let rounds = 300;
    let steps = vec![0; rounds];

    //println!("seed: {:?}", seed);
    //println!("permu: {:?}", permu);
    let _ = seed;
    let _ = permu;

    // uncomment below to reproduce the bug found during fuzzing
    /*use common::Span;
    let seed = common::RandomSeed();
    let permu = vec![];
    let test_data = common::reprod_test_data(seed, permu, RND_DATA_LEN, DATA_LEN);
    let steps = [];*/

    // start fuzz rounds
    // ------------------
    for round in 0..steps.len() {
        let step = Step::new(round, &nodes, &test_data);
        //let step = &steps[round];
        //println!("{:?}", step);

        test_round(round, &mut env, &step, &mut nodes);
    }

    // verify
    // ------------------
    verify(&mut env, &mut nodes);
}

#[test]
//#[ignore]
fn fuzz_fs_mt() {
    let env_ref = Arc::new(RwLock::new(common::setup()));
    let (seed, permu, test_data) =
        common::make_test_data(RND_DATA_LEN, DATA_LEN);
    let test_data_ref = Arc::new(test_data);
    // control group
    let nodes = Arc::new(RwLock::new(vec![Node::new_dir("/")]));
    let worker_cnt = 4;
    let rounds = 30;
    let round_idx_ref = Arc::new(AtomicUsize::new(0));

    //println!("seed: {:?}", seed);
    //println!("permu: {:?}", permu);
    let _ = seed;
    let _ = permu;

    // uncomment below to reproduce the bug found during fuzzing
    /*use common::Span;
    let seed = common::RandomSeed();
    let permu = vec![];
    let test_data = common::reprod_test_data(seed, permu, RND_DATA_LEN, DATA_LEN);
    let steps = [];*/

    // start fuzz rounds
    // ------------------
    let mut workers = Vec::new();
    for _ in 0..worker_cnt {
        let env = env_ref.clone();
        let nodes = nodes.clone();
        let test_data = test_data_ref.clone();
        let round_idx = round_idx_ref.clone();

        workers.push(thread::spawn(move || for _ in 0..rounds {
            let mut env = env.write().unwrap();
            let mut nodes = nodes.write().unwrap();
            let round = round_idx.fetch_add(1, Ordering::SeqCst);
            let step = Step::new(round, &nodes, &test_data);
            //println!("{:?}", step);

            test_round(round, &mut env, &step, &mut nodes);
        }));
    }
    for w in workers {
        w.join().unwrap();
    }

    // verify
    // ------------------
    {
        let mut env = env_ref.write().unwrap();
        let mut nodes = nodes.write().unwrap();
        verify(&mut env, &mut nodes);
    }
}
