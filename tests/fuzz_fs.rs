#![cfg(feature = "fuzz-test")]

extern crate bytes;
extern crate zbox;

mod common;

use std::fmt::{self, Debug};
use std::fs;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::thread;

use bytes::{Buf, BufMut, LittleEndian};

use common::fuzz;
use zbox::{Error, File, OpenOptions, Repo};

#[derive(Debug, Clone, Copy)]
enum FileType {
    File,
    Dir,
}

impl FileType {
    fn random() -> Self {
        match fuzz::random_u32(2) {
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
        let rnd = fuzz::random_u32(weight.iter().sum());
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

    fn is_file(&self) -> bool {
        !self.is_dir()
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
struct Step {
    round: usize,
    action: Action,
    node_idx: usize,
    tgt_idx: usize,
    ftype: FileType,
    name: String,
    file_pos: usize,
    data_pos: usize,
    data_len: usize,
}

impl Step {
    const BYTES_LEN: usize = 8 * 8 + 32;

    fn new_random(round: usize, nodes: &Vec<Node>, data: &[u8]) -> Self {
        let node_idx = fuzz::random_usize(nodes.len());
        let (data_pos, buf) = fuzz::random_slice(&data);
        let file_pos = fuzz::random_usize(nodes[node_idx].data.len());
        Step {
            round,
            action: Action::new_random(),
            node_idx,
            tgt_idx: fuzz::random_usize(nodes.len()),
            ftype: FileType::random(),
            name: format!("{}", fuzz::random_usize(round)),
            file_pos,
            data_pos,
            data_len: buf.len(),
        }
    }

    // append single step
    fn save(&self, env: &fuzz::TestEnv) {
        let mut buf = Vec::new();
        let path = env.path.join("steps");
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(&path)
            .unwrap();
        buf.put_u64::<LittleEndian>(self.round as u64);
        buf.put_u64::<LittleEndian>(self.action.to_u64());
        buf.put_u64::<LittleEndian>(self.node_idx as u64);
        buf.put_u64::<LittleEndian>(self.tgt_idx as u64);
        buf.put_u64::<LittleEndian>(self.ftype.to_u64());
        buf.put_u64::<LittleEndian>(self.file_pos as u64);
        buf.put_u64::<LittleEndian>(self.data_pos as u64);
        buf.put_u64::<LittleEndian>(self.data_len as u64);
        let mut s = self.name.clone().into_bytes();
        s.resize(32, 0);
        buf.put(s);
        file.write_all(&buf).unwrap();
    }

    // load all steps
    fn load_all(env: &fuzz::TestEnv) -> Vec<Self> {
        let mut buf = Vec::new();
        let path = env.path.join("steps");
        let mut file = fs::File::open(&path).unwrap();
        let read = file.read_to_end(&mut buf).unwrap();
        let mut ret = Vec::new();
        let round = read / Self::BYTES_LEN;

        let mut cur = Cursor::new(buf);
        for _ in 0..round {
            let round = cur.get_u64::<LittleEndian>() as usize;
            let action = Action::from_u64(cur.get_u64::<LittleEndian>());
            let node_idx = cur.get_u64::<LittleEndian>() as usize;
            let tgt_idx = cur.get_u64::<LittleEndian>() as usize;
            let ftype = FileType::from_u64(cur.get_u64::<LittleEndian>());
            let file_pos = cur.get_u64::<LittleEndian>() as usize;
            let data_pos = cur.get_u64::<LittleEndian>() as usize;
            let data_len = cur.get_u64::<LittleEndian>() as usize;
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

        println!("Loaded {} steps", round);

        ret
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

fn compare_file_content(repo: &mut Repo, node: &Node) {
    let mut f = repo.open_file(&node.path).unwrap();
    let meta = f.metadata().unwrap();
    assert_eq!(meta.len(), node.data.len());

    let mut dst = Vec::new();
    f.read_to_end(&mut dst).unwrap();
    assert_eq!(&dst[..], &node.data[..]);
}

// write data to file at random position
fn write_data_to_file(f: &mut File, step: &Step, data: &[u8]) {
    let meta = f.metadata().unwrap();
    let file_size = meta.len();
    assert!(step.file_pos <= file_size);
    f.seek(SeekFrom::Start(step.file_pos as u64)).unwrap();
    f.write_all(&data[..]).unwrap();
    f.finish().unwrap();
}

fn handle_rename(
    new_path: &Path,
    node: &Node,
    nodes: &mut Vec<Node>,
    repo: &mut Repo,
) {
    let mut new_path_exists = false;
    let mut new_path_is_dir = false;
    if let Some(nd) = nodes.iter().find(|n| &n.path == &new_path) {
        new_path_exists = true;
        new_path_is_dir = nd.is_dir();
    }
    let new_path_has_child = nodes
        .iter()
        .filter(|n| n.path.starts_with(&new_path))
        .count() > 1;
    let result = repo.rename(&node.path, &new_path);

    if new_path == node.path {
        result.unwrap();
        return;
    }
    if new_path.starts_with(&node.path) {
        assert_eq!(result.unwrap_err(), Error::InvalidArgument);
        return;
    }
    if new_path_exists {
        if node.is_file() && new_path_is_dir {
            assert_eq!(result.unwrap_err(), Error::IsDir);
            return;
        } else if node.is_dir() && !new_path_is_dir {
            assert_eq!(result.unwrap_err(), Error::NotDir);
            return;
        } else if node.is_dir() && new_path_has_child {
            assert_eq!(result.unwrap_err(), Error::NotEmpty);
            return;
        }
    }

    result.unwrap();

    if new_path_exists {
        nodes.retain(|n| &n.path != &new_path);
    }

    for nd in nodes.iter_mut().filter(|n| n.path.starts_with(&node.path)) {
        let child = nd.path.strip_prefix(&node.path).unwrap().to_path_buf();
        nd.path = new_path.join(child);
    }
}

fn test_round(env: &mut fuzz::TestEnv, step: &Step, nodes: &mut Vec<Node>) {
    //println!("=== step: {:?}", step);

    let node = nodes[step.node_idx].clone();

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
                    let result = OpenOptions::new()
                        .create(true)
                        .open(&mut env.repo, &path);
                    if !node.is_dir() {
                        assert_eq!(result.unwrap_err(), Error::NotDir);
                        return;
                    }

                    let data =
                        &env.data[step.data_pos..step.data_pos + step.data_len];

                    // write initial data to the new file
                    let mut f = result.unwrap();
                    write_data_to_file(&mut f, &step, data);

                    // add new control node
                    nodes.push(Node::new_file(&path, &data[..]));
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
            let result = OpenOptions::new()
                .write(true)
                .open(&mut env.repo, &node.path);
            if node.is_dir() {
                assert_eq!(result.unwrap_err(), Error::IsDir);
                return;
            }
            let data = &env.data[step.data_pos..step.data_pos + step.data_len];
            let mut f = result.unwrap();
            write_data_to_file(&mut f, &step, data);

            // update control group node
            let nd = nodes.iter_mut().find(|n| &n.path == &node.path).unwrap();
            let old_len = nd.data.len();
            let pos = step.file_pos;
            let new_len = pos + step.data_len;
            if new_len > old_len {
                nd.data[pos..].copy_from_slice(&data[..old_len - pos]);
                nd.data.extend_from_slice(&data[old_len - pos..]);
            } else {
                nd.data[pos..pos + step.data_len].copy_from_slice(&data[..]);
            }
        }

        Action::Truncate => {
            let result = OpenOptions::new()
                .write(true)
                .open(&mut env.repo, &node.path);
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
            handle_rename(&new_path, &node, nodes, &mut env.repo);
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
            handle_rename(&new_path, &node, nodes, &mut env.repo);
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

fn verify(env: &mut fuzz::TestEnv, nodes: &mut Vec<Node>) {
    println!("Start verifying...");
    nodes.sort_by(|a, b| a.path.cmp(&b.path));
    for (idx, node) in nodes.iter().enumerate() {
        if node.is_file() {
            // if node is file, compare its content
            compare_file_content(&mut env.repo, &node);
            continue;
        }

        // find node's immediate children
        let mut children: Vec<Node> = Vec::new();
        let mut pos = idx + 1;
        while let Some(nd) = nodes.get(pos) {
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
        let mut subdirs = env.repo.read_dir(&node.path).unwrap();
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

fn fuzz_fs_st(rounds: usize) {
    let mut env = fuzz::TestEnv::new("fs");
    let mut nodes: Vec<Node> = vec![Node::new_dir("/")]; // control group

    let curr = thread::current();
    let worker = curr.name().unwrap();
    println!("{}: Start {} fs fuzz test rounds...", worker, rounds);

    // start fuzz rounds
    // ------------------
    for round in 0..rounds {
        let step = Step::new_random(round, &nodes, &env.data);
        step.save(&env);
        test_round(&mut env, &step, &mut nodes);
        if round % 10 == 0 {
            println!("{}: {}/{}...", worker, round, rounds);
        }
    }
    println!("{}: Finished.", worker);

    // verify
    // ------------------
    verify(&mut env, &mut nodes);
}

#[allow(dead_code)]
fn fuzz_fs_reproduce(batch_id: &str) {
    let mut env = fuzz::TestEnv::load(batch_id);
    let mut nodes: Vec<Node> = vec![Node::new_dir("/")]; // control group
    let steps = Step::load_all(&env);
    let rounds = steps.len();

    let curr = thread::current();
    let worker = curr.name().unwrap();
    println!("{}: Start {} fs fuzz test rounds...", worker, rounds);

    // start fuzz rounds
    // ------------------
    for round in 0..rounds {
        let step = &steps[round];
        test_round(&mut env, &step, &mut nodes);
        if round % 10 == 0 {
            println!("{}: {}/{}...", worker, round, rounds);
        }
    }
    println!("{}: Finished.", worker);

    // verify
    // ------------------
    verify(&mut env, &mut nodes);
}

fn fuzz_fs_mt(rounds: usize) {
    let env = fuzz::TestEnv::new("fs_mt").into_ref();
    // control group
    let nodes = Arc::new(RwLock::new(vec![Node::new_dir("/")]));
    let worker_cnt = 4;

    // start fuzz rounds
    // ------------------
    let mut workers = Vec::new();
    for i in 0..worker_cnt {
        let env = env.clone();
        let nodes = nodes.clone();
        let name = format!("worker-{}", i);
        let builder = thread::Builder::new().name(name);

        workers.push(
            builder
                .spawn(move || {
                    let curr = thread::current();
                    let worker = curr.name().unwrap();

                    for round in 0..rounds {
                        let mut env = env.write().unwrap();
                        let mut nodes = nodes.write().unwrap();
                        let step = Step::new_random(round, &nodes, &env.data);

                        test_round(&mut env, &step, &mut nodes);

                        if round % 10 == 0 {
                            println!("{}: {}/{}...", worker, round, rounds);
                        }
                    }
                    println!("{}: Finished.", worker);
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
        let mut env = env.write().unwrap();
        let mut nodes = nodes.write().unwrap();
        verify(&mut env, &mut nodes);
    }
}

#[test]
fn fuzz_fs() {
    fuzz_fs_st(30);
    //fuzz_fs_reproduce("fs_1534713939");
    fuzz_fs_mt(30);
}
