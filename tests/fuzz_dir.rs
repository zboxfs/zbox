#![cfg(feature = "fuzz-test")]

extern crate bytes;
extern crate zbox;

mod common;

use std::path::{Path, PathBuf};
use std::thread;
use std::sync::{Arc, RwLock};
use std::fs;
use std::io::{Cursor, Read, Write};

use bytes::{Buf, BufMut};

use common::fuzz;
use zbox::Repo;

#[derive(Debug, Clone)]
enum Action {
    New,
    Read,
    Delete,
    DeleteAll,
    Rename,
}

impl Action {
    // pick a random action
    fn new_random() -> Self {
        match fuzz::random_u32(5) {
            0 => Action::New,
            1 => Action::Read,
            2 => Action::Delete,
            3 => Action::DeleteAll,
            4 => Action::Rename,
            _ => unreachable!(),
        }
    }

    fn to_u8(&self) -> u8 {
        match *self {
            Action::New => 0,
            Action::Read => 1,
            Action::Delete => 2,
            Action::DeleteAll => 3,
            Action::Rename => 4,
        }
    }

    fn from_u8(val: u8) -> Self {
        match val {
            0 => Action::New,
            1 => Action::Read,
            2 => Action::Delete,
            3 => Action::DeleteAll,
            4 => Action::Rename,
            _ => unreachable!(),
        }
    }

    // append single action
    fn save(&self, env: &fuzz::TestEnv) {
        let mut buf = Vec::new();
        let path = env.path.join("actions");
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(&path)
            .unwrap();
        buf.put_u8(self.to_u8());
        file.write_all(&buf).unwrap();
    }

    // load all actions
    fn load_all(env: &fuzz::TestEnv) -> Vec<Self> {
        let mut buf = Vec::new();
        let path = env.path.join("actions");
        let mut file = fs::File::open(&path).unwrap();
        let read = file.read_to_end(&mut buf).unwrap();
        let mut ret = Vec::new();
        let round = read;

        let mut cur = Cursor::new(buf);
        for _ in 0..round {
            let val = cur.get_u8();
            ret.push(Action::from_u8(val));
        }

        println!("Loaded {} actions.", round);

        ret
    }
}

fn test_round(
    repo: &mut Repo,
    action: &Action,
    round: usize,
    rounds: usize,
    ctl_grp: &mut Vec<PathBuf>,
) {
    let curr = thread::current();
    let worker = curr.name().unwrap();
    let root = PathBuf::from("/");

    // randomly pick up a dir node from control group
    let node = ctl_grp[fuzz::random_usize(ctl_grp.len())].clone();

    match *action {
        Action::New => {
            let name = format!("{}", fuzz::random_usize(round));
            let path = node.join(name);
            if ctl_grp.iter().find(|&p| p == &path).is_none() {
                ctl_grp.push(path.clone());
                repo.create_dir(path).unwrap();
            } else {
                assert!(repo.create_dir(path).is_err());
            }
        }
        Action::Read => {
            let children = repo.read_dir(&node).unwrap();
            let mut dirs: Vec<&PathBuf> = ctl_grp
                .iter()
                .skip(1)    // skip root
                .filter(|p| p.parent().unwrap() == &node)
                .collect();
            dirs.sort();
            let mut cmp_grp: Vec<&Path> =
                children.iter().map(|c| c.path()).collect();
            cmp_grp.sort();
            assert_eq!(dirs, cmp_grp);
        }
        Action::Delete => {
            if ctl_grp
                .iter()
                .skip(1)    // skip root
                .filter(|p| p.parent().unwrap() == &node)
                .count() == 0
            {
                if node == root {
                    assert!(repo.remove_dir(&node).is_err());
                } else {
                    repo.remove_dir(&node).unwrap();
                    ctl_grp.retain(|p| p != &node);
                }
            } else {
                assert!(repo.remove_dir(&node).is_err());
            }
        }
        Action::DeleteAll => {
            repo.remove_dir_all(&node).unwrap();
            if node == PathBuf::from("/") {
                ctl_grp.retain(|p| p == &root);
            } else {
                ctl_grp.retain(|p| !p.starts_with(&node));
            }
        }
        Action::Rename => {
            let name = format!("{}", fuzz::random_usize(round));
            let tgt_parent = ctl_grp[fuzz::random_usize(ctl_grp.len())].clone();
            let path = tgt_parent.join(name);
            if node == path {
                repo.rename(&node, &path).unwrap();
            } else if node == root || path.starts_with(&node)
                || ctl_grp.iter().filter(|p| p.starts_with(&path)).count() > 1
            {
                assert!(repo.rename(&node, &path).is_err());
            } else {
                repo.rename(&node, &path).unwrap();
                for p in ctl_grp.iter_mut().filter(|p| p.starts_with(&node)) {
                    let mut new_path =
                        path.join(p.strip_prefix(&node).unwrap());
                    new_path.push("dummy");
                    new_path.pop();
                    *p = new_path;
                }
            }
        }
    }

    if round % 100 == 0 {
        println!("{}: {}/{}...", worker, round, rounds);
    }
    if round == rounds - 1 {
        println!("{}: Finished", worker);
    }
}

fn dir_fuzz_st(rounds: usize) {
    let mut env = fuzz::TestEnv::new("dir");
    let mut ctl_grp: Vec<PathBuf> = vec![PathBuf::from("/")];

    // start fuzz rounds
    // ------------------
    for round in 0..rounds {
        let action = Action::new_random();
        action.save(&env);
        test_round(&mut env.repo, &action, round, rounds, &mut ctl_grp);
    }
}

#[allow(dead_code)]
fn dir_fuzz_reproduce(batch_id: &str) {
    let mut env = fuzz::TestEnv::load(batch_id);
    let mut ctl_grp: Vec<PathBuf> = vec![PathBuf::from("/")];
    let actions = Action::load_all(&env);
    let rounds = actions.len();

    // start fuzz rounds
    // ------------------
    for round in 0..rounds {
        let action = &actions[round];
        test_round(&mut env.repo, action, round, rounds, &mut ctl_grp);
    }
}

fn dir_fuzz_mt(rounds: usize) {
    let env = fuzz::TestEnv::new("dir_mt").into_ref();
    let ctl_grp = Arc::new(RwLock::new(vec![PathBuf::from("/")]));
    let worker_cnt = 4;

    let mut workers = Vec::new();
    for i in 0..worker_cnt {
        let env = env.clone();
        let ctl_grp = ctl_grp.clone();
        let name = format!("worker-{}", i);
        let builder = thread::Builder::new().name(name);
        workers.push(
            builder
                .spawn(move || {
                    for round in 0..rounds {
                        let mut env = env.write().unwrap();
                        let mut ctl_grp = ctl_grp.write().unwrap();
                        let action = Action::new_random();
                        test_round(
                            &mut env.repo,
                            &action,
                            round,
                            rounds,
                            &mut ctl_grp,
                        );
                    }
                })
                .unwrap(),
        );
    }
    for w in workers {
        w.join().unwrap();
    }
}

#[test]
fn fuzz_dir() {
    dir_fuzz_st(200);
    //dir_fuzz_reproduce("dir_1524198910");
    dir_fuzz_mt(200);
}
