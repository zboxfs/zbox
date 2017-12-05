extern crate zbox;

mod common;

use std::path::{Path, PathBuf};
use std::thread;
use std::sync::{Arc, RwLock};
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
        match common::random_u32(5) {
            0 => Action::New,
            1 => Action::Read,
            2 => Action::Delete,
            3 => Action::DeleteAll,
            4 => Action::Rename,
            _ => unreachable!(),
        }
    }
}

fn test_round(repo: &mut Repo, round: usize, ctl_grp: &mut Vec<PathBuf>) {
    let root = PathBuf::from("/");
    let action = Action::new_random();
    let node = ctl_grp[common::random_usize(ctl_grp.len())].clone();
    /*println!(
        "node: {:?}, action: {:?}, ctl_grp: {:?}",
        node.display(),
        action,
        ctl_grp
    );*/

    match action {
        Action::New => {
            let name = format!("{}", common::random_usize(round));
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
            let name = format!("{}", common::random_usize(round));
            let tgt_parent = ctl_grp[common::random_usize(ctl_grp.len())]
                .clone();
            let path = tgt_parent.join(name);
            //println!("rename to path: {}", path.display());
            if node == root || path.starts_with(&node) ||
                ctl_grp.iter().filter(|p| *p == &path).count() > 0
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
}

fn dir_fuzz() {
    let mut env = common::setup();
    let repo = &mut env.repo;
    let mut ctl_grp: Vec<PathBuf> = vec![PathBuf::from("/")];
    let rounds = 30;

    for round in 0..rounds {
        test_round(repo, round, &mut ctl_grp);
    }
}

fn dir_fuzz_mt() {
    let env = Arc::new(RwLock::new(common::setup()));
    let root = PathBuf::from("/");
    let ctl_grp = Arc::new(RwLock::new(vec![root.clone()]));
    let worker_cnt = 4;
    let rounds = 30;

    let mut workers = Vec::new();
    for _ in 0..worker_cnt {
        let env = env.clone();
        let ctl_grp = ctl_grp.clone();
        workers.push(thread::spawn(move || {
            let mut env = env.write().unwrap();
            let repo = &mut env.repo;
            let mut ctl_grp = ctl_grp.write().unwrap();
            for round in 0..rounds {
                test_round(repo, round, &mut ctl_grp);
            }
        }));
    }
    for w in workers {
        w.join().unwrap();
    }
}

fn main() {
    dir_fuzz();
    dir_fuzz_mt();
}
