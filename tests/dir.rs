extern crate tempdir;
extern crate zbox;

mod common;

use std::path::{Path, PathBuf};
use std::{thread, time};
use std::sync::{Arc, RwLock};
use zbox::Repo;

#[test]
fn dir_create() {
    let mut env = common::setup();
    let repo = &mut env.repo;

    // #1: basic test
    repo.create_dir("/dir").unwrap();
    assert!(repo.create_dir("/dir").is_err());
    assert!(repo.create_dir("/xxx/yyy").is_err());
    repo.create_dir("/dir2").unwrap();
    repo.create_dir("/dir3").unwrap();
    assert!(repo.is_dir("/dir"));
    assert!(repo.is_dir("/dir2"));
    assert!(repo.is_dir("/dir3"));

    // #2: test create_dir_all
    repo.create_dir_all("/xxx/yyy").unwrap();
    repo.create_dir_all("/xxx/111/222").unwrap();

    // #3: check dir modify time
    let m = repo.metadata("/xxx/111/222").unwrap();
    thread::sleep(time::Duration::from_millis(1500));
    repo.create_dir_all("/xxx/111/222/333").unwrap();
    let m2 = repo.metadata("/xxx/111/222").unwrap();
    assert!(m2.modified() > m.modified());
}

#[test]
fn dir_create_mt() {
    let env = Arc::new(RwLock::new(common::setup()));
    let worker_cnt = 4;
    let task_cnt = 8;

    let mut workers = Vec::new();
    for i in 0..worker_cnt {
        let env = env.clone();
        workers.push(thread::spawn(move || {
            let base = i * task_cnt;
            for j in base..base + task_cnt {
                let path = format!("/mt/{}", j);
                let mut env = env.write().unwrap();
                env.repo.create_dir_all(&path).unwrap();
            }
        }));
    }
    for w in workers {
        w.join().unwrap();
    }

    // verify
    let env = env.read().unwrap();
    let dirs = env.repo.read_dir("/mt").unwrap();
    assert_eq!(dirs.len(), worker_cnt * task_cnt);
}

#[test]
fn dir_read() {
    let mut env = common::setup();
    let repo = &mut env.repo;

    repo.create_dir_all("/aaa/aaa1/aaa11").unwrap();
    repo.create_dir_all("/aaa/aaa1/aaa12").unwrap();
    repo.create_dir_all("/aaa/aaa2/").unwrap();
    repo.create_dir("/aaa/aaa2/xxx").unwrap();
    repo.create_dir_all("/bbb/bbb1").unwrap();
    repo.create_dir("/bbb/xxx").unwrap();
    repo.create_dir_all("/ccc").unwrap();

    let dirs = repo.read_dir("/").unwrap();
    assert_eq!(dirs.len(), 3);
    let dirs = repo.read_dir("/aaa").unwrap();
    assert_eq!(dirs.len(), 2);
    let dirs = repo.read_dir("/bbb").unwrap();
    assert_eq!(dirs.len(), 2);
    let dirs = repo.read_dir("/ccc").unwrap();
    assert_eq!(dirs.len(), 0);
}

#[test]
fn dir_remove() {
    let mut env = common::setup();
    let repo = &mut env.repo;

    repo.create_dir_all("/aaa/bbb/ccc").unwrap();
    repo.create_dir_all("/aaa/bbb/ddd").unwrap();
    assert!(repo.remove_dir("/aaa").is_err());
    assert!(repo.remove_dir("/aaa/bbb").is_err());
    repo.remove_dir("/aaa/bbb/ccc").unwrap();
    assert!(repo.remove_dir("/not_exist").is_err());
    repo.remove_dir_all("/aaa").unwrap();
    assert!(repo.remove_dir("/aaa").is_err());
    assert!(repo.remove_dir("/").is_err());
}

#[test]
fn dir_rename() {
    let mut env = common::setup();
    let repo = &mut env.repo;

    assert!(repo.rename("/", "/xxx").is_err());
    assert!(repo.rename("/not_exist", "/xxx").is_err());

    repo.create_dir_all("/aaa/bbb/ccc").unwrap();
    repo.rename("/aaa/bbb/ccc", "/aaa/ddd").unwrap();
    let dirs = repo.read_dir("/aaa/ddd").unwrap();
    assert_eq!(dirs.len(), 0);
    let dirs = repo.read_dir("/aaa").unwrap();
    assert_eq!(dirs.len(), 2);

    repo.create_dir_all("/3/8").unwrap();
    repo.rename("/3/8", "/3/14").unwrap();
    let dirs = repo.read_dir("/3").unwrap();
    assert_eq!(dirs.len(), 1);
    assert_eq!(dirs[0].path().to_str().unwrap(), "/3/14");

    repo.create_dir("/15").unwrap();
    repo.create_dir("/10").unwrap();
    repo.rename("/10", "/15/21").unwrap();
    let dirs = repo.read_dir("/15").unwrap();
    assert_eq!(dirs.len(), 1);
    assert_eq!(dirs[0].path().to_str().unwrap(), "/15/21");
    repo.remove_dir("/15/21").unwrap();

    repo.create_dir_all("/0/3").unwrap();
    repo.create_dir_all("/0/4").unwrap();
    repo.create_dir("/17").unwrap();
    assert!(repo.rename("/17", "/0/4").is_err());
}

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

#[test]
fn dir_fuzz() {
    let mut env = common::setup();
    let repo = &mut env.repo;
    let mut ctl_grp: Vec<PathBuf> = vec![PathBuf::from("/")];
    let rounds = 30;

    for round in 0..rounds {
        test_round(repo, round, &mut ctl_grp);
    }
}

#[test]
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
