extern crate zbox;

mod common;

use std::error::Error as StdError;
use std::path::Path;

use common::controller::imp::Controller;
use common::fuzzer::{
    Action, ControlGroup, FileType, Fuzzer, Node, RepoHandle, Step, Testable,
};
use zbox::{Error, OpenOptions, Repo, RepoOpener, Result};

// check if the error is caused by the faulty storage
macro_rules! is_faulty_err {
    ($x:expr) => {
        if cfg!(feature = "storage-faulty") {
            match $x {
                Err(ref err) if err.description() == "Faulty error" => true,
                _ => false,
            }
        } else {
            false
        }
    };
}

// return if the error is caused by the faulty storage, otherwise return the
// expression result
macro_rules! skip_faulty {
    ($x:expr) => {{
        let result = $x;
        if cfg!(feature = "storage-faulty") {
            if let Err(ref err) = result {
                if err.description() == "Faulty error" {
                    return;
                }
            }
        }
        result
    }};
}

fn handle_rename(
    new_path: &Path,
    node: &Node,
    ctlgrp: &mut ControlGroup,
    repo: &mut Repo,
) -> Result<()> {
    let mut new_path_exists = false;
    let mut new_path_is_dir = false;
    if let Some(nd) = ctlgrp.find_node(&new_path) {
        new_path_exists = true;
        new_path_is_dir = nd.is_dir();
    }
    let new_path_has_child = ctlgrp
        .0
        .iter()
        .filter(|n| n.path.starts_with(&new_path))
        .count() > 1;

    let result = repo.rename(&node.path, &new_path);
    if is_faulty_err!(result) {
        return result;
    }

    if new_path == node.path {
        result.unwrap();
        return Ok(());
    }
    if new_path.starts_with(&node.path) {
        assert_eq!(result.unwrap_err(), Error::InvalidArgument);
        return Ok(());
    }
    if new_path_exists {
        if node.is_file() && new_path_is_dir {
            assert_eq!(result.unwrap_err(), Error::IsDir);
            return Ok(());
        } else if node.is_dir() && !new_path_is_dir {
            assert_eq!(result.unwrap_err(), Error::NotDir);
            return Ok(());
        } else if node.is_dir() && new_path_has_child {
            assert_eq!(result.unwrap_err(), Error::NotEmpty);
            return Ok(());
        }
    }

    result.unwrap();

    if new_path_exists {
        ctlgrp.del(&new_path);
    }

    for nd in ctlgrp
        .0
        .iter_mut()
        .filter(|n| n.path.starts_with(&node.path))
    {
        let child = nd.path.strip_prefix(&node.path).unwrap().to_path_buf();
        nd.path = new_path.join(child);
    }

    Ok(())
}

// fuzz tester
#[derive(Debug)]
struct Tester {}

impl Testable for Tester {
    fn test_round(
        &self,
        repo_handle: &mut RepoHandle,
        step: &Step,
        ctlgrp: &mut ControlGroup,
        data: &[u8],
        ctlr: &Controller,
    ) {
        let node = ctlgrp.0[step.node_idx].clone();
        //println!("=== node: {:?}, step: {:?}", node, step);

        match step.action {
            Action::New => {
                // path for the new object
                let path = node.path.join(&step.name);

                match step.ftype {
                    FileType::File => {
                        let result = OpenOptions::new()
                            .create_new(true)
                            .open(&mut repo_handle.repo, &path);

                        if is_faulty_err!(result) {
                            // because the open() is not atomic, we have to
                            // check the file if is created in repo by
                            // turnining off random error temporarily
                            ctlr.turn_off();
                            if repo_handle.repo.path_exists(&path) {
                                // if the file is created, do the same to
                                // control group
                                ctlgrp.add_file(&path, &data[..0]);
                            }
                            ctlr.turn_on();
                            return;
                        }

                        // if the file already exists, the action should fail
                        if ctlgrp.has_node(&path) {
                            assert_eq!(
                                result.unwrap_err(),
                                Error::AlreadyExists
                            );
                            return;
                        }

                        // if the current node is not dir, the action
                        // should fail
                        if !node.is_dir() {
                            assert_eq!(result.unwrap_err(), Error::NotDir);
                            return;
                        }

                        // otherwise, file is created then write data to file
                        // and do the same to control group
                        let mut file = result.unwrap();
                        let result = step.write_to_file(&mut file, data);
                        if !is_faulty_err!(result) {
                            ctlgrp.add_file(&path, &data[..]);
                        }
                    }
                    FileType::Dir => {
                        let result =
                            skip_faulty!(repo_handle.repo.create_dir(&path));

                        // if the dir already exists, the action should fail
                        if ctlgrp.has_node(&path) {
                            assert_eq!(
                                result.unwrap_err(),
                                Error::AlreadyExists
                            );
                            return;
                        }

                        // if the current node is not dir, the action
                        // should fail
                        if !node.is_dir() {
                            assert_eq!(result.unwrap_err(), Error::NotDir);
                            return;
                        }

                        // otherwise, dir is created then do the same
                        // to control group
                        let _ = result.unwrap();
                        ctlgrp.add_dir(&path);
                    }
                }
            }

            Action::Read => {
                if node.is_file() {
                    // compare file content
                    let _ = skip_faulty!(
                        node.compare_file_content(&mut repo_handle.repo)
                    );
                } else {
                    // compre directory
                    let result =
                        skip_faulty!(repo_handle.repo.read_dir(&node.path));
                    let children = result.unwrap();
                    let mut cmp_grp: Vec<&Path> =
                        children.iter().map(|c| c.path()).collect();
                    cmp_grp.sort();
                    let dirs = ctlgrp.get_children(&node.path);
                    assert_eq!(dirs, cmp_grp);
                }
            }

            Action::Update => {
                let result = skip_faulty!(
                    OpenOptions::new()
                        .write(true)
                        .open(&mut repo_handle.repo, &node.path)
                );
                if node.is_dir() {
                    assert_eq!(result.unwrap_err(), Error::IsDir);
                    return;
                }

                // update file
                let data = &data[step.data_pos..step.data_pos + step.data_len];
                let mut file = result.unwrap();
                let result = step.write_to_file(&mut file, data);
                if is_faulty_err!(result) {
                    return;
                }

                // and do the same to to control group
                let mut nd = ctlgrp.find_node_mut(&node.path).unwrap();
                let old_len = nd.data.len();
                let pos = step.file_pos;
                let new_len = pos + step.data_len;
                if new_len > old_len {
                    nd.data[pos..].copy_from_slice(&data[..old_len - pos]);
                    nd.data.extend_from_slice(&data[old_len - pos..]);
                } else {
                    nd.data[pos..pos + step.data_len]
                        .copy_from_slice(&data[..]);
                }
            }

            Action::Truncate => {
                let result = skip_faulty!(
                    OpenOptions::new()
                        .write(true)
                        .open(&mut repo_handle.repo, &node.path)
                );
                if node.is_dir() {
                    assert_eq!(result.unwrap_err(), Error::IsDir);
                    return;
                }

                // truncate file
                let mut file = result.unwrap();
                let result = file.set_len(step.data_len);
                if is_faulty_err!(result) {
                    return;
                }

                // and do the same to to control group
                let mut nd = ctlgrp.find_node_mut(&node.path).unwrap();
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
                    let result =
                        skip_faulty!(repo_handle.repo.remove_dir(&node.path));
                    if node.is_root() {
                        assert_eq!(result.unwrap_err(), Error::IsRoot);
                    } else {
                        if ctlgrp.has_child(&node.path) {
                            assert_eq!(result.unwrap_err(), Error::NotEmpty);
                        } else {
                            result.unwrap();

                            // remove dir in control group
                            ctlgrp.del(&node.path);
                        }
                    }
                } else {
                    // remove file and do the same to control group
                    let _ =
                        skip_faulty!(repo_handle.repo.remove_file(&node.path));
                    ctlgrp.del(&node.path);
                }
            }

            Action::DeleteAll => {
                // NOTE: DeleteAll is not a atomic operation, it is hard to
                // replicate the action to control group, so we have to skip
                // this test for faulty storage test.
                if cfg!(feature = "storage-faulty") {
                    return;
                }

                let result =
                    skip_faulty!(repo_handle.repo.remove_dir_all(&node.path));
                if node.is_root() {
                    result.unwrap();
                    ctlgrp.0.retain(|n| n.is_root());
                } else if node.is_dir() {
                    result.unwrap();
                    ctlgrp.del_all_children(&node.path);
                } else {
                    assert_eq!(result.unwrap_err(), Error::NotDir);
                }
            }

            Action::Rename => {
                if node.is_root() {
                    let result = skip_faulty!(
                        repo_handle.repo.rename(&node.path, "/xxx")
                    );
                    assert_eq!(result.unwrap_err(), Error::InvalidArgument);
                } else {
                    let new_path = node.path.parent().unwrap().join(&step.name);
                    let _ = skip_faulty!(handle_rename(
                        &new_path,
                        &node,
                        ctlgrp,
                        &mut repo_handle.repo
                    ));
                }
            }

            Action::Move => {
                if node.is_root() {
                    return;
                }

                let tgt = &ctlgrp[step.tgt_idx].clone();
                if tgt.is_root() {
                    let result = skip_faulty!(
                        repo_handle.repo.rename(&node.path, &tgt.path)
                    );
                    assert_eq!(result.unwrap_err(), Error::IsRoot);
                    return;
                }

                let new_path = if tgt.is_dir() {
                    tgt.path.join(&step.name)
                } else {
                    tgt.path.clone()
                };
                let _ = skip_faulty!(handle_rename(
                    &new_path,
                    &node,
                    ctlgrp,
                    &mut repo_handle.repo
                ));
            }

            Action::Copy => {
                // NOTE: DeleteAll is not a atomic operation, it is hard to
                // replicate the action to control group, so we have to skip
                // this test for faulty storage test.
                if cfg!(feature = "storage-faulty") {
                    return;
                }

                let tgt = &ctlgrp[step.tgt_idx].clone();

                let result =
                    skip_faulty!(repo_handle.repo.copy(&node.path, &tgt.path));

                if node.is_dir() || tgt.is_dir() {
                    assert_eq!(result.unwrap_err(), Error::NotFile);
                    return;
                }

                result.unwrap();

                if ctlgrp.has_node(&tgt.path) {
                    // copy to existing node
                    let mut nd = ctlgrp.find_node_mut(&tgt.path).unwrap();
                    nd.data = node.data.clone();
                } else {
                    // copy to new node
                    ctlgrp.add_file(&tgt.path, &node.data[..]);
                }
            }

            Action::Reopen => {
                let info = repo_handle.repo.info();
                let result = skip_faulty!(
                    RepoOpener::new().open(info.uri(), Fuzzer::PWD)
                );
                repo_handle.repo = result.unwrap();
            }
        }
    }
}

#[test]
//#[ignore]
fn fuzz_test() {
    let batches = 1;
    let rounds = 30;

    let storage = if cfg!(feature = "storage-faulty") {
        "faulty"
    } else {
        "file"
    };

    for _ in 0..batches {
        let tester = Tester {};
        let mut fuzzer = Fuzzer::new(storage, Box::new(tester));
        fuzzer.run(rounds);
    }
}

#[test]
#[ignore]
fn fuzz_test_rerun() {
    let tester = Tester {};
    Fuzzer::rerun("1535506168", Box::new(tester));
}
