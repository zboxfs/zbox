extern crate tempdir;
extern crate zbox;

mod common;

use std::io::{Read, Write, Seek, SeekFrom};
use std::fmt::{self, Debug};
use std::sync::{Arc, RwLock};
use std::thread;
use std::cmp::min;
use zbox::{OpenOptions, Repo, File};

const RND_DATA_LEN: usize = 2 * 1024 * 1024;
const DATA_LEN: usize = 2 * RND_DATA_LEN;

struct Step<'a> {
    round: usize,
    do_set_len: bool,
    new_len: usize,
    file_pos: usize,
    data_pos: usize,
    data_len: usize,
    data: &'a [u8],
}

impl<'a> Debug for Step<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Step {{ round: {}, do_set_len: {}, new_len: {}, \
                file_pos: {}, data_pos: {}, data_len: {}, \
                data: &test_data[{}..{}] }},",
            self.round,
            self.do_set_len,
            self.new_len,
            self.file_pos,
            self.data_pos,
            self.data_len,
            self.data_pos,
            self.data_pos + self.data_len,
        )
    }
}

fn verify(repo: &mut Repo, ctl: &[u8]) {
    //println!("===verify===");
    let mut f = repo.open_file("/file").unwrap();
    let mut dst = Vec::new();
    f.seek(SeekFrom::Start(0)).unwrap();
    let file_len = f.read_to_end(&mut dst).unwrap();
    assert_eq!(file_len, ctl.len());
    assert_eq!(&dst[..], &ctl[..]);
}

fn test_round(round: usize, f: &mut File, test_data: &[u8], ctl: &mut Vec<u8>) {
    let meta = f.metadata();
    let old_len = meta.len();

    let file_pos = common::random_usize(old_len);
    let (data_pos, buf) = common::random_slice(&test_data);
    let do_set_len = common::random_u32(4) == 1;
    let new_len = common::random_usize(old_len * 2);
    let step = Step {
        round,
        do_set_len,
        new_len,
        file_pos,
        data_pos,
        data_len: buf.len(),
        data: buf,
    };

    //let step = &steps[round];
    //println!("{:?}", step);

    if step.do_set_len {
        f.set_len(step.new_len).unwrap();

        // do same to control group
        if step.new_len > old_len {
            let extra = vec![0u8; step.new_len - old_len];
            ctl.extend_from_slice(&extra[..]);
        } else {
            ctl.truncate(step.new_len);
        }

    } else {
        f.seek(SeekFrom::Start(step.file_pos as u64)).unwrap();
        f.write_all(&step.data[..]).unwrap();
        f.finish().unwrap();

        // write to control group
        let overlap = min(ctl.len() - step.file_pos, step.data_len);
        &mut ctl[step.file_pos..step.file_pos + overlap]
            .copy_from_slice(&step.data[..overlap]);
        if overlap < step.data_len {
            ctl.extend_from_slice(&step.data[overlap..]);
        }
    }
}

#[test]
//#[cfg_attr(rustfmt, rustfmt_skip)]
fn fuzz_file_read_write() {
    let mut env = common::setup();
    let mut repo = &mut env.repo;
    let mut f = OpenOptions::new()
        .create(true)
        .open(&mut repo, "/file")
        .unwrap();
    let (seed, permu, test_data) =
        common::make_test_data(RND_DATA_LEN, DATA_LEN);
    let mut ctl = Vec::new();
    let rounds = 10;
    let steps = vec![0; rounds];

    //println!("seed: {:?}", seed);
    //println!("permu: {:?}", permu);
    let _ = seed;
    let _ = permu;

    // uncomment below to reproduce the bug found during fuzzing
    /*use common::Span;
    let seed = common::RandomSeed;
    let permu = vec!;
    let test_data = common::reprod_test_data(seed, permu);
    let steps = [];*/

    // start fuzz rounds
    // ------------------
    for round in 0..steps.len() {
        test_round(round, &mut f, &test_data, &mut ctl);
    }

    // verify
    // ------------------
    verify(repo, &ctl);
}

#[test]
fn fuzz_file_read_write_mt() {
    let env_ref = Arc::new(RwLock::new(common::setup()));
    let (seed, permu, test_data) =
        common::make_test_data(RND_DATA_LEN, DATA_LEN);
    let test_data_ref = Arc::new(test_data);
    let ctl_ref = Arc::new(RwLock::new(Vec::new()));
    let worker_cnt = 4;
    let rounds = 30;

    {
        let mut env = env_ref.write().unwrap();
        OpenOptions::new()
            .create(true)
            .open(&mut env.repo, "/file")
            .unwrap();
    }

    //println!("seed: {:?}", seed);
    //println!("permu: {:?}", permu);
    let _ = seed;
    let _ = permu;

    // uncomment below to reproduce the bug found during fuzzing
    /*let seed = common::RandomSeed;
    let permu = vec!;
    let test_data = common::reprod_test_data(seed, permu);
    let steps = [];*/

    // start fuzz rounds
    // ------------------
    let mut workers = Vec::new();
    for _ in 0..worker_cnt {
        let mut env = env_ref.write().unwrap();
        let mut f = OpenOptions::new()
            .write(true)
            .open(&mut env.repo, "/file")
            .unwrap();
        let test_data = test_data_ref.clone();
        let ctl = ctl_ref.clone();

        workers.push(thread::spawn(move || for round in 0..rounds {
            // randomly skip some rounds
            if common::random_u32(5) == 1 {
                continue;
            }
            let mut ctl = ctl.write().unwrap();
            test_round(round, &mut f, &test_data, &mut ctl);
        }));
    }
    for w in workers {
        w.join().unwrap();
    }

    // verify
    // ------------------
    {
        let mut env = env_ref.write().unwrap();
        let ctl = ctl_ref.read().unwrap();
        verify(&mut env.repo, &ctl);
    }
}
