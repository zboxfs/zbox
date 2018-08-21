#![cfg(feature = "fuzz-test")]

extern crate bytes;
extern crate rmp_serde;
extern crate serde;
extern crate zbox;

mod common;

use std::cmp::min;
use std::fmt::{self, Debug};
use std::fs;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, RwLock};
use std::thread;

use bytes::{Buf, BufMut, LittleEndian};

use common::fuzz;
use zbox::{File, OpenOptions, Repo};

#[derive(Default)]
struct Step {
    round: usize,
    do_set_len: bool,
    new_len: usize,
    file_pos: usize,
    data_pos: usize,
    data_len: usize,
}

impl Step {
    const BYTES_LEN: usize = 6 * 8;

    fn new_random(round: usize, old_len: usize, data: &[u8]) -> Self {
        let file_pos = fuzz::random_usize(old_len);
        let (data_pos, buf) = fuzz::random_slice(data);
        let do_set_len = fuzz::random_u32(4) == 1;
        let new_len = fuzz::random_usize((old_len as f32 * 1.2) as usize);
        Step {
            round,
            do_set_len,
            new_len,
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
        buf.put_u64::<LittleEndian>(self.do_set_len as u64);
        buf.put_u64::<LittleEndian>(self.new_len as u64);
        buf.put_u64::<LittleEndian>(self.file_pos as u64);
        buf.put_u64::<LittleEndian>(self.data_pos as u64);
        buf.put_u64::<LittleEndian>(self.data_len as u64);
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
            let do_set_len = cur.get_u64::<LittleEndian>() == 1;
            let new_len = cur.get_u64::<LittleEndian>() as usize;
            let file_pos = cur.get_u64::<LittleEndian>() as usize;
            let data_pos = cur.get_u64::<LittleEndian>() as usize;
            let data_len = cur.get_u64::<LittleEndian>() as usize;
            let step = Step {
                round,
                do_set_len,
                new_len,
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
            "Step {{ round: {}, do_set_len: {}, new_len: {}, \
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
    println!("Start verifying...");
    let mut f = repo.open_file("/file").unwrap();
    let mut dst = Vec::new();
    f.seek(SeekFrom::Start(0)).unwrap();
    let file_len = f.read_to_end(&mut dst).unwrap();
    assert_eq!(file_len, ctl.len());
    if &dst[..] != &ctl[..] {
        panic!("content not match");
    }
    println!("Completed.");
}

fn test_round(
    f: &mut File,
    step: &Step,
    src_data: &[u8],
    round: usize,
    rounds: usize,
    ctl: &mut Vec<u8>,
) {
    let curr = thread::current();
    let worker = curr.name().unwrap();

    if round == 0 {
        println!("{}: Start {} file fuzz test rounds...", worker, rounds);
    }

    let old_len = f.metadata().unwrap().len();
    let data = &src_data[step.data_pos..step.data_pos + step.data_len];
    //println!("step: {:?}", step);

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
        f.write_all(&data[..]).unwrap();
        f.finish().unwrap();

        // write to control group
        let overlap = min(ctl.len() - step.file_pos, step.data_len);
        &mut ctl[step.file_pos..step.file_pos + overlap]
            .copy_from_slice(&data[..overlap]);
        if overlap < step.data_len {
            ctl.extend_from_slice(&data[overlap..]);
        }
    }

    if round % 10 == 0 {
        let meta = f.metadata().unwrap();
        println!(
            "{}: {}/{}, file len: {}, ...",
            worker,
            round,
            rounds,
            fuzz::readable(meta.len().to_string())
        );
    }
    if round == rounds - 1 {
        println!("{}: Finished.", worker);
    }
}

fn fuzz_file_read_write(rounds: usize) {
    let mut env = fuzz::TestEnv::new("file");
    let mut file = OpenOptions::new()
        .create(true)
        .open(&mut env.repo, "/file")
        .unwrap();
    let mut ctl = Vec::new();

    // start fuzz rounds
    // ------------------
    for round in 0..rounds {
        let meta = file.metadata().unwrap();
        let old_len = meta.len();
        let step = Step::new_random(round, old_len, &env.data);
        step.save(&env);
        test_round(&mut file, &step, &env.data, round, rounds, &mut ctl);
    }

    // verify
    // ------------------
    verify(&mut env.repo, &ctl);
}

#[allow(dead_code)]
fn fuzz_file_read_write_reproduce(batch_id: &str) {
    let mut env = fuzz::TestEnv::load(batch_id);
    let mut file = OpenOptions::new()
        .create(true)
        .open(&mut env.repo, "/file")
        .unwrap();
    let mut ctl = Vec::new();
    let steps = Step::load_all(&env);
    let rounds = steps.len();

    // start fuzz rounds
    // ------------------
    for round in 0..rounds {
        let step = &steps[round];
        test_round(&mut file, step, &env.data, round, rounds, &mut ctl);
    }

    // verify
    // ------------------
    verify(&mut env.repo, &ctl);
}

fn fuzz_file_read_write_mt(rounds: usize) {
    let env = fuzz::TestEnv::new("file_mt").into_ref();
    let ctl_grp = Arc::new(RwLock::new(Vec::new()));
    let worker_cnt = 4;

    // create empty file
    {
        let mut env = env.write().unwrap();
        OpenOptions::new()
            .create(true)
            .open(&mut env.repo, "/file")
            .unwrap();
    }

    // start fuzz rounds
    // ------------------
    let mut workers = Vec::new();
    for i in 0..worker_cnt {
        let env = env.clone();
        let ctl = ctl_grp.clone();
        let name = format!("worker-{}", i);
        let builder = thread::Builder::new().name(name);

        workers.push(
            builder
                .spawn(move || {
                    for round in 0..rounds {
                        let mut env = env.write().unwrap();
                        let mut file = OpenOptions::new()
                            .write(true)
                            .open(&mut env.repo, "/file")
                            .unwrap();
                        let mut ctl = ctl.write().unwrap();
                        let old_len = file.metadata().unwrap().len();
                        let step = Step::new_random(round, old_len, &env.data);

                        test_round(
                            &mut file, &step, &env.data, round, rounds,
                            &mut ctl,
                        );
                    }
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
        let ctl = ctl_grp.read().unwrap();
        verify(&mut env.repo, &ctl);
    }
}

#[test]
fn fuzz_file() {
    fuzz_file_read_write(30);
    //fuzz_file_read_write_reproduce("file_1513641767");
    fuzz_file_read_write_mt(30);
}
