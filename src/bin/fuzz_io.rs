#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate rmp_serde;
extern crate bytes;
extern crate zbox;

mod common;

use std::io::{Read, Write, Seek, SeekFrom, Cursor};
use std::fmt::{self, Debug};
use std::sync::{Arc, RwLock};
use std::thread;
use std::cmp::min;
use std::fs;

use bytes::{Buf, BufMut, LittleEndian};
use zbox::{OpenOptions, Repo, File};

const RND_DATA_LEN: usize = 2 * 1024 * 1024;
const DATA_LEN: usize = 2 * RND_DATA_LEN;

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
        let file_pos = common::random_usize(old_len);
        let (data_pos, buf) = common::random_slice(data);
        let do_set_len = common::random_u32(4) == 1;
        let new_len = common::random_usize(old_len * 2);
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
    fn save(&self, env: &common::TestEnv2) {
        let mut buf = Vec::new();
        let steps_path = env.path.join("steps");
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(&steps_path)
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
    fn load_all(env: &common::TestEnv2) -> Vec<Self> {
        let mut buf = Vec::new();
        let steps_path = env.path.join("steps");
        let mut file = fs::File::open(&steps_path).unwrap();
        let read = file.read_to_end(&mut buf).unwrap();
        let mut ret = Vec::new();
        let round = read / Self::BYTES_LEN;
        println!(
            "read: {}, size: {}, round: {}",
            read,
            Self::BYTES_LEN,
            round
        );

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

        ret
    }
}

impl Debug for Step {
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
    let mut f = repo.open_file("/file").unwrap();
    let mut dst = Vec::new();
    f.seek(SeekFrom::Start(0)).unwrap();
    let file_len = f.read_to_end(&mut dst).unwrap();
    assert_eq!(file_len, ctl.len());
    assert_eq!(&dst[..], &ctl[..]);
}

fn test_round(f: &mut File, step: &Step, src_data: &[u8], ctl: &mut Vec<u8>) {
    let old_len = f.metadata().len();
    let data = &src_data[step.data_pos..step.data_pos + step.data_len];

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
}

fn fuzz_file_read_write() {
    let mut env = common::TestEnv2::new("file_io");
    let mut file = OpenOptions::new()
        .create(true)
        .open(&mut env.repo, "/file")
        .unwrap();
    let mut ctl = Vec::new();
    let rounds = 5;
    let steps = vec![0; rounds];

    // start fuzz rounds
    // ------------------
    for round in 0..steps.len() {
        let meta = file.metadata();
        let old_len = meta.len();
        let step = Step::new_random(round, old_len, &env.data);
        step.save(&env);
        test_round(&mut file, &step, &env.data, &mut ctl);
    }

    // verify
    // ------------------
    verify(&mut env.repo, &ctl);
}

fn fuzz_file_read_write_reproduce(batch_id: &str) {
    let mut env = common::TestEnv2::load(batch_id);
    let mut file = OpenOptions::new()
        .create(true)
        .open(&mut env.repo, "/file")
        .unwrap();
    let mut ctl = Vec::new();
    let steps = Step::load_all(&env);

    // start fuzz rounds
    // ------------------
    for round in 0..steps.len() {
        let step = &steps[round];
        test_round(&mut file, step, &env.data, &mut ctl);
    }

    // verify
    // ------------------
    verify(&mut env.repo, &ctl);
}

/*fn fuzz_file_read_write_mt() {
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
}*/

fn main() {
    //fuzz_file_read_write();
    fuzz_file_read_write_reproduce("file_io-1513211014");
    //fuzz_file_read_write_mt();
}
