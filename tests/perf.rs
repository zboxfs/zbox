#![allow(dead_code)]
#![cfg(feature = "test-perf")]

extern crate rand;
extern crate rand_xorshift;
extern crate zbox;

use std::env;
use std::fs;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::ptr;
use std::time::{Duration, Instant};

use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use zbox::{init_env, File, OpenOptions, Repo, RepoOpener};

const DATA_LEN: usize = 60 * 1024 * 1024;
const FILE_LEN: usize = DATA_LEN / ROUND;
const ROUND: usize = 3;
const TX_ROUND: usize = 30;

#[inline]
fn time_str(duration: &Duration) -> String {
    format!("{}.{}s", duration.as_secs(), duration.subsec_nanos())
}

fn speed_str(duration: &Duration) -> String {
    let secs = duration.as_secs() as f32
        + duration.subsec_nanos() as f32 / 1_000_000_000.0;
    let speed = DATA_LEN as f32 / (1024.0 * 1024.0) / secs;
    format!("{:.2} MB/s", speed)
}

fn tps_str(duration: &Duration) -> String {
    if duration.eq(&Duration::default()) {
        return format!("N/A");
    }
    let secs = duration.as_secs() as f32
        + duration.subsec_nanos() as f32 / 1_000_000_000.0;
    let speed = TX_ROUND as f32 / secs;
    format!("{:.0} tx/s", speed)
}

fn print_result(
    read_time: &Duration,
    write_time: &Duration,
    tx_time: &Duration,
) {
    println!(
        "read: {}, write: {}, tps: {}",
        speed_str(&read_time),
        speed_str(&write_time),
        tps_str(&tx_time),
    );
}

fn make_test_data() -> Vec<u8> {
    print!(
        "\nMaking {} MB pseudo random test data...",
        DATA_LEN / 1024 / 1024
    );
    io::stdout().flush().unwrap();
    let mut buf = vec![0u8; DATA_LEN];
    let mut rng = XorShiftRng::from_seed([42u8; 16]);
    rng.fill_bytes(&mut buf);
    println!("done\n");
    buf
}

fn test_baseline(data: &Vec<u8>, dir: &Path) {
    println!("---------------------------------------------");
    println!("Baseline test");
    println!("---------------------------------------------");

    let mut buf = vec![0u8; FILE_LEN];
    let tx_time = Duration::default();

    // test memcpy speed
    let now = Instant::now();
    for i in 0..ROUND {
        unsafe {
            ptr::copy_nonoverlapping(
                (&data[i * FILE_LEN..(i + 1) * FILE_LEN]).as_ptr(),
                (&mut buf[..]).as_mut_ptr(),
                FILE_LEN,
            );
        }
    }
    let memcpy_time = now.elapsed();
    print!("memcpy: ");
    print_result(&memcpy_time, &memcpy_time, &tx_time);

    // test os file system speed
    let now = Instant::now();
    for i in 0..ROUND {
        let path = dir.join(format!("file_{}", i));
        let mut file = fs::File::create(&path).unwrap();
        file.write_all(&data[i * FILE_LEN..(i + 1) * FILE_LEN])
            .unwrap();
        file.flush().unwrap();
    }
    let write_time = now.elapsed();

    let now = Instant::now();
    for i in 0..ROUND {
        let path = dir.join(format!("file_{}", i));
        let mut file = fs::File::open(&path).unwrap();
        file.read_to_end(&mut buf).unwrap();
    }
    let read_time = now.elapsed();

    print!("file system: ");
    print_result(&read_time, &write_time, &tx_time);
    println!();
}

fn make_files(repo: &mut Repo) -> Vec<File> {
    let mut files: Vec<File> = Vec::new();
    for i in 0..ROUND {
        let filename = format!("/file_{}", i);
        let file = OpenOptions::new()
            .create(true)
            .open(repo, filename)
            .unwrap();
        files.push(file);
    }
    files
}

fn test_perf(repo: &mut Repo, files: &mut Vec<File>, data: &[u8]) {
    print!("Performing testing...");
    io::stdout().flush().unwrap();

    // write
    let now = Instant::now();
    for i in 0..ROUND {
        let data = &data[i * FILE_LEN..(i + 1) * FILE_LEN];
        files[i].write_once(&data[..]).unwrap();
    }
    let write_time = now.elapsed();

    // read
    let mut buf = Vec::new();
    let now = Instant::now();
    for i in 0..ROUND {
        files[i].seek(SeekFrom::Start(0)).unwrap();
        let read = files[i].read_to_end(&mut buf).unwrap();
        assert_eq!(read, FILE_LEN);
    }
    let read_time = now.elapsed();

    // tx
    let mut dirs = Vec::new();
    for i in 0..TX_ROUND {
        dirs.push(format!("/dir{}", i));
    }
    let now = Instant::now();
    for i in 0..TX_ROUND {
        repo.create_dir(&dirs[i]).unwrap();
    }
    let tx_time = now.elapsed();

    println!("done");
    print_result(&read_time, &write_time, &tx_time);
    println!();
}

fn test_mem_perf(data: &[u8]) {
    println!("---------------------------------------------");
    println!("Memory storage performance test (no compress)");
    println!("---------------------------------------------");
    let mut repo = RepoOpener::new()
        .create(true)
        .open("mem://perf", "pwd")
        .unwrap();
    let mut files = make_files(&mut repo);
    test_perf(&mut repo, &mut files, data);

    println!("---------------------------------------------");
    println!("Memory storage performance test (compress)");
    println!("---------------------------------------------");
    let mut repo = RepoOpener::new()
        .create(true)
        .compress(true)
        .open("mem://perf2", "pwd")
        .unwrap();
    let mut files = make_files(&mut repo);
    test_perf(&mut repo, &mut files, data);
}

fn test_file_perf(data: &[u8], dir: &Path) {
    println!("---------------------------------------------");
    println!("File storage performance test (no compress)");
    println!("---------------------------------------------");
    let mut repo = RepoOpener::new()
        .create_new(true)
        .open(&format!("file://{}/repo", dir.display()), "pwd")
        .unwrap();
    let mut files = make_files(&mut repo);
    test_perf(&mut repo, &mut files, data);

    println!("---------------------------------------------");
    println!("File storage performance test (compress)");
    println!("---------------------------------------------");
    let mut repo = RepoOpener::new()
        .create_new(true)
        .compress(true)
        .open(&format!("file://{}/repo2", dir.display()), "pwd")
        .unwrap();
    let mut files = make_files(&mut repo);
    test_perf(&mut repo, &mut files, data);
}

#[test]
fn perf_test() {
    init_env();

    let mut dir = env::temp_dir();
    dir.push("zbox_perf_test");
    if dir.exists() {
        fs::remove_dir_all(&dir).unwrap();
    }
    fs::create_dir(&dir).unwrap();

    let data = make_test_data();
    test_baseline(&data, &dir);
    test_mem_perf(&data);
    test_file_perf(&data, &dir);

    fs::remove_dir_all(&dir).unwrap();
}
