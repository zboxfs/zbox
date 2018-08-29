#![allow(dead_code)]
#![cfg(feature = "test-perf")]

extern crate rand;
extern crate zbox;

use std::env;
use std::fs;
use std::io::{self, Read, Write};
use std::path::Path;
use std::ptr;
use std::time::{Duration, Instant};

use rand::{Rng, SeedableRng, XorShiftRng};
use zbox::{init_env, File, OpenOptions, Repo, RepoOpener};

const DATA_LEN: usize = 60 * 1024 * 1024;
const ROUND: usize = 3;
const FILE_LEN: usize = DATA_LEN / ROUND;

#[inline]
fn time_str(duration: &Duration) -> String {
    format!("{}.{}s", duration.as_secs(), duration.subsec_nanos())
}

fn speed_str(duration: &Duration) -> String {
    let secs = duration.as_secs() as f32
        + duration.subsec_nanos() as f32 / 1_000_000_000.0;
    let speed = DATA_LEN as f32 / (1024.0 * 1024.0) / secs;
    format!("{} MB/s", speed)
}

fn print_result(read_time: &Duration, write_time: &Duration) {
    println!(
        "read: {}, write: {}",
        speed_str(&read_time),
        speed_str(&write_time)
    );
}

fn make_test_data() -> Vec<u8> {
    print!("\nMaking {}MB test data...", DATA_LEN / 1024 / 1024);
    io::stdout().flush().unwrap();
    let mut buf = vec![0u8; DATA_LEN];
    let mut rng = XorShiftRng::from_seed([42u32; 4]);
    rng.fill_bytes(&mut buf);
    println!("done\n");
    buf
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

fn test_perf(files: &mut Vec<File>, data: &[u8]) {
    print!("Writing data to file...");
    io::stdout().flush().unwrap();
    let now = Instant::now();
    for i in 0..ROUND {
        files[i]
            .write_once(&data[i * FILE_LEN..(i + 1) * FILE_LEN])
            .unwrap();
    }
    let write_time = now.elapsed();
    println!("done");

    print!("Reading data from file...");
    io::stdout().flush().unwrap();
    let mut buf = vec![0u8; FILE_LEN];
    let now = Instant::now();
    for i in 0..ROUND {
        files[i].read_to_end(&mut buf).unwrap();
    }
    let read_time = now.elapsed();
    println!("done");

    print_result(&read_time, &write_time);
    println!();
}

fn test_baseline(data: &Vec<u8>, dir: &Path) {
    println!("----------------------------------");
    println!("Baseline test");
    println!("----------------------------------");

    let mut buf = vec![0u8; FILE_LEN];

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
    print_result(&memcpy_time, &memcpy_time);

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
    print_result(&read_time, &write_time);
    println!();
}

fn test_mem_perf(data: &[u8]) {
    println!("----------------------------------");
    println!("Memory storage performance test");
    println!("----------------------------------");

    let mut repo = RepoOpener::new()
        .create(true)
        .open("mem://perf", "pwd")
        .unwrap();
    let mut files = make_files(&mut repo);

    test_perf(&mut files, data);
}

fn test_file_perf(data: &[u8], dir: &Path) {
    println!("----------------------------------");
    println!("File storage performance test");
    println!("----------------------------------");

    let mut repo = RepoOpener::new()
        .create_new(true)
        .open(&format!("file://{}/repo", dir.display()), "pwd")
        .unwrap();
    let mut files = make_files(&mut repo);

    test_perf(&mut files, data);
}

#[test]
fn perf_test() {
    init_env();

    let mut dir = env::temp_dir();
    dir.push("zbox_perf_test");
    fs::create_dir(&dir).unwrap();

    let data = make_test_data();
    test_baseline(&data, &dir);
    test_mem_perf(&data);
    test_file_perf(&data, &dir);

    fs::remove_dir_all(&dir).unwrap();
}
