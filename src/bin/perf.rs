#![allow(dead_code)]

extern crate zbox;

mod common;

use std::fs;
use std::io::{self, Write};
use std::time::{Duration, Instant};
use std::env;

use zbox::{init_env, Repo, RepoOpener, OpenOptions, File};

const DATA_LEN: usize = 30 * 1024 * 1024;
const ROUND: usize = 3;
const FILE_LEN: usize = DATA_LEN / ROUND;

#[inline]
fn time_str(duration: &Duration) -> String {
    format!("{}.{}s", duration.as_secs(), duration.subsec_nanos())
}

fn speed_str(duration: &Duration) -> String {
    let secs = duration.as_secs() as f32 +
        duration.subsec_nanos() as f32 / 1_000_000_000.0;
    let speed = DATA_LEN as f32 / (1024.0 * 1024.0) / secs;
    format!("{} MB/s", speed)
}

fn print_result(duration: &Duration) {
    println!(
        "Result: duration: {}, speed: {}",
        time_str(&duration),
        speed_str(&duration)
    );
}

fn make_test_data() -> Vec<u8> {
    print!("Making {}MB test data...", DATA_LEN / 1024 / 1024);
    io::stdout().flush().unwrap();
    let seed = common::RandomSeed::from(&[0u8; 32]);
    let mut buf = vec![0u8; DATA_LEN];
    common::random_buf_deterministic(&mut buf, &seed);
    println!("done");
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

    let duration = now.elapsed();
    println!("done");
    print_result(&duration);
}

fn test_mem_perf(data: &[u8]) {
    println!("\nStart memory storage performance test");

    let mut repo = RepoOpener::new()
        .create(true)
        .open("mem://perf", "pwd")
        .unwrap();
    let mut files = make_files(&mut repo);

    test_perf(&mut files, data);
}

fn test_file_perf(data: &[u8]) {
    println!("\nStart file storage performance test");

    let mut dir = env::temp_dir();
    dir.push("zbox_perf_test");

    let mut repo = RepoOpener::new()
        .create_new(true)
        .open(&format!("file://{}", dir.display()), "pwd")
        .unwrap();
    let mut files = make_files(&mut repo);

    test_perf(&mut files, data);

    fs::remove_dir_all(&dir).unwrap();
}

fn main() {
    init_env();
    let data = make_test_data();
    test_mem_perf(&data);
    test_file_perf(&data);
}
