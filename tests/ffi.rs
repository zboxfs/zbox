extern crate tempdir;

use self::tempdir::TempDir;

use std::process::Command;

#[test]
fn ffi_c() {
    let tmpdir = TempDir::new("zbox_test_ffi").expect("Create temp dir failed");

    let output_dir = tmpdir.path();
    let exe = output_dir.join("ffi");
    println!("==={}", exe.display());
    let mut release_link = "";
    if !cfg!(debug_assertions) {
        release_link = "-Ltarget/release";
    }
    let mut cmd = Command::new("cc");
    cmd.arg("-o").arg(&exe).arg("-lzbox").arg("tests/ffi.c");

    if cfg!(debug_assertions) {
        cmd.arg("-Ltarget/debug");
    } else {
        cmd.arg("-Ltarget/release");
    }

    let output = cmd.output().expect("Failed to run command");
    println!("==={:?}", output);
    if !output.status.success() {
        return;
    }

    let output = Command::new(&exe).output().expect("Failed to run command");
    println!("==={:?}", output);
}
