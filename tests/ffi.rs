extern crate tempdir;

use self::tempdir::TempDir;

use std::process::Command;

#[test]
fn ffi() {
    let tmpdir = TempDir::new("zbox_test_ffi").expect("Create temp dir failed");
    let output_dir = tmpdir.path();
    let mut exe = output_dir.join("ffi");
    let mut cmd;

    if cfg!(target_os = "windows") {
        exe.set_extension("exe");

        cmd = Command::new("cl");
        cmd.arg("/nologo")
            .arg("/WX")
            .arg("-Isrc/ffi/include")
            .arg("tests/ffi.c");

        if cfg!(debug_assertions) {
            cmd.arg("target/debug/zbox.dll.lib");
        } else {
            cmd.arg("target/release/zbox.dll.lib");
        }
        cmd.arg("/link");
        cmd.arg("/out:".to_owned() + exe.to_str().unwrap());
    } else {
        cmd = Command::new("gcc");
        cmd.arg("-Wall")
            .arg("-o")
            .arg(&exe)
            .arg("tests/ffi.c")
            .arg("-lzbox")
            .arg("-Isrc/ffi/include");

        if cfg!(debug_assertions) {
            cmd.arg("-Ltarget/debug");
        } else {
            cmd.arg("-Ltarget/release");
        }
    }

    // compile
    let output = cmd.output().expect("Failed to run compiler");
    if !output.status.success() {
        println!("{:#?}", output);
    }
    assert!(output.status.success());

    // execute
    let output = Command::new(&exe).output().expect("Failed to run ffi");
    if !output.status.success() {
        println!("{:#?}", output);
    }
    assert!(output.status.success());
}
