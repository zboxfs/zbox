#![cfg(feature = "jni-lib")]

extern crate tempdir;

use self::tempdir::TempDir;

use std::process::Command;

#[test]
fn jni() {
    let tmpdir = TempDir::new("zbox_test_jni").expect("Create temp dir failed");
    let output_dir = tmpdir.path();

    // compile
    let mut cmd = Command::new("javac");
    cmd.arg("-sourcepath")
        .arg("./src/jni_lib")
        .arg("-d")
        .arg(&output_dir)
        .arg("./tests/JniTest.java");
    let output = cmd.output().expect("Failed to run compiler");
    if !output.status.success() {
        println!("{:#?}", output);
    }
    assert!(output.status.success());

    // execute
    let mut cmd = Command::new("java");
    cmd.arg("-Djava.library.path=target/debug")
        .arg("-cp")
        .arg(&output_dir)
        .arg("JniTest");
    let output = cmd.output().expect("Failed to run java");
    println!("{:?}", output);
    if !output.status.success() {
        println!("{:#?}", output);
    }
    assert!(output.status.success());
}
