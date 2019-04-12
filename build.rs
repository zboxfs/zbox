extern crate libflate;
extern crate pkg_config;
extern crate reqwest;
extern crate tar;

use std::env;

fn main() {
    #[cfg(feature = "libsodium-bundled")]
    download_and_install_libsodium();

    #[cfg(not(feature = "libsodium-bundled"))]
    {
        println!("cargo:rerun-if-env-changed=SODIUM_LIB_DIR");
        println!("cargo:rerun-if-env-changed=SODIUM_STATIC");
    }

    // add libsodium link options
    if let Ok(lib_dir) = env::var("SODIUM_LIB_DIR") {
        println!("cargo:rustc-link-search=native={}", lib_dir);
        let mode = match env::var_os("SODIUM_STATIC") {
            Some(_) => "static",
            None => "dylib",
        };
        if cfg!(target_os = "windows") {
            println!("cargo:rustc-link-lib={0}=libsodium", mode);
        } else {
            println!("cargo:rustc-link-lib={0}=sodium", mode);
        }
    } else {
        // the static linking doesn't work if libsodium is installed
        // under '/usr' dir, in that case use the environment variables
        // mentioned above
        pkg_config::Config::new()
            .atleast_version("1.0.16")
            .statik(true)
            .probe("libsodium")
            .unwrap();
    }
}

// This downloads function and builds the libsodium from source for linux and unix targets.
// The steps are taken from the libsodium installation instructions:
// https://libsodium.gitbook.io/doc/installation
// effectively:
// $ ./configure
// $ make && make check
// $ sudo make install
#[cfg(all(feature = "libsodium-bundled", not(target_os = "windows")))]
fn download_and_install_libsodium() {
    use libflate::non_blocking::gzip::Decoder;
    use std::io::{stderr, stdout, Write};
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use tar::Archive;
    static LIBSODIUM_ZIP: &'static str = "https://download.libsodium.org/libsodium/releases/libsodium-1.0.17.tar.gz";
    static LIBSODIUM_NAME: &'static str = "libsodium-1.0.17";
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let install_dir = out_dir.join("libsodium_install");
    let source_dir = install_dir.join(LIBSODIUM_NAME);
    let prefix_dir = out_dir.join("libsodium");
    let sodium_lib_dir = prefix_dir.join("lib");

    if !install_dir.exists() {
        let response = reqwest::get(LIBSODIUM_ZIP).unwrap();
        let decoder = Decoder::new(response);
        let mut ar = Archive::new(decoder);
        ar.unpack(&install_dir).unwrap();
    }

    if !sodium_lib_dir.exists() {
        let configure = source_dir.join("./configure");
        let output = Command::new(&configure)
            .current_dir(&source_dir)
            .args(&[Path::new("--prefix"), &prefix_dir])
            .output()
            .expect("failed to execute configure");
        stdout().write_all(&output.stdout).unwrap();
        stderr().write_all(&output.stderr).unwrap();

        let output = Command::new("make")
            .current_dir(&source_dir)
            .output()
            .expect("failed to execute make");
        stdout().write_all(&output.stdout).unwrap();
        stderr().write_all(&output.stderr).unwrap();

        let output = Command::new("make")
            .current_dir(&source_dir)
            .arg("check")
            .output()
            .expect("failed to execute make check");
        stdout().write_all(&output.stdout).unwrap();
        stderr().write_all(&output.stderr).unwrap();

        let output = std::process::Command::new("make")
            .current_dir(&source_dir)
            .arg("install")
            .output()
            .expect("failed to execute sudo make install");
        stdout().write_all(&output.stdout).unwrap();
        stderr().write_all(&output.stderr).unwrap();
    }

    assert!(
        &sodium_lib_dir.exists(),
        "libsodium lib directory was not created."
    );

    env::set_var("SODIUM_LIB_DIR", &sodium_lib_dir);
    env::set_var("SODIUM_STATIC", "true");
}

// This downloads function and builds the libsodium from source for windows msvc target.
// The binaries are pre-compiled, so we simply download and link.
// The binary is compressed in zip format.
#[cfg(all(
    feature = "libsodium-bundled",
    target_os = "windows",
    target_env = "msvc"
))]
fn download_and_install_libsodium() {
    use std::fs;
    use std::fs::OpenOptions;
    use std::io;
    use std::path::PathBuf;
    #[cfg(target_env = "msvc")]
    static LIBSODIUM_ZIP: &'static str = "https://download.libsodium.org/libsodium/releases/libsodium-1.0.17-msvc.zip";
    #[cfg(target_env = "mingw")]
    static LIBSODIUM_ZIP: &'static str = "https://download.libsodium.org/libsodium/releases/libsodium-1.0.17-mingw.tar.gz";
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let sodium_lib_dir = out_dir.join("libsodium");
    if !sodium_lib_dir.exists() {
        fs::create_dir(&sodium_lib_dir).unwrap();
    }
    let sodium_lib_file_path = sodium_lib_dir.join("libsodium.lib");
    if !sodium_lib_file_path.exists() {
        let mut tmpfile = tempfile::tempfile().unwrap();
        reqwest::get(LIBSODIUM_ZIP)
            .unwrap()
            .copy_to(&mut tmpfile)
            .unwrap();
        let mut zip = zip::ZipArchive::new(tmpfile).unwrap();
        #[cfg(target_arch = "x86_64")]
        let mut lib = zip
            .by_name("x64/Release/v142/static/libsodium.lib")
            .unwrap();
        #[cfg(target_arch = "x86")]
        let mut lib = zip
            .by_name("Win32/Release/v142/static/libsodium.lib")
            .unwrap();
        #[cfg(not(any(target_arch = "x86_64", target_arch = "x86")))]
        compile_error!("Bundled libsodium is only supported on x86 or x86_64 target architecture.");
        let mut libsodium_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&sodium_lib_file_path)
            .unwrap();
        io::copy(&mut lib, &mut libsodium_file).unwrap();
    }
    assert!(
        &sodium_lib_dir.exists(),
        "libsodium lib directory was not created."
    );
    env::set_var("SODIUM_LIB_DIR", &sodium_lib_dir);
    env::set_var("SODIUM_STATIC", "true");
}

// This downloads function and builds the libsodium from source for windows mingw target.
// The binaries are pre-compiled, so we simply download and link.
// The binary is compressed in tar.gz format.
#[cfg(all(
    feature = "libsodium-bundled",
    target_os = "windows",
    target_env = "gnu"
))]
fn download_and_install_libsodium() {
    use libflate::non_blocking::gzip::Decoder;
    use std::fs;
    use std::fs::OpenOptions;
    use std::io;
    use std::path::PathBuf;
    use tar::Archive;
    static LIBSODIUM_ZIP: &'static str = "https://download.libsodium.org/libsodium/releases/libsodium-1.0.17-mingw.tar.gz";
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let sodium_lib_dir = out_dir.join("libsodium");
    if !sodium_lib_dir.exists() {
        fs::create_dir(&sodium_lib_dir).unwrap();
    }
    let sodium_lib_file_path = sodium_lib_dir.join("libsodium.lib");
    if !sodium_lib_file_path.exists() {
        let response = reqwest::get(LIBSODIUM_ZIP).unwrap();
        let decoder = Decoder::new(response);
        let mut ar = Archive::new(decoder);
        #[cfg(target_arch = "x86_64")]
        let filename = PathBuf::from("libsodium-win64/lib/libsodium.a");
        #[cfg(target_arch = "x86")]
        let filename = PathBuf::from("libsodium-win32/lib/libsodium.a");
        #[cfg(not(any(target_arch = "x86_64", target_arch = "x86")))]
        compile_error!("Bundled libsodium is only supported on x86 or x86_64 target architecture.");
        for file in ar.entries().unwrap() {
            let mut f = file.unwrap();
            if f.path().unwrap() == *filename {
                let mut libsodium_file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(&sodium_lib_file_path)
                    .unwrap();
                io::copy(&mut f, &mut libsodium_file).unwrap();
                break;
            }
        }
    }
    assert!(
        &sodium_lib_dir.exists(),
        "libsodium lib directory was not created."
    );
    env::set_var("SODIUM_LIB_DIR", &sodium_lib_dir);
    env::set_var("SODIUM_STATIC", "true");
}
