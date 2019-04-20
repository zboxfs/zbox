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
            .atleast_version("1.0.17")
            .statik(true)
            .probe("libsodium")
            .unwrap();
    }

    // add liblz4 link options
    if let Ok(lib_dir) = env::var("LZ4_LIB_DIR") {
        println!("cargo:rustc-link-search=native={}", lib_dir);
        if cfg!(target_os = "windows") {
            println!("cargo:rustc-link-lib=static=liblz4");
        } else {
            println!("cargo:rustc-link-lib=static=lz4");
        }
    } else {
        download_and_build_lz4();
    }
}

// This function download lz4 source files from GitHub and build static library
// for non-windows target.
#[cfg(not(target_os = "windows"))]
fn download_and_build_lz4() {
    use libflate::non_blocking::gzip::Decoder;
    use std::io::{stderr, stdout, Write};
    use std::path::PathBuf;
    use std::process::Command;
    use tar::Archive;

    static LZ4_ZIP: &'static str =
        "https://github.com/lz4/lz4/archive/v1.9.0.tar.gz";
    static LZ4_NAME: &'static str = "lz4-1.9.0";
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let lz4_dir = out_dir.join(LZ4_NAME);
    let lz4_lib_dir = lz4_dir.join("lib");
    let lz4_lib_file = lz4_lib_dir.join("liblz4.a");

    if !lz4_dir.exists() {
        let response = reqwest::get(LZ4_ZIP).unwrap();
        let decoder = Decoder::new(response);
        let mut ar = Archive::new(decoder);
        ar.unpack(&out_dir).unwrap();
    }

    if !lz4_lib_file.exists() {
        let output = Command::new("make")
            .current_dir(&lz4_dir)
            .env("CFLAGS", "-fPIC -O3")
            .arg("lib-release")
            .output()
            .expect("failed to execute make for lz4 compilation");
        stdout().write_all(&output.stdout).unwrap();
        stderr().write_all(&output.stderr).unwrap();
    }

    assert!(&lz4_lib_file.exists(), "lz4 lib was not created");

    println!("cargo:rustc-link-search=native={}", lz4_lib_dir.display());
    println!("cargo:rustc-link-lib=static=lz4");
}

// This function download lz4 source files from GitHub and build for
// Windows and msvc target.
#[cfg(all(target_os = "windows", target_env = "msvc"))]
fn download_and_build_lz4() {
    use libflate::non_blocking::gzip::Decoder;
    use std::io::{stderr, stdout, Write};
    use std::path::PathBuf;
    use std::process::Command;
    use tar::Archive;

    static LZ4_ZIP: &'static str =
        "https://github.com/lz4/lz4/archive/v1.9.0.tar.gz";
    static LZ4_NAME: &'static str = "lz4-1.9.0";
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let lz4_dir = out_dir.join(LZ4_NAME);
    let lz4_lib_file = lz4_dir.join("liblz4.lib");

    if !lz4_dir.exists() {
        let response = reqwest::get(LZ4_ZIP).unwrap();
        let decoder = Decoder::new(response);
        let mut ar = Archive::new(decoder);
        ar.unpack(&out_dir).unwrap();
    }

    if !lz4_lib_file.exists() {
        let output = Command::new("cl")
            .current_dir(&lz4_dir)
            .args(&[
                "/c",
                "lib/lz4.c",
                "lib/lz4hc.c",
                "lib/lz4frame.c",
                "lib/xxhash.c",
            ])
            .output()
            .expect("failed to execute cl for lz4 compilation");
        stdout().write_all(&output.stdout).unwrap();
        stderr().write_all(&output.stderr).unwrap();

        let output = Command::new("lib")
            .current_dir(&lz4_dir)
            .args(&[
                "lz4.obj",
                "lz4hc.obj",
                "lz4frame.obj",
                "xxhash.obj",
                "/OUT:liblz4.lib",
            ])
            .output()
            .expect("failed to execute lib for lz4 linking");
        stdout().write_all(&output.stdout).unwrap();
        stderr().write_all(&output.stderr).unwrap();
    }

    assert!(&lz4_lib_file.exists(), "lz4 lib was not created");

    println!("cargo:rustc-link-search=native={}", lz4_dir.display());
    println!("cargo:rustc-link-lib=static=liblz4");
}

// This function download lz4 pre-built static lib file from GitHub for
// Windows and mingw target.
#[cfg(all(target_os = "windows", target_env = "gnu"))]
fn download_and_build_lz4() {
    use std::path::PathBuf;

    static LZ4_ZIP: &'static str =
        "https://github.com/lz4/lz4/releases/download/v1.9.0/lz4_v1_9_0_win64.zip";
    static LZ4_NAME: &'static str = "lz4-1.9.0";
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let lz4_lib_dir = out_dir.join(LZ4_NAME);
    let lz4_lib_file = lz4_lib_dir.join("liblz4_static.lib");

    if !lz4_lib_dir.exists() {
        fs::create_dir(&lz4_lib_dir).unwrap();
    }

    if !lz4_lib_file.exists() {
        let mut tmpfile = tempfile::tempfile().unwrap();
        reqwest::get(LZ4_ZIP)
            .unwrap()
            .copy_to(&mut tmpfile)
            .unwrap();
        let mut zip = zip::ZipArchive::new(tmpfile).unwrap();
        let mut lib = zip.by_name("static/liblz4_static.lib").unwrap();
        let mut liblz4_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&lz4_lib_file)
            .unwrap();
        io::copy(&mut lib, &mut liblz4_file).unwrap();
    }

    assert!(&lz4_lib_file.exists(), "lz4 lib was not created");

    println!("cargo:rustc-link-search=native={}", lz4_lib_dir.display());
    println!("cargo:rustc-link-lib=static=liblz4_static");
}

// This downloads function and builds the libsodium from source for linux and
// unix targets.
// The steps are taken from the libsodium installation instructions:
// https://libsodium.gitbook.io/doc/installation
// effectively:
// $ ./configure
// $ make && make check
// $ sudo make install
#[cfg(all(feature = "libsodium-bundled", not(target_os = "windows")))]
fn download_and_install_libsodium() {
    use libflate::non_blocking::gzip::Decoder;
    use std::fs::File;
    use std::io::{stderr, stdout, Write};
    use std::path::PathBuf;
    use std::process::Command;
    use tar::Archive;

    static LIBSODIUM_URL: &'static str =
        "https://download.libsodium.org/libsodium/releases";
    static LIBSODIUM_NAME: &'static str = "libsodium-1.0.17";

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let install_dir = out_dir.join("libsodium_install");
    let source_dir = install_dir.join(LIBSODIUM_NAME);
    let prefix_dir = out_dir.join("libsodium");
    let sodium_lib_dir = prefix_dir.join("lib");

    if !install_dir.exists() {
        let tmpdir = tempfile::tempdir().unwrap();

        // download source code file
        let src_file_name = format!("{}.tar.gz", LIBSODIUM_NAME);
        let url = format!("{}/{}", LIBSODIUM_URL, src_file_name);
        let src_file_path = tmpdir.path().join(&src_file_name);
        let mut file = File::create(&src_file_path).unwrap();
        reqwest::get(&url).unwrap().copy_to(&mut file).unwrap();

        // download signature file
        let sig_file_name = format!("{}.tar.gz.sig", LIBSODIUM_NAME);
        let url = format!("{}/{}", LIBSODIUM_URL, sig_file_name);
        let sig_file_path = tmpdir.path().join(&sig_file_name);
        let mut file = File::create(sig_file_path).unwrap();
        reqwest::get(&url).unwrap().copy_to(&mut file).unwrap();

        // import libsodium author's public key
        let output = Command::new("gpg")
            .arg("--import")
            .arg("libsodium.gpg.key")
            .output()
            .expect("failed to import libsodium author's gpg key");
        stdout().write_all(&output.stdout).unwrap();
        stderr().write_all(&output.stderr).unwrap();

        // verify signature
        let output = Command::new("gpg")
            .current_dir(tmpdir.path())
            .arg("--verify")
            .arg(&sig_file_name)
            .output()
            .expect("failed to verify libsodium file");
        stdout().write_all(&output.stdout).unwrap();
        stderr().write_all(&output.stderr).unwrap();

        // unpack source code files
        let decoder = Decoder::new(File::open(&src_file_path).unwrap());
        let mut ar = Archive::new(decoder);
        ar.unpack(&install_dir).unwrap();
    }

    if !sodium_lib_dir.exists() {
        let configure = source_dir.join("./configure");
        let output = Command::new(&configure)
            .current_dir(&source_dir)
            .args(&[std::path::Path::new("--prefix"), &prefix_dir])
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
