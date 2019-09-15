extern crate cc;
extern crate pkg_config;

#[cfg(target_os = "windows")]
extern crate libflate;
#[cfg(target_os = "windows")]
extern crate reqwest;
#[cfg(target_os = "windows")]
extern crate tar;

use std::env;
use std::path::PathBuf;
#[cfg(feature = "libsodium-bundled")]
use std::process::Command;

#[cfg(all(feature = "libsodium-bundled", not(target_os = "windows")))]
const LIBSODIUM_NAME: &'static str = "libsodium-1.0.17";
#[cfg(all(feature = "libsodium-bundled", not(target_os = "windows")))]
const LIBSODIUM_URL: &'static str =
    "https://download.libsodium.org/libsodium/releases/libsodium-1.0.17.tar.gz";

// skip the build script when building doc on docs.rs
#[cfg(feature = "docs-rs")]
fn main() {}

#[cfg(not(feature = "docs-rs"))]
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
        // build lz4 static library
        let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
        if !out_dir.join("liblz4.a").exists() {
            let mut compiler = cc::Build::new();
            compiler
                .file("vendor/lz4/lz4.c")
                .file("vendor/lz4/lz4frame.c")
                .file("vendor/lz4/lz4hc.c")
                .file("vendor/lz4/xxhash.c")
                .define("XXH_NAMESPACE", "LZ4_")
                .opt_level(3)
                .debug(false)
                .pic(true)
                .shared_flag(false);
            if !cfg!(windows) {
                compiler.static_flag(true);
            }
            compiler.compile("liblz4.a");
        }
    }
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
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let source_dir = out_dir.join(LIBSODIUM_NAME);
    let prefix_dir = out_dir.join("libsodium");
    let sodium_lib_dir = prefix_dir.join("lib");
    let src_file_name = format!("{}.tar.gz", LIBSODIUM_NAME);

    // check if command tools exist
    Command::new("curl")
        .arg("--version")
        .output()
        .expect("curl not found");
    Command::new("tar")
        .arg("--version")
        .output()
        .expect("tar not found");
    Command::new("gpg")
        .arg("--version")
        .output()
        .expect("gpg not found");
    Command::new("make")
        .arg("--version")
        .output()
        .expect("make not found");

    if !source_dir.exists() {
        // download source code file
        let output = Command::new("curl")
            .current_dir(&out_dir)
            .args(&[LIBSODIUM_URL, "-sSfL", "-o", &src_file_name])
            .output()
            .expect("failed to download libsodium");
        assert!(output.status.success());

        // download signature file
        let sig_file_name = format!("{}.sig", src_file_name);
        let sig_url = format!("{}.sig", LIBSODIUM_URL);
        let output = Command::new("curl")
            .current_dir(&out_dir)
            .args(&[&sig_url, "-sSfL", "-o", &sig_file_name])
            .output()
            .expect("failed to download libsodium signature file");
        assert!(output.status.success());

        // import libsodium author's public key
        let output = Command::new("gpg")
            .arg("--import")
            .arg("libsodium.gpg.key")
            .output()
            .expect("failed to import libsodium author's gpg key");
        assert!(output.status.success());

        // verify signature
        let output = Command::new("gpg")
            .current_dir(&out_dir)
            .arg("--verify")
            .arg(&sig_file_name)
            .output()
            .expect("failed to verify libsodium file");
        assert!(output.status.success());

        // unpack source code files
        let output = Command::new("tar")
            .current_dir(&out_dir)
            .args(&["zxf", &src_file_name])
            .output()
            .expect("failed to unpack libsodium");
        assert!(output.status.success());
    }

    if !sodium_lib_dir.exists() {
        let configure = source_dir.join("./configure");
        let output = Command::new(&configure)
            .current_dir(&source_dir)
            .args(&[std::path::Path::new("--prefix"), &prefix_dir])
            .output()
            .expect("failed to execute configure");
        assert!(output.status.success());

        let output = Command::new("make")
            .current_dir(&source_dir)
            .output()
            .expect("failed to execute make");
        assert!(output.status.success());

        let output = Command::new("make")
            .current_dir(&source_dir)
            .arg("install")
            .output()
            .expect("failed to execute make install");
        assert!(output.status.success());
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
            .by_name("x64/Release/v141/static/libsodium.lib")
            .unwrap();
        #[cfg(target_arch = "x86")]
        let mut lib = zip
            .by_name("Win32/Release/v141/static/libsodium.lib")
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
