extern crate pkg_config;

use std::env;

fn main() {
    println!("cargo:rerun-if-env-changed=SODIUM_LIB_DIR");
    println!("cargo:rerun-if-env-changed=SODIUM_STATIC");

    // add libsodium link options
    if let Ok(lib_dir) = env::var("SODIUM_LIB_DIR") {
        println!("cargo:rustc-link-search=native={}", lib_dir);
        let mode = match env::var_os("SODIUM_STATIC") {
            Some(_) => "static",
            None => "dylib",
        };
        println!("cargo:rustc-link-lib={0}=sodium", mode);
    } else {
        pkg_config::Config::new()
            .atleast_version("1.0.11")
            .statik(true)
            .probe("libsodium")
            .unwrap();
    }
}
