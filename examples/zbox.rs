//! This example is to demonstrate basic usage of Zbox Cloud Storage. The
//! storage for local cache is memory-based,
//!
//! To run this example, firstly create a test repo on https://zbox.io/try/ and
//! use its URI in below code, then use the command below to run this example:
//!
//! $ cargo run --example zbox --features storage-zbox-native

extern crate zbox;

use std::io::Read;
use zbox::{init_env, OpenOptions, RepoOpener};

fn main() {
    // initialise zbox environment, called first
    init_env();

    // create and open a repository
    // Note: replace the repo URI below with yours
    let mut repo = RepoOpener::new()
        .create(true)
        .open("zbox://mcA4LKLT4mtSxHdSTptcmwHw@QDWYbndSEzPWrw", "pwd")
        .unwrap();

    // display repo information
    let info = repo.info().unwrap();
    dbg!(info);

    let filename = "/file";
    let buf = [1u8, 2u8, 3u8];
    let buf2 = [4u8, 5u8, 6u8, 7u8];

    // create a file with version enabled and write data to it
    {
        let mut f = OpenOptions::new()
            .version_limit(5)
            .create(true)
            .open(&mut repo, &filename)
            .unwrap();
        f.write_once(&buf[..]).unwrap();
    }

    // write another version of content to the file
    {
        let mut f = OpenOptions::new()
            .write(true)
            .open(&mut repo, &filename)
            .unwrap();
        f.write_once(&buf2[..]).unwrap();
    }

    // read latest file content and display file history
    {
        let mut f = OpenOptions::new().open(&mut repo, &filename).unwrap();
        let mut content = Vec::new();
        f.read_to_end(&mut content).unwrap();
        dbg!(content);

        let hist = f.history().unwrap();
        dbg!(hist);
    }
}
