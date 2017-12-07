<img src="https://www.zbox.io/svg/logo.svg" alt="Zbox Logo" height="96" /> Zbox
======
[![Travis](https://img.shields.io/travis/zboxfs/zbox.svg?style=flat-square)](https://travis-ci.org/zboxfs/zbox)
[![Crates.io](https://img.shields.io/crates/d/zbox.svg?style=flat-square)](https://crates.io/crates/zbox)
[![Crates.io](https://img.shields.io/crates/v/zbox.svg?style=flat-square)](https://crates.io/crates/zbox)
[![GitHub last commit](https://img.shields.io/github/last-commit/zboxfs/zbox.svg?style=flat-square)](https://github.com/zboxfs/zbox)
[![license](https://img.shields.io/github/license/zboxfs/zbox.svg?style=flat-square)](https://github.com/zboxfs/zbox)
[![GitHub stars](https://img.shields.io/github/stars/zboxfs/zbox.svg?style=social&label=Stars)](https://github.com/zboxfs/zbox)

Zbox is a zero-details, privacy-focused embeddable file system. Its goal is
to help application store files securely, privately and reliably. By
encapsulating files and directories into an encrypted repository, it provides
a virtual file system and exclusive access to authorised application.

Unlike other system-level file systems, such as [ext4], [XFS] and [Btrfs], which
provide shared access to multiple processes, Zbox is a file system that runs
in the same memory space as the application. It only provides access to one
process at a time.

By abstracting IO access, Zbox supports a variety of underlying storage layers.
Memory and OS file system are supported now, RDBMS and key-value object store
supports are coming soon.

## Disclaimer

Zbox is under active development, we are not responsible for any data loss
or leak caused by using it. Always back up your files and use at your own risk!

Features
========
- Everything is encrypted :lock:, including metadata and directory structure,
  no knowledge can be leaked to underlying storage
- State-of-the-art cryptography: AES-256-GCM (hardware), XChaCha20-Poly1305,
  Argon2 password hashing and etc., empowered by [libsodium]
- Content-based data chunk deduplication and file-based deduplication
- Data compression using [LZ4] in fast mode
- Data integrity is guaranteed by authenticated encryption primitives (AEAD
  crypto)
- File contents versioning
- Copy-on-write (COW :cow:) semantics
- ACID transactional operations
- Snapshot :camera:
- Support multiple storages, including memory, OS file system, RDBMS (coming
  soon), Key-value object store (coming soon) and more
- Built with [Rust] :hearts:

## Comparison

Many OS-level file systems support encryption, such as [EncFS], [APFS] and
[ZFS]. Some disk encryption tools also provide virtual file system, such as
[TrueCrypt] and [VeraCrypt].

This diagram shows the difference between Zbox and them.

![Comparison](https://www.zbox.io/svg/zbox-compare.svg)

Below is the feature comparison list.

|                             | Zbox                     | OS-level File Systems    | Disk Encryption Tools    |
| --------------------------- | ------------------------ | ------------------------ | ------------------------ |
| Encrypts file contents      | :heavy_check_mark:       | partial                  | :heavy_check_mark:       |
| Encrypts file metadata      | :heavy_check_mark:       | partial                  | :heavy_check_mark:       |
| Encrypts directory          | :heavy_check_mark:       | partial                  | :heavy_check_mark:       |
| Data integrity              | :heavy_check_mark:       | partial                  | :heavy_multiplication_x: |
| Shared access for processes | :heavy_multiplication_x: | :heavy_check_mark:       | :heavy_check_mark:       |
| Deduplication               | :heavy_check_mark:       | :heavy_multiplication_x: | :heavy_multiplication_x: |
| Compression                 | :heavy_check_mark:       | partial                  | :heavy_multiplication_x: |
| COW semantics               | :heavy_check_mark:       | partial                  | :heavy_multiplication_x: |
| ACID Transaction            | :heavy_check_mark:       | :heavy_multiplication_x: | :heavy_multiplication_x: |
| Multiple storage layers     | :heavy_check_mark:       | :heavy_multiplication_x: | :heavy_multiplication_x: |
| API access                  | :heavy_check_mark:       | through VFS              | through VFS              |
| Symbolic links              | :heavy_multiplication_x: | :heavy_check_mark:       | depends on inner FS      |
| Users and permissions       | :heavy_multiplication_x: | :heavy_check_mark:       | :heavy_check_mark:       |
| FUSE support                | :heavy_multiplication_x: | :heavy_check_mark:       | :heavy_check_mark:       |
| Linux and macOS support     | :heavy_check_mark:       | :heavy_check_mark:       | :heavy_check_mark:       |
| Windows support             | :heavy_multiplication_x: | partial                  | :heavy_check_mark:       |

How to use
==========
For reference documentation, please visit [documentation](https://docs.rs/zbox).

## Requirements

- [Rust] stable >= 1.21
- [libsodium] >= 1.0.15

## Supported Platforms

- 64-bit Debian-based Linux, such as Ubuntu
- 64-bit macOS

32-bit OS and Windows are `NOT` supported yet.

## Usage

Add the following dependency to your `Cargo.toml`:

```toml
[dependencies]
zbox = "~0.1"
```

## Example

```rust
extern crate zbox;

use std::io::{Read, Write};
use zbox::{init_env, RepoOpener, OpenOptions};

fn main() {
    // initialise zbox environment, called first
    init_env();

    // create and open a repository in current OS directory
    let mut repo = RepoOpener::new()
        .create(true)
        .open("file://./my_repo", "your password")
        .unwrap();

    // create and open a file in repository for writing
    let mut file = OpenOptions::new()
        .create(true)
        .open(&mut repo, "/my_file.txt")
        .unwrap();

    // use std::io::Write trait to write data into it
    file.write_all(b"Hello, world!").unwrap();

    // finish writting to make a permanent version of content
    file.finish().unwrap();

    // read file content using std::io::Read trait
    let mut content = String::new();
    file.read_to_string(&mut content).unwrap();
    assert_eq!(content, "Hello, world!");
}
```

## Build with Docker

Zbox comes with Docker support, it is based on [rust:latest] and [libsodium] is
included. Check the [Dockerfile](Dockerfile) for the details.

First, we build the Docker image which can be used to compile Zbox, run below
commands from Zbox project folder.
```bash
docker build --force-rm -t zbox ./
```

After the Docker image is built, we can use it to build Zbox.
```bash
docker run --rm -v $PWD:/zbox zbox cargo build
```

Or run the test suite.
```bash
docker run --rm -v $PWD:/zbox zbox cargo test
```

Contribution
============

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall
be licensed as above, without any additional terms of conditions.

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of
conduct, and the process for submitting pull requests to us.

Community
=========

- [Gitter Chat Room](https://gitter.im/zboxfs/zbox)
- [Twitter](https://twitter.com/ZboxFS)

License
=======
`Zbox` is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE)
file for details.

[ext4]: https://en.wikipedia.org/wiki/Ext4
[xfs]: http://xfs.org
[btrfs]: https://btrfs.wiki.kernel.org
[Rust]: https://www.rust-lang.org
[libsodium]: https://libsodium.org
[LZ4]: http://www.lz4.org
[EncFS]: https://vgough.github.io/encfs/
[APFS]: https://en.wikipedia.org/wiki/Apple_File_System
[ZFS]: https://en.wikipedia.org/wiki/ZFS
[TrueCrypt]: http://truecrypt.sourceforge.net
[VeraCrypt]: https://veracrypt.codeplex.com
[rust:latest]: https://hub.docker.com/_/rust/
