<img src="https://zboxfs.github.io/zbox/images/logo.svg" alt="ZboxFS Logo" height="96" /> ZboxFS
======
[![GitHub action](https://github.com/zboxfs/zbox/workflows/build/badge.svg)](https://github.com/zboxfs/zbox/actions)
[![Crates.io](https://img.shields.io/crates/d/zbox.svg)](https://crates.io/crates/zbox)
[![Crates.io](https://img.shields.io/crates/v/zbox.svg)](https://crates.io/crates/zbox)
[![GitHub last commit](https://img.shields.io/github/last-commit/zboxfs/zbox.svg)](https://github.com/zboxfs/zbox)
[![license](https://img.shields.io/github/license/zboxfs/zbox.svg)](https://github.com/zboxfs/zbox)
[![GitHub stars](https://img.shields.io/github/stars/zboxfs/zbox.svg?style=social&label=Stars)](https://github.com/zboxfs/zbox)

ZboxFS is a zero-details, privacy-focused in-app file system. Its goal is
to help application store files securely, privately and reliably. By
encapsulating files and directories into an encrypted repository, it provides
a virtual file system and exclusive access to authorised application.

Unlike other system-level file systems, such as [ext4], [XFS] and [Btrfs], which
provide shared access to multiple processes, ZboxFS is a file system that runs
in the same memory space as the application. It provides access to only one
process at a time.

By abstracting IO access, ZboxFS supports a variety of underlying storage layers,
including memory, OS file system, RDBMS and key-value object store.

## Disclaimer

ZboxFS is under active development, we are not responsible for any data loss
or leak caused by using it. Always back up your files and use at your own risk!

Features
========
- Everything is encrypted :lock:, including metadata and directory structure,
  no knowledge can be leaked to underlying storage
- State-of-the-art cryptography: AES-256-GCM (hardware), XChaCha20-Poly1305,
  Argon2 password hashing and etc., powered by [libsodium]
- Support varieties of underlying storages, including memory, OS file system,
  RDBMS, Key-value object store and more
- Files and directories are packed into same-sized blocks to eliminate metadata
  leakage
- Content-based data chunk deduplication and file-based deduplication
- Data compression using [LZ4] in fast mode, optional
- Data integrity is guaranteed by authenticated encryption primitives (AEAD
  crypto)
- File contents versioning
- Copy-on-write (COW :cow:) semantics
- ACID transactional operations
- Built with [Rust] :hearts:

## Comparison

Many OS-level file systems support encryption, such as [EncFS], [APFS] and
[ZFS]. Some disk encryption tools also provide virtual file system, such as
[TrueCrypt], [LUKS] and [VeraCrypt].

This diagram shows the difference between ZboxFS and them.

![Comparison](https://zboxfs.github.io/zbox/images/zbox-compare.svg)

Below is the feature comparison list.

|                             | ZboxFS                   | OS-level File Systems    | Disk Encryption Tools    |
| --------------------------- | ------------------------ | ------------------------ | ------------------------ |
| Encrypts file contents      | :heavy_check_mark:       | partial                  | :heavy_check_mark:       |
| Encrypts file metadata      | :heavy_check_mark:       | partial                  | :heavy_check_mark:       |
| Encrypts directory          | :heavy_check_mark:       | partial                  | :heavy_check_mark:       |
| Data integrity              | :heavy_check_mark:       | partial                  | :heavy_multiplication_x: |
| Shared access for processes | :heavy_multiplication_x: | :heavy_check_mark:       | :heavy_check_mark:       |
| Deduplication               | :heavy_check_mark:       | :heavy_multiplication_x: | :heavy_multiplication_x: |
| Compression                 | :heavy_check_mark:       | partial                  | :heavy_multiplication_x: |
| Content versioning          | :heavy_check_mark:       | :heavy_multiplication_x: | :heavy_multiplication_x: |
| COW semantics               | :heavy_check_mark:       | partial                  | :heavy_multiplication_x: |
| ACID Transaction            | :heavy_check_mark:       | :heavy_multiplication_x: | :heavy_multiplication_x: |
| Varieties of storages           | :heavy_check_mark:       | :heavy_multiplication_x: | :heavy_multiplication_x: |
| API access                  | :heavy_check_mark:       | through VFS              | through VFS              |
| Symbolic links              | :heavy_multiplication_x: | :heavy_check_mark:       | depends on inner FS      |
| Users and permissions       | :heavy_multiplication_x: | :heavy_check_mark:       | :heavy_check_mark:       |
| FUSE support                | :heavy_multiplication_x: | :heavy_check_mark:       | :heavy_check_mark:       |
| Linux and macOS support     | :heavy_check_mark:       | :heavy_check_mark:       | :heavy_check_mark:       |
| Windows support             | :heavy_check_mark:       | partial                  | :heavy_check_mark:       |

## Supported Storage

ZboxFS supports a variety of underlying storages. Memory storage is enabled by
default. All the other storages can be enabled individually by specifying its
corresponding Cargo feature when building ZboxFS.

| Storage            | URI identifier  | Cargo Feature       |
| ------------------ | --------------- | ------------------- |
| Memory             | "mem://"        | N/A                 |
| OS file system     | "file://"       | storage-file        |
| SQLite             | "sqlite://"     | storage-sqlite      |
| Redis              | "redis://"      | storage-redis       |
| Zbox Cloud Storage | "zbox://"       | storage-zbox-native |

\* Visit [zbox.io](https://zbox.io) to learn more about Zbox Cloud Storage.

## Specs

| Algorithm and data structure         | Value                             |
| ------------------------------------ | --------------------------------- |
| Authenticated encryption             | AES-256-GCM or XChaCha20-Poly1305 |
| Password hashing                     | Argon2                            |
| Key derivation                       | BLAKE2B                           |
| Content dedup                        | Rabin rolling hash                |
| File dedup                           | Merkle tree                       |
| Index structure                      | Log-structured merge-tree         |
| Compression                          | LZ4 in fast mode                  |

### Limits

| Limit                                     | Value                        |
| ----------------------------------------- | ---------------------------- |
| Data block size                           | 8 KiB                        |
| Maximum encryption frame size             | 128 KiB                      |
| Super block size                          | 8 KiB                        |
| Maximum filename length                   | No limit                     |
| Allowable characters in directory entries | Any UTF-8 character except / |
| Maximum pathname length                   | No limit                     |
| Maximum file size                         | 16 EiB                       |
| Maximum repo size                         | 16 EiB                       |
| Max number of files                       | No limit                     |

### Metadata

| Metadata                                  | Value                        |
| ----------------------------------------- | ---------------------------- |
| Stores file owner                         | No                           |
| POSIX file permissions                    | No                           |
| Creation timestamps                       | Yes                          |
| Last access / read timestamps             | No                           |
| Last change timestamps                    | Yes                          |
| Access control lists                      | No                           |
| Security                                  | Integrated with crypto       |
| Extended attributes                       | No                           |

### Capabilities

| Capability                                | Value                        |
| ----------------------------------------- | ---------------------------- |
| Hard links                                | No                           |
| Symbolic links                            | No                           |
| Case-sensitive                            | Yes                          |
| Case-preserving                           | Yes                          |
| File Change Log                           | By content versioning        |
| Filesystem-level encryption               | Yes                          |
| Data deduplication                        | Yes                          |
| Data checksums                            | Integrated with crypto       |
| Offline grow                              | No                           |
| Online grow                               | Auto                         |
| Offline shrink                            | No                           |
| Online shrink                             | Auto                         |

### Allocation and layout policies

| Feature                     | Value                             |
| --------------------------- | --------------------------------- |
| Address allocation scheme   | Append-only, linear address space |
| Sparse files                | No                                |
| Transparent compression     | Yes                               |
| Extents                     | No                                |
| Copy on write               | Yes                               |

### Storage fragmentation

| Fragmentation                | Value                        |
| ---------------------------- | ---------------------------- |
| Memory storage               | No                           |
| File storage                 | fragment unit size < 32 MiB  |
| RDBMS storage                | No                           |
| Key-value storage            | No                           |
| Zbox cloud storage           | fragment unit size < 128 KiB |

How to use
==========
For reference documentation, please visit [documentation](https://docs.rs/zbox).

## Requirements

- [Rust] stable >= 1.38
- [libsodium] >= 1.0.17

## Supported Platforms

- 64-bit Debian-based Linux, such as Ubuntu
- 64-bit macOS
- 64-bit Windows
- 64-bit Android, API level >= 21

32-bit and other OS are `NOT` supported yet.

## Usage

Add the following dependency to your `Cargo.toml`:

```toml
[dependencies]
zbox = "0.9.2"
```

If you don't want to install libsodium by yourself, simply specify
`libsodium-bundled` feature in dependency, which will automatically download,
verify and build libsodium.

```toml
[dependencies]
zbox = { version = "0.9.2", features = ["libsodium-bundled"] }
```

## Example

```rust
extern crate zbox;

use std::io::{Read, Write, Seek, SeekFrom};
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
    file.write_all(b"Hello, World!").unwrap();

    // finish writing to make a permanent content version
    file.finish().unwrap();

    // read file content using std::io::Read trait
    let mut content = String::new();
    file.seek(SeekFrom::Start(0)).unwrap();
    file.read_to_string(&mut content).unwrap();
    assert_eq!(content, "Hello, World!");
}
```

## Build with Docker

ZboxFS comes with [Docker] support, which made building ZboxFS easier. Check
each repo for more details.

- [zboxfs/base]
  Base image for building ZboxFS on Linux

- [zboxfs/wasm]
  Docker image for building WebAssembly binding

- [zboxfs/nodejs]
  Docker image for building Node.js binding

- [zboxfs/android]
  Docker image for building Android Java binding

## Static linking with libsodium

By default, ZboxFS uses dynamic linking when it is linked with libsodium. If you
want to change this behavior and use static linking, you can enable below two
environment variables.

On Linux/macOS,

```bash
export SODIUM_LIB_DIR=/path/to/your/libsodium/lib
export SODIUM_STATIC=true
```

On Windows,

```bash
set SODIUM_LIB_DIR=C:\path\to\your\libsodium\lib
set SODIUM_STATIC=true
```

And then re-build the code.

```bash
cargo build
```

Performance
============

The performance test is run on a Macbook Pro 2017 laptop with spec as below.

| Spec                    | Value                       |
| ----------------------- | --------------------------- |
| Processor Name:         | Intel Core i7               |
| Processor Speed:        | 3.5 GHz                     |
| Number of Processors:   | 1                           |
| Total Number of Cores:  | 2                           |
| L2 Cache (per Core):    | 256 KB                      |
| L3 Cache:               | 4 MB                        |
| Memory:                 | 16 GB                       |
| OS Version:             | macOS High Sierra 10.13.6   |

Test result:

|                               | Read            | Write          | TPS          |
| ----------------------------- | --------------- | -------------- | ------------ |
| Baseline (memcpy):            | 3658.23 MB/s    | 3658.23 MB/s   | N/A          |
| Baseline (file):              | 1307.97 MB/s    | 2206.30 MB/s   | N/A          |
| Memory storage (no compress): | 605.01 MB/s     | 186.20 MB/s    | 1783 tx/s    |
| Memory storage (compress):    | 505.04 MB/s     | 161.11 MB/s    | 1180 tx/s    |
| File storage (no compress):   | 445.28 MB/s     | 177.39 MB/s    | 313 tx/s     |
| File storage (compress):      | 415.85 MB/s     | 158.22 MB/s    | 325 tx/s     |

To run the performance test on your own computer, please follow the
instructions in [CONTRIBUTING.md](CONTRIBUTING.md#run-performance-test).

Contribution
============

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall
be licensed as above, without any additional terms of conditions.

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of
conduct, and the process for submitting pull requests to us.

Community
=========

- [Twitter](https://twitter.com/ZboxFS)

License
=======
`ZboxFS` is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE)
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
[LUKS]: https://gitlab.com/cryptsetup/cryptsetup/
[VeraCrypt]: https://veracrypt.codeplex.com
[Docker]: https://www.docker.com
[zboxfs/base]: https://github.com/zboxfs/zbox-docker-base
[zboxfs/wasm]: https://github.com/zboxfs/zbox-docker-wasm
[zboxfs/nodejs]: https://github.com/zboxfs/zbox-docker-nodejs
[zboxfs/android]: https://github.com/zboxfs/zbox-android
