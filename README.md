Zbox
======
Zbox is a zero-knowledge, privacy focused embeddable file system. Its goal is
to help application store files securely, privately and reliably. By
encapsulating files and directories into an encrypted repository, it can provide
exclusive access to the authorised application.

Unlike other system-level file systems, such as ext4, XFS and btrfs, which
provide shared access to multiple processes, Zbox is an user-space file system
and runs in the same memory space as the application. It provides access to
only one process at a time.

To protect privacy and minimise data exposure, Zbox intentionally does not
support [FUSE](https://github.com/libfuse/libfuse).

Features
========
- Everything is encrypted, including metadata and directory structure, no
  knowledge is leaked to underneath storage
- State-of-the-art cryptography: AES-256-GCM (hardware), ChaCha20-Poly1305,
  Argon2 password hashing and etc., empowered by [libsodium]
  (https://libsodium.org/)
- Content-based data chunk deduplication and file-based deduplication
- Data compression using [LZ4](http://www.lz4.org) in fast mode
- Data integrity is guranteed by authenticated encryption primitives
- File content Revision history
- Copy-on-write (COW) semantics
- ACID transactional operations
- Snapshot
- Append-only content data on storage
- Support multiple storages, including memory, OS file system, RDBMS (incoming),
  Key-value object store (incoming) and more
- Build in love with Rust

Disclaimer
==========
`Zbox` is under active development, we are not responsible for any data loss
or leak caused by it. Use it at your own risk.

How to use
==========
For reference documentation, please visit [documentation](https://docs.rs/zbox).

Requirements
------------
- [Rust](https://www.rust-lang.org/) stable >= 1.21
- [libsodium](https://libsodium.org/) >= 1.0.15

Supported Platforms
-------------------
- 64-bit Debian or Ubuntu Linux
- 64-bit macOS

32-bit OS and Windows is not supported yet.

Usage
------
Add the following dependency to your `Cargo.toml`:

```toml
[dependencies]
zbox = "0.1"
```

Example
-------
```rust
extern crate zbox;

use zbox::{zbox_init, RepoOpener};

fn main() {
    // initialise zbox environment, only called once
    zbox_init();

    // repository path on your OS file system
    let repo_uri = "file://./hello_world_repo";

    // password of your repository
    let pwd = "your secret";

    // create the repository
    let mut repo = RepoOpener::new()
        .create(true)
        .open(&repo_uri, &pwd)
        .unwrap();
}
```

License
=======
`Zbox` is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE)
file for details.

