use std::fmt::{self, Debug};
use std::io::SeekFrom;
use std::path::Path;
use std::time::SystemTime;

use super::{File, Result};
use base::crypto::{Cipher, Cost, MemLimit, OpsLimit};
use base::{self, Time};
use error::Error;
use fs::{Config, DirEntry, FileType, Fs, Metadata, Options, Version};
use trans::Eid;

/// A builder used to create a repository [`Repo`] in various manners.
///
/// This builder exposes the ability to configure how a [`Repo`] is opened and
/// what operations are permitted on the opened repository.
///
/// Generally speaking, when using `RepoOpener`, you'll first call [`new`], then
/// chain calls to methods to set each option, then call [`open`], passing the
/// URI of the repository and password you're trying to open. This will give
/// you a [`Result`] with a [`Repo`] inside that you can further operate on.
///
/// # Examples
///
/// Opening a repository and creating it if it doesn't exist.
///
/// ```
/// # #![allow(unused_mut, unused_variables)]
/// # use zbox::{init_env, Result};
/// use zbox::RepoOpener;
///
/// # fn foo() -> Result<()> {
/// # init_env();
/// let mut repo = RepoOpener::new().create(true).open("mem://foo", "pwd")?;
/// # Ok(())
/// # }
/// # foo().unwrap();
/// ```
///
/// Specify options for creating a repository.
///
/// ```
/// # #![allow(unused_mut, unused_variables)]
/// # use zbox::{init_env, Result};
/// use zbox::{RepoOpener, OpsLimit, MemLimit, Cipher};
///
/// # fn foo() -> Result<()> {
/// # init_env();
/// let mut repo = RepoOpener::new()
///     .ops_limit(OpsLimit::Moderate)
///     .mem_limit(MemLimit::Interactive)
///     .cipher(Cipher::Xchacha)
///     .create(true)
///     .open("mem://foo", "pwd")?;
/// # Ok(())
/// # }
/// # foo().unwrap();
/// ```
///
/// [`Repo`]: struct.Repo.html
/// [`new`]: struct.RepoOpener.html#method.new
/// [`open`]: struct.RepoOpener.html#method.open
/// [`Result`]: type.Result.html
#[derive(Debug, Clone, Default)]
pub struct RepoOpener {
    cfg: Config,
    create: bool,
    create_new: bool,
    read_only: bool,
    force: bool,
}

impl RepoOpener {
    /// Creates a blank new set of options ready for configuration.
    #[inline]
    pub fn new() -> Self {
        RepoOpener::default()
    }

    /// Sets the password hash operation limit.
    ///
    /// This option is only used for creating a repository.
    /// `OpsLimit::Interactive` is the default.
    pub fn ops_limit(&mut self, ops_limit: OpsLimit) -> &mut Self {
        self.cfg.cost.ops_limit = ops_limit;
        self
    }

    /// Sets the password hash memory limit.
    ///
    /// This option is only used for creating a repository.
    /// `MemLimit::Interactive` is the default.
    pub fn mem_limit(&mut self, mem_limit: MemLimit) -> &mut Self {
        self.cfg.cost.mem_limit = mem_limit;
        self
    }

    /// Sets the crypto cipher encrypts the repository.
    ///
    /// This option is only used for creating a repository. `Cipher::Aes` is
    /// the default if CPU supports AES-NI instructions, otherwise it will fall
    /// back to `Cipher::Xchacha`.
    pub fn cipher(&mut self, cipher: Cipher) -> &mut Self {
        self.cfg.cipher = cipher;
        self
    }

    /// Sets the option for creating a new repository.
    ///
    /// This option indicates whether a new repository will be created if the
    /// repository does not yet already exist.
    pub fn create(&mut self, create: bool) -> &mut Self {
        self.create = create;
        self
    }

    /// Sets the option to always create a new repository.
    ///
    /// This option indicates whether a new repository will be created. No
    /// repository is allowed to exist at the target path.
    pub fn create_new(&mut self, create_new: bool) -> &mut Self {
        self.create_new = create_new;
        if create_new {
            self.create = true;
        }
        self
    }

    /// Sets the option for data compression.
    ///
    /// This options indicates whether the LZ4 compression should be used in
    /// the repository. Default is false.
    pub fn compress(&mut self, compress: bool) -> &mut Self {
        self.cfg.compress = compress;
        self
    }

    /// Sets the default maximum number of file version.
    ///
    /// The `version_limit` must be within [1, 255], default is 1. This
    /// setting is a repository-wise setting, individual file can overwrite it
    /// by setting [`version_limit`] in [`OpenOptions`].
    ///
    /// [`version_limit`]: struct.OpenOptions.html#method.version_limit
    /// [`OpenOptions`]: struct.OpenOptions.html
    pub fn version_limit(&mut self, version_limit: u8) -> &mut Self {
        self.cfg.opts.version_limit = version_limit;
        self
    }

    /// Sets the default option for file data chunk deduplication.
    ///
    /// This option indicates whether data chunk should be deduped when
    /// writing data to a file. This setting is a repository-wise setting,
    /// individual file can overwrite it by setting [`dedup_chunk`]
    /// in [`OpenOptions`]. Default is false.
    ///
    /// [`dedup_chunk`]: struct.OpenOptions.html#method.dedup_chunk
    /// [`OpenOptions`]: struct.OpenOptions.html
    pub fn dedup_chunk(&mut self, dedup_chunk: bool) -> &mut Self {
        self.cfg.opts.dedup_chunk = dedup_chunk;
        self
    }

    /// Sets the option for read-only mode.
    ///
    /// This option cannot be true with either `create` or `create_new` is true.
    pub fn read_only(&mut self, read_only: bool) -> &mut Self {
        self.read_only = read_only;
        self
    }

    /// Sets the option to open repo regardless repo lock.
    ///
    /// Normally, repo will be exclusively locked once it is opened. But when
    /// this option is set to true, the repo will be opened regardless the repo
    /// lock. This option breaks exclusive access to repo, so use it cautiously.
    /// Default is false.
    pub fn force(&mut self, force: bool) -> &mut Self {
        self.force = force;
        self
    }

    /// Opens a repository at URI with the password and options specified by
    /// `self`.
    ///
    /// In general, the URI is structured as follows:
    ///
    /// ```notrust
    /// storage://username:password@/path/data?key=value&key2=value2
    /// |------| |-----------------||---------||-------------------|
    ///     |             |              |                |
    /// identifier    authority         path          parameters
    /// ```
    ///
    /// Only `identifier` and `path` are required, all the others are optional.
    ///
    /// Supported storage:
    ///
    /// - Memory storage, URI identifier is `mem://`
    ///
    ///   After the identifier is a name to distinguish a particular memory
    ///   storage location.
    ///
    ///   For example, `mem://foobar`.
    ///
    /// - OS file system storage, URI identifier is `file://`
    ///
    ///   After the identifier is the path to a directory on OS file system. It can
    ///   be a relative or absolute path.
    ///
    ///   For example, `file://./foo/bar`.
    ///
    ///   This storage must be enabled by Cargo feature `storage-file`.
    ///
    /// - SQLite storage, URI identifier is `sqlite://`
    ///
    ///   After the identifier is the path to a SQLite database file. It can also
    ///   be a in-memory SQLite database, that is, the path can be ":memory:".
    ///
    ///   For example, `sqlite://./foobar.sqlite`.
    ///
    ///   This storage must be enabled by Cargo feature `storage-sqlite`.
    ///
    /// - Redis storage, URI identifier is `redis://`
    ///
    ///   After the identifier is the path to a Redis instance. Unix socket is
    ///   supported. The URI format is:
    ///
    ///   `redis://[+unix+][:<passwd>@]<hostname>[:port][/<db>]`
    ///
    ///   This storage must be enabled by Cargo feature `storage-redis`.
    ///
    /// After a repository is opened, all of the other methods provided by
    /// ZboxFS will be thread-safe.
    ///
    /// Your application should destroy the password as soon as possible after
    /// calling this method.
    ///
    /// # Errors
    ///
    /// Open a memory based repository without enable `create` option will
    /// return an error.
    pub fn open(&self, uri: &str, pwd: &str) -> Result<Repo> {
        // version limit must be greater than 0
        if self.cfg.opts.version_limit == 0 {
            return Err(Error::InvalidArgument);
        }

        if self.create {
            if self.read_only {
                return Err(Error::InvalidArgument);
            }
            if Repo::exists(uri)? {
                if self.create_new {
                    return Err(Error::RepoExists);
                }
                Repo::open(uri, pwd, self.read_only, self.force)
            } else {
                Repo::create(uri, pwd, &self.cfg)
            }
        } else {
            Repo::open(uri, pwd, self.read_only, self.force)
        }
    }
}

/// Options and flags which can be used to configure how a file is opened.
///
/// This builder exposes the ability to configure how a [`File`] is opened and
/// what operations are permitted on the opened file. The [`Repo::open_file`]
/// and [`Repo::create_file`] methods are aliases for commonly used options
/// using this builder.
///
/// Generally speaking, when using `OpenOptions`, you'll first call [`new`], then
/// chain calls to methods to set each option, then call [`open`], passing the
/// path of the file you're trying to open. This will give you a [`Result`]
/// with a [`File`] inside that you can further operate on.
///
/// # Examples
///
/// Opening a file for both reading and writing, as well as creating it if it
/// doesn't exist.
///
/// ```
/// # #![allow(unused_mut, unused_variables)]
/// # use zbox::{init_env, Result, RepoOpener};
/// # use zbox::OpenOptions;
/// # fn foo() -> Result<()> {
/// # init_env();
/// # let mut repo = RepoOpener::new().create(true).open("mem://foo", "pwd")?;
/// let file = OpenOptions::new()
///     .read(true)
///     .write(true)
///     .create(true)
///     .open(&mut repo, "/foo.txt")?;
/// # Ok(())
/// # }
/// # foo().unwrap();
/// ```
///
/// [`File`]: struct.File.html
/// [`Repo::open_file`]: struct.Repo.html#method.open_file
/// [`Repo::create_file`]: struct.Repo.html#method.create_file
/// [`new`]: struct.OpenOptions.html#method.new
/// [`open`]: struct.OpenOptions.html#method.open
/// [`Result`]: type.Result.html
#[derive(Debug, Default)]
pub struct OpenOptions {
    read: bool,
    write: bool,
    append: bool,
    truncate: bool,
    create: bool,
    create_new: bool,
    version_limit: Option<u8>,
    dedup_chunk: Option<bool>,
}

impl OpenOptions {
    /// Creates a blank new set of options ready for configuration.
    ///
    /// All options are initially set to false, except for `read`.
    pub fn new() -> Self {
        let mut opt = Self::default();
        opt.read = true;
        opt
    }

    /// Sets the option for read access.
    pub fn read(&mut self, read: bool) -> &mut OpenOptions {
        self.read = read;
        self
    }

    /// Sets the option for write access.
    pub fn write(&mut self, write: bool) -> &mut OpenOptions {
        self.write = write;
        self
    }

    /// Sets the option for the append mode.
    ///
    /// This option, when true, means that writes will append to a file instead
    /// of overwriting previous content. Note that setting
    /// `.write(true).append(true)` has the same effect as setting only
    /// `.append(true)`.
    pub fn append(&mut self, append: bool) -> &mut OpenOptions {
        self.append = append;
        if append {
            self.write = true;
        }
        self
    }

    /// Sets the option for truncating a previous file.
    ///
    /// Note that setting `.write(true).truncate(true)` has the same effect as
    /// setting only `.truncate(true)`.
    pub fn truncate(&mut self, truncate: bool) -> &mut OpenOptions {
        self.truncate = truncate;
        if truncate {
            self.write = true;
        }
        self
    }

    /// Sets the option for creating a new file.
    ///
    /// This option indicates whether a new file will be created if the file
    /// does not yet already exist.
    pub fn create(&mut self, create: bool) -> &mut OpenOptions {
        self.create = create;
        if create {
            self.write = true;
        }
        self
    }

    /// Sets the option to always create a new file.
    ///
    /// This option indicates whether a new file will be created. No file is
    /// allowed to exist at the target location.
    pub fn create_new(&mut self, create_new: bool) -> &mut OpenOptions {
        self.create_new = create_new;
        if create_new {
            self.create = true;
            self.write = true;
        }
        self
    }

    /// Sets the maximum number of file versions allowed.
    ///
    /// The `version_limit` must be within [1, 255], default is 1. It will fall
    /// back to repository's [`version_limit`] if it is not set.
    ///
    /// [`version_limit`]: struct.RepoOpener.html#method.version_limit
    pub fn version_limit(&mut self, version_limit: u8) -> &mut OpenOptions {
        self.version_limit = Some(version_limit);
        self
    }

    /// Sets the option for file data chunk deduplication.
    ///
    /// This option indicates whether data chunk should be deduped when
    /// writing data to a file. It will fall back to repository's
    /// [`dedup_chunk`] if it is not set.
    ///
    /// [`dedup_chunk`]: struct.RepoOpener.html#method.dedup_chunk
    pub fn dedup_chunk(&mut self, dedup_chunk: bool) -> &mut OpenOptions {
        self.dedup_chunk = Some(dedup_chunk);
        self
    }

    /// Opens a file at path with the options specified by `self`.
    pub fn open<P: AsRef<Path>>(
        &self,
        repo: &mut Repo,
        path: P,
    ) -> Result<File> {
        // version limit must be greater than 0
        if let Some(version_limit) = self.version_limit {
            if version_limit == 0 {
                return Err(Error::InvalidArgument);
            }
        }
        open_file_with_options(&mut repo.fs, path, self)
    }
}

/// Information about a repository.
///
/// This structure is returned from the [`Repo::info`] represents known metadata
/// about a repository such as its volume ID, version, URI, creation times and
/// etc.
///
/// [`Repo::info`]: struct.Repo.html#method.info
#[derive(Debug)]
pub struct RepoInfo {
    volume_id: Eid,
    ver: base::Version,
    uri: String,
    cost: Cost,
    cipher: Cipher,
    compress: bool,
    version_limit: u8,
    dedup_chunk: bool,
    read_only: bool,
    ctime: Time,
}

impl RepoInfo {
    /// Returns the unique volume id of this repository.
    #[inline]
    pub fn volume_id(&self) -> &Eid {
        &self.volume_id
    }

    /// Returns repository version as string.
    ///
    /// This is the string representation of the repository version, for
    /// example, `0.6.0`.
    #[inline]
    pub fn version(&self) -> String {
        self.ver.to_string()
    }

    /// Returns the location URI string of this repository.
    #[inline]
    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// Returns the operation limit for repository password hash.
    #[inline]
    pub fn ops_limit(&self) -> OpsLimit {
        self.cost.ops_limit
    }

    /// Returns the memory limit for repository password hash
    #[inline]
    pub fn mem_limit(&self) -> MemLimit {
        self.cost.mem_limit
    }

    /// Returns repository password encryption cipher.
    #[inline]
    pub fn cipher(&self) -> Cipher {
        self.cipher
    }

    /// Returns whether compression is enabled.
    #[inline]
    pub fn compress(&self) -> bool {
        self.compress
    }

    /// Returns the default maximum number of file versions.
    #[inline]
    pub fn version_limit(&self) -> u8 {
        self.version_limit
    }

    /// Returns whether the file data chunk deduplication is enabled.
    #[inline]
    pub fn dedup_chunk(&self) -> bool {
        self.dedup_chunk
    }

    /// Returns whether this repository is read-only.
    #[inline]
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// Returns the creation time of this repository.
    #[inline]
    pub fn created_at(&self) -> SystemTime {
        self.ctime.to_system_time()
    }
}

// open a regular file with options
fn open_file_with_options<P: AsRef<Path>>(
    fs: &mut Fs,
    path: P,
    open_opts: &OpenOptions,
) -> Result<File> {
    if fs.is_read_only()
        && (open_opts.write
            || open_opts.append
            || open_opts.truncate
            || open_opts.create
            || open_opts.create_new)
    {
        return Err(Error::ReadOnly);
    }

    let path = path.as_ref();

    match fs.resolve(path) {
        Ok(_) => {
            if open_opts.create_new {
                return Err(Error::AlreadyExists);
            }
        }
        Err(ref err) if *err == Error::NotFound && open_opts.create => {
            let mut opts = fs.get_opts();
            if let Some(version_limit) = open_opts.version_limit {
                opts.version_limit = version_limit;
            }
            if let Some(dedup_chunk) = open_opts.dedup_chunk {
                opts.dedup_chunk = dedup_chunk;
            }
            fs.create_fnode(path, FileType::File, opts)?;
        }
        Err(err) => return Err(err),
    }

    let curr_len;
    let handle = fs.open_fnode(path)?;
    {
        let fnode = handle.fnode.read().unwrap();
        if fnode.is_dir() {
            return Err(Error::IsDir);
        }
        curr_len = fnode.curr_len();
    }

    let pos = if open_opts.append {
        SeekFrom::Start(curr_len as u64)
    } else {
        SeekFrom::Start(0)
    };
    let mut file = File::new(handle, pos, open_opts.read, open_opts.write);

    if open_opts.truncate && curr_len > 0 {
        file.set_len(0)?;
    }

    Ok(file)
}

/// An encrypted repository contains the whole file system.
///
/// A `Repo` represents a secure collection which consists of files,
/// directories and their associated data. Similar to [`std::fs`], `Repo`
/// provides methods to manipulate the enclosed file system.
///
/// # Storages
///
/// ZboxFS supports a variety of underlying storages, which are listed below.
///
/// | Storage            | URI identifier  | Cargo Feature       |
/// | ------------------ | --------------- | ------------------- |
/// | Memory             | "mem://"        | N/A                 |
/// | OS file system     | "file://"       | storage-file        |
/// | SQLite             | "sqlite://"     | storage-sqlite      |
/// | Redis              | "redis://"      | storage-redis       |
/// | Zbox Cloud Storage | "zbox://"       | storage-zbox-native |
///
/// \* Visit [zbox.io](https://zbox.io) to learn more about Zbox Cloud Storage.
///
/// By default, only memory storage is enabled. To use other storages, you need
/// to specify it as dependency features in your Cargo.toml.
///
/// For example, to use OS file as underlying storage, specify its feature in
/// your project's Cargo.toml file.
///
/// ```toml
/// [dependencies]
/// zbox = { version = "0.8.7", features = ["storage-file"] }
/// ```
///
/// # Create and open `Repo`
///
/// `Repo` can be created on different underlying storages using [`RepoOpener`].
/// It uses an URI-like string to specify its storage type and location. The
/// URI string starts with an identifier which specifies the storage type, as
/// shown in above table. You can check more location URI details at:
/// [RepoOpener](struct.RepoOpener.html#method.open).
///
/// `Repo` can only be opened once at a time. After opened, it keeps locked
/// from other open attempts until it goes out scope.
///
/// Optionally, `Repo` can be opened in [`read-only`] mode if you only need
/// read access.
///
/// # Examples
///
/// Create an OS file system based repository.
///
/// ```no_run
/// # #![allow(unused_mut, unused_variables, dead_code)]
/// # use zbox::Result;
/// use zbox::{init_env, RepoOpener};
///
/// # fn foo() -> Result<()> {
/// init_env();
/// let mut repo = RepoOpener::new()
///     .create(true)
///     .open("file:///path/to/repo", "pwd")?;
/// # Ok(())
/// # }
/// ```
///
/// Create a memory based repository.
///
/// ```
/// # #![allow(unused_mut, unused_variables, dead_code)]
/// # use zbox::{init_env, Result, RepoOpener};
/// # fn foo() -> Result<()> {
/// # init_env();
/// let mut repo = RepoOpener::new().create(true).open("mem://foo", "pwd")?;
/// # Ok(())
/// # }
/// ```
///
/// Open a repository in read-only mode.
///
/// ```no_run
/// # #![allow(unused_mut, unused_variables, dead_code)]
/// # use zbox::{Result, RepoOpener};
/// # fn foo() -> Result<()> {
/// let mut repo = RepoOpener::new()
///     .read_only(true)
///     .open("file:///path/to/repo", "pwd")?;
/// # Ok(())
/// # }
/// ```
///
/// [`std::fs`]: https://doc.rust-lang.org/std/fs/index.html
/// [`init_env`]: fn.init_env.html
/// [`RepoOpener`]: struct.RepoOpener.html
/// [`read-only`]: struct.RepoOpener.html#method.read_only
pub struct Repo {
    fs: Fs,
}

impl Repo {
    /// Returns whether the URI points at an existing repository.
    #[inline]
    pub fn exists(uri: &str) -> Result<bool> {
        Fs::exists(uri)
    }

    // create repo
    #[inline]
    fn create(uri: &str, pwd: &str, cfg: &Config) -> Result<Repo> {
        let fs = Fs::create(uri, pwd, cfg)?;
        Ok(Repo { fs })
    }

    // open repo
    #[inline]
    fn open(
        uri: &str,
        pwd: &str,
        read_only: bool,
        force: bool,
    ) -> Result<Repo> {
        let fs = Fs::open(uri, pwd, read_only, force)?;
        Ok(Repo { fs })
    }

    /// Get repository metadata information.
    pub fn info(&self) -> Result<RepoInfo> {
        let meta = self.fs.info();
        Ok(RepoInfo {
            volume_id: meta.vol_info.id.clone(),
            ver: meta.vol_info.ver.clone(),
            uri: meta.vol_info.uri.clone(),
            cost: meta.vol_info.cost,
            cipher: meta.vol_info.cipher,
            compress: meta.vol_info.compress,
            version_limit: meta.opts.version_limit,
            dedup_chunk: meta.opts.dedup_chunk,
            read_only: meta.read_only,
            ctime: meta.vol_info.ctime,
        })
    }

    /// Reset password for the repository.
    ///
    /// Note: if this method failed due to IO error, super block might be
    /// damaged. If it is the case, use
    /// [repair_super_block](struct.Repo.html#method.repair_super_block)
    /// to restore super block before re-opening the repo.
    pub fn reset_password(
        &mut self,
        old_pwd: &str,
        new_pwd: &str,
        ops_limit: OpsLimit,
        mem_limit: MemLimit,
    ) -> Result<()> {
        let cost = Cost::new(ops_limit, mem_limit);
        self.fs.reset_password(old_pwd, new_pwd, cost)
    }

    /// Repair possibly damaged super block.
    ///
    /// This method will try to repair super block using backup. One scenario
    /// is when [reset_password](struct.Repo.html#method.reset_password) failed
    /// due to IO error, super block might be damaged. Using this method can
    /// restore the damaged super block from backup. If super block is all
    /// good, this method is no-op.
    ///
    /// This method is not useful for memory-based storage and must be called
    /// when repo is closed.
    #[inline]
    pub fn repair_super_block(uri: &str, pwd: &str) -> Result<()> {
        Fs::repair_super_block(uri, pwd)
    }

    /// Returns whether the path points at an existing entity in repository.
    ///
    /// `path` must be an absolute path.
    pub fn path_exists<P: AsRef<Path>>(&self, path: P) -> Result<bool> {
        Ok(self
            .fs
            .resolve(path.as_ref())
            .map(|_| true)
            .unwrap_or(false))
    }

    /// Returns whether the path exists in repository and is pointing at
    /// a regular file.
    ///
    /// `path` must be an absolute path.
    pub fn is_file<P: AsRef<Path>>(&self, path: P) -> Result<bool> {
        match self.fs.resolve(path.as_ref()) {
            Ok(fnode_ref) => {
                let fnode = fnode_ref.read().unwrap();
                Ok(fnode.is_file())
            }
            Err(_) => Ok(false),
        }
    }

    /// Returns whether the path exists in repository and is pointing at
    /// a directory.
    ///
    /// `path` must be an absolute path.
    pub fn is_dir<P: AsRef<Path>>(&self, path: P) -> Result<bool> {
        match self.fs.resolve(path.as_ref()) {
            Ok(fnode_ref) => {
                let fnode = fnode_ref.read().unwrap();
                Ok(fnode.is_dir())
            }
            Err(_) => Ok(false),
        }
    }

    /// Create a file in read-write mode.
    ///
    /// This method will create a file if it does not exist, and will
    /// truncate it if it does.
    ///
    /// See the [`OpenOptions::open`](struct.OpenOptions.html#method.open)
    /// method for more details.
    ///
    /// `path` must be an absolute path.
    #[inline]
    pub fn create_file<P: AsRef<Path>>(&mut self, path: P) -> Result<File> {
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .open(self, path)
    }

    /// Attempts to open a file in read-only mode.
    ///
    /// `path` must be an absolute path.
    ///
    /// See the [`OpenOptions::open`] method for more details.
    ///
    /// # Errors
    /// This method will return an error if path does not already exist.
    /// Other errors may also be returned according to [`OpenOptions::open`].
    ///
    /// # Examples
    ///
    /// ```
    /// # #![allow(unused_mut, unused_variables, dead_code)]
    /// # use zbox::{init_env, Result, RepoOpener};
    /// # fn foo() -> Result<()> {
    /// # init_env();
    /// # let mut repo = RepoOpener::new()
    /// #     .create(true)
    /// #     .open("mem://foo", "pwd")?;
    /// let mut f = repo.open_file("foo.txt")?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`OpenOptions::open`]: struct.OpenOptions.html#method.open
    #[inline]
    pub fn open_file<P: AsRef<Path>>(&mut self, path: P) -> Result<File> {
        OpenOptions::new().open(self, path)
    }

    /// Creates a new, empty directory at the specified path.
    ///
    /// `path` must be an absolute path.
    #[inline]
    pub fn create_dir<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        self.fs
            .create_fnode(path.as_ref(), FileType::Dir, Options::default())
            .map(|_| ())
    }

    /// Recursively create a directory and all of its parent components if they
    /// are missing.
    ///
    /// `path` must be an absolute path.
    #[inline]
    pub fn create_dir_all<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        self.fs.create_dir_all(path.as_ref())
    }

    /// Returns a vector of all the entries within a directory.
    ///
    /// `path` must be an absolute path.
    #[inline]
    pub fn read_dir<P: AsRef<Path>>(&self, path: P) -> Result<Vec<DirEntry>> {
        self.fs.read_dir(path.as_ref())
    }

    /// Get the metadata about a file or directory at specified path.
    ///
    /// `path` must be an absolute path.
    #[inline]
    pub fn metadata<P: AsRef<Path>>(&self, path: P) -> Result<Metadata> {
        self.fs.metadata(path.as_ref())
    }

    /// Return a vector of history versions of a regular file at specified path.
    ///
    /// `path` must be an absolute path to a regular file.
    #[inline]
    pub fn history<P: AsRef<Path>>(&self, path: P) -> Result<Vec<Version>> {
        self.fs.history(path.as_ref())
    }

    /// Copies the content of one file to another.
    ///
    /// This method will **overwrite** the content of `to`.
    ///
    /// `from` and `to` must be absolute paths to regular files.
    ///
    /// If `from` and `to` both point to the same file, this method is no-op.
    #[inline]
    pub fn copy<P: AsRef<Path>, Q: AsRef<Path>>(
        &mut self,
        from: P,
        to: Q,
    ) -> Result<()> {
        self.fs.copy(from.as_ref(), to.as_ref())
    }

    /// Copies a directory to another recursively.
    ///
    /// This method will **overwrite** the content of files in `to` with
    /// the files in `from` which have same relative location.
    ///
    /// `from` and `to` must be absolute paths to directories.
    ///
    /// If `to` is not empty, the entire directory tree of `from` will be
    /// merged to `to`.
    ///
    /// This method will stop if any errors happened.
    ///
    /// If `from` and `to` both point to the same directory, this method is
    /// no-op.
    #[inline]
    pub fn copy_dir_all<P: AsRef<Path>, Q: AsRef<Path>>(
        &mut self,
        from: P,
        to: Q,
    ) -> Result<()> {
        self.fs.copy_dir_all(from.as_ref(), to.as_ref())
    }

    /// Removes a regular file from the repository.
    ///
    /// `path` must be an absolute path.
    #[inline]
    pub fn remove_file<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        self.fs.remove_file(path.as_ref())
    }

    /// Remove an existing empty directory.
    ///
    /// `path` must be an absolute path.
    #[inline]
    pub fn remove_dir<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        self.fs.remove_dir(path.as_ref())
    }

    /// Removes a directory at this path, after removing all its children.
    /// Use carefully!
    ///
    /// `path` must be an absolute path.
    #[inline]
    pub fn remove_dir_all<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        self.fs.remove_dir_all(path.as_ref())
    }

    /// Rename a file or directory to a new name, replacing the original file
    /// if `to` already exists.
    ///
    /// `from` and `to` must be absolute paths.
    #[inline]
    pub fn rename<P: AsRef<Path>, Q: AsRef<Path>>(
        &mut self,
        from: P,
        to: Q,
    ) -> Result<()> {
        self.fs.rename(from.as_ref(), to.as_ref())
    }
}

impl Debug for Repo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Repo").finish()
    }
}
