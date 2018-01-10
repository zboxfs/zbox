use std::path::Path;
use std::io::SeekFrom;
use std::time::SystemTime;

use error::Error;
use base::{self, IntoRef, Time};
use base::crypto::{OpsLimit, MemLimit, Cost, Cipher, Crypto};
use trans::Eid;
use fs::{Fs, FsRef, FileType, Version, Metadata, DirEntry, Fnode};
use super::{Result, File};

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
/// #![allow(unused_mut, unused_variables)]
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
/// Specify parameters for creating a repository.
///
/// ```
/// #![allow(unused_mut, unused_variables)]
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
#[derive(Debug)]
pub struct RepoOpener {
    cost: Cost,
    cipher: Cipher,
    create: bool,
    create_new: bool,
    read_only: bool,
}

impl RepoOpener {
    /// Creates a blank new set of options ready for configuration.
    pub fn new() -> Self {
        RepoOpener::default()
    }

    /// Sets the password hash operation limit.
    ///
    /// This option is only used for creating a repository.
    /// `OpsLimit::Interactive` is the default.
    pub fn ops_limit(&mut self, ops_limit: OpsLimit) -> &mut Self {
        self.cost.ops_limit = ops_limit;
        self
    }

    /// Sets the password hash memory limit.
    ///
    /// This option is only used for creating a repository.
    /// `MemLimit::Interactive` is the default.
    pub fn mem_limit(&mut self, mem_limit: MemLimit) -> &mut Self {
        self.cost.mem_limit = mem_limit;
        self
    }

    /// Sets the crypto cipher encrypts the repository.
    ///
    /// This option is only used for creating a repository. `Cipher::Aes` is
    /// the default if hardware supports AES-NI instructions, otherwise it will
    /// fall back to `Cipher::Xchacha`.
    pub fn cipher(&mut self, cipher: Cipher) -> &mut Self {
        self.cipher = cipher;
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
    /// repository is allowed to exist at the target location.
    pub fn create_new(&mut self, create_new: bool) -> &mut Self {
        self.create_new = create_new;
        if create_new {
            self.create = true;
        }
        self
    }

    /// Sets the option for read-only mode.
    ///
    /// This option cannot be true with either `create` or `create_new` is true.
    pub fn read_only(&mut self, read_only: bool) -> &mut Self {
        self.read_only = read_only;
        self
    }

    /// Opens a repository at URI with the password and options specified by
    /// `self`.
    ///
    /// Currently two types of storages are supported:
    ///
    /// - OS file system based storage, location prefix is `file://`
    ///
    ///   After the prefix is the path to a directory on OS file system. It can
    ///   be a relative or absolute path.
    ///
    /// - Memory based storage, location prefix is `mem://`
    ///
    ///   As memory stoage is volatile, it is always be used with `create`
    ///   option. It doesn't make sense to open an existing memory storage,
    ///   thus the string after prefix is arbitrary.
    ///
    /// After a repository is opened, all of the other functions provided by
    /// Zbox will be thread-safe.
    ///
    /// The application should destroy the password as soon as possible after
    /// calling this function.
    ///
    /// # Errors
    ///
    /// Open a memory based repository without enable `create` option will
    /// return an error.
    pub fn open(&self, uri: &str, pwd: &str) -> Result<Repo> {
        if self.create {
            if self.read_only {
                return Err(Error::InvalidArgument);
            }
            if Repo::exists(uri)? {
                if self.create_new {
                    return Err(Error::AlreadyExists);
                }
                Repo::open(uri, pwd, self.read_only)
            } else {
                Repo::create(uri, pwd, self.cost, self.cipher)
            }
        } else {
            Repo::open(uri, pwd, self.read_only)
        }
    }
}

impl Default for RepoOpener {
    fn default() -> Self {
        let default_cipher = if Crypto::is_aes_hardware_available() {
            Cipher::Aes
        } else {
            Cipher::Xchacha
        };

        RepoOpener {
            cost: Cost::default(),
            cipher: default_cipher,
            create: false,
            create_new: false,
            read_only: false,
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
/// #![allow(unused_mut, unused_variables)]
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
#[derive(Debug)]
pub struct OpenOptions {
    read: bool,
    write: bool,
    append: bool,
    truncate: bool,
    create: bool,
    create_new: bool,
    version_limit: u8,
}

impl OpenOptions {
    /// Creates a blank new set of options ready for configuration.
    ///
    /// All options are initially set to false, except for `read`.
    pub fn new() -> Self {
        OpenOptions {
            read: true,
            write: false,
            append: false,
            truncate: false,
            create: false,
            create_new: false,
            version_limit: Fnode::DEFAULT_VERSION_LIMIT,
        }
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

    /// Sets the maximum number of version allowed.
    ///
    /// The `version_limit` must be within [1, 255].
    pub fn version_limit(&mut self, version_limit: u8) -> &mut OpenOptions {
        self.version_limit = version_limit;
        self
    }

    /// Opens a file at path with the options specified by `self`.
    pub fn open<P: AsRef<Path>>(
        &self,
        repo: &mut Repo,
        path: P,
    ) -> Result<File> {
        // version limit must be greater than 0
        if self.version_limit == 0 {
            return Err(Error::InvalidArgument);
        }
        repo.open_file_with_options(path, self)
    }
}

/// Information about a repository.
#[derive(Debug)]
pub struct RepoInfo {
    volume_id: Eid,
    ver: base::Version,
    uri: String,
    cost: Cost,
    cipher: Cipher,
    ctime: Time,
    read_only: bool,
}

impl RepoInfo {
    /// Returns the unique volume id in this repository.
    pub fn volume_id(&self) -> &Eid {
        &self.volume_id
    }

    /// Returns repository version string.
    ///
    /// This is the string representation of this repository, for example,
    /// `1.0.2`.
    pub fn version(&self) -> String {
        self.ver.to_string()
    }

    /// Returns the location URI string of this repository.
    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// Returns the operation limit for repository password hash.
    pub fn ops_limit(&self) -> OpsLimit {
        self.cost.ops_limit
    }

    /// Returns the memory limit for repository password hash
    pub fn mem_limit(&self) -> MemLimit {
        self.cost.mem_limit
    }

    /// Returns repository password encryption cipher.
    pub fn cipher(&self) -> Cipher {
        self.cipher
    }

    /// Returns the creation time of this repository.
    pub fn created(&self) -> SystemTime {
        self.ctime.to_system_time()
    }

    /// Returns whether this repository is read-only.
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }
}

/// An encrypted repository contains the whole file system.
///
/// A `Repo` represents a secure collection which consists of files,
/// directories and their associated data. Similar to [`std::fs`], `Repo`
/// provides methods to manipulate the enclosed file system.
///
/// # Create and open `Repo`
///
/// `Repo` can be created on different storages using [`RepoOpener`]. It uses
/// an URI-like string to specify its location. Currently two types of storages
/// are supported:
///
/// * OS file system based storage, location prefix: `file://`
/// * Memory based storage, location prefix: `mem://`
///
/// `Repo` can only be opened once at a time. After opened, it keeps locked
/// from other open attempts until it goes out scope.
///
/// Optionally, `Repo` can also be opened in [`read-only`] mode.
///
/// # Examples
///
/// Create an OS file system based repository.
///
/// ```no_run
/// #![allow(unused_mut, unused_variables, dead_code)]
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
/// #![allow(unused_mut, unused_variables, dead_code)]
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
/// #![allow(unused_mut, unused_variables, dead_code)]
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
#[derive(Debug)]
pub struct Repo {
    fs: FsRef,
    read_only: bool,
}

impl Repo {
    /// Returns whether the URI points at an existing repository.
    ///
    /// Existence check depends on the underlying storage implementation, for
    /// memory storage, it always returns false. For file storage, it will
    /// return if the specified path exists on the OS file system.
    pub fn exists(uri: &str) -> Result<bool> {
        Fs::exists(uri)
    }

    // create repo
    fn create(
        uri: &str,
        pwd: &str,
        cost: Cost,
        cipher: Cipher,
    ) -> Result<Repo> {
        let fs = Fs::create(uri, pwd, cost, cipher)?.into_ref();
        Ok(Repo {
            fs,
            read_only: false,
        })
    }

    // open repo
    fn open(uri: &str, pwd: &str, read_only: bool) -> Result<Repo> {
        let fs = Fs::open(uri, pwd)?.into_ref();
        Ok(Repo { fs, read_only })
    }

    /// Get repository metadata infomation.
    pub fn info(&self) -> RepoInfo {
        let fs = self.fs.read().unwrap();
        let meta = fs.volume_meta();
        RepoInfo {
            volume_id: meta.id.clone(),
            ver: meta.ver.clone(),
            uri: meta.uri.clone(),
            cost: meta.cost.clone(),
            cipher: meta.cipher.clone(),
            ctime: meta.ctime.clone(),
            read_only: self.read_only,
        }
    }

    /// Reset password for the respository.
    pub fn reset_password(
        &mut self,
        old_pwd: &str,
        new_pwd: &str,
        ops_limit: OpsLimit,
        mem_limit: MemLimit,
    ) -> Result<()> {
        let mut fs = self.fs.write().unwrap();
        let cost = Cost::new(ops_limit, mem_limit);
        fs.reset_password(old_pwd, new_pwd, cost)
    }

    /// Returns whether the path points at an existing entity in repository.
    ///
    /// `path` must be an absolute path.
    pub fn path_exists<P: AsRef<Path>>(&self, path: P) -> bool {
        let fs = self.fs.read().unwrap();
        fs.resolve(path.as_ref()).map(|_| true).unwrap_or(false)
    }

    /// Returns whether the path exists in repository and is pointing at
    /// a regular file.
    ///
    /// `path` must be an absolute path.
    pub fn is_file<P: AsRef<Path>>(&self, path: P) -> bool {
        let fs = self.fs.read().unwrap();
        match fs.resolve(path.as_ref()) {
            Ok(fnode_ref) => {
                let fnode = fnode_ref.read().unwrap();
                !fnode.is_dir()
            }
            Err(_) => false,
        }
    }

    /// Returns whether the path exists in repository and is pointing at
    /// a directory.
    ///
    /// `path` must be an absolute path.
    pub fn is_dir<P: AsRef<Path>>(&self, path: P) -> bool {
        let fs = self.fs.read().unwrap();
        match fs.resolve(path.as_ref()) {
            Ok(fnode_ref) => {
                let fnode = fnode_ref.read().unwrap();
                fnode.is_dir()
            }
            Err(_) => false,
        }
    }

    // open a regular file with options
    fn open_file_with_options<P: AsRef<Path>>(
        &mut self,
        path: P,
        opts: &OpenOptions,
    ) -> Result<File> {
        if self.read_only &&
            (opts.write || opts.append || opts.truncate || opts.create ||
                 opts.create_new)
        {
            return Err(Error::ReadOnly);
        }

        let mut fs = self.fs.write().unwrap();
        let path = path.as_ref();

        match fs.resolve(path) {
            Ok(_) => {
                if opts.create_new {
                    return Err(Error::AlreadyExists);
                }
            }
            Err(ref err) if *err == Error::NotFound => {
                fs.create_fnode(path, FileType::File, opts.version_limit)?;
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

        let pos = if opts.append {
            SeekFrom::Start(curr_len as u64)
        } else {
            SeekFrom::Start(0)
        };
        let mut file = File::new(handle, pos, opts.read, opts.write);

        if opts.truncate {
            file.set_len(0)?;
        }

        Ok(file)
    }

    /// Create a file in read-write mode.
    ///
    /// This function will create a file if it does not exist, and will
    /// truncate it if it does.
    ///
    /// See the [`OpenOptions::open`](struct.OpenOptions.html#method.open)
    /// function for more details.
    pub fn create_file<P: AsRef<Path>>(&mut self, path: P) -> Result<File> {
        OpenOptions::new().create(true).truncate(true).open(
            self,
            path,
        )
    }

    /// Attempts to open a file in read-only mode.
    ///
    /// `path` must be an absolute path.
    ///
    /// See the [`OpenOptions::open`] function for more details.
    ///
    /// # Errors
    /// This function will return an error if path does not already exist.
    /// Other errors may also be returned according to [`OpenOptions::open`].
    ///
    /// # Examples
    ///
    /// ```
    /// #![allow(unused_mut, unused_variables, dead_code)]
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
    pub fn open_file<P: AsRef<Path>>(&mut self, path: P) -> Result<File> {
        OpenOptions::new().open(self, path)
    }

    /// Creates a new, empty directory at the specified path.
    ///
    /// `path` must be an absolute path.
    pub fn create_dir<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let mut fs = self.fs.write().unwrap();
        fs.create_fnode(path.as_ref(), FileType::Dir, 0)?;
        Ok(())
    }

    /// Recursively create a directory and all of its parent components if they
    /// are missing.
    ///
    /// `path` must be an absolute path.
    pub fn create_dir_all<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let mut fs = self.fs.write().unwrap();
        fs.create_dir_all(path.as_ref())
    }

    /// Returns a vector of all the entries within a directory.
    ///
    /// `path` must be an absolute path.
    pub fn read_dir<P: AsRef<Path>>(&self, path: P) -> Result<Vec<DirEntry>> {
        let fs = self.fs.read().unwrap();
        fs.read_dir(path.as_ref())
    }

    /// Given a path, query the repository to get information about a file,
    /// directory, etc.
    ///
    /// `path` must be an absolute path.
    pub fn metadata<P: AsRef<Path>>(&self, path: P) -> Result<Metadata> {
        let fs = self.fs.read().unwrap();
        fs.metadata(path.as_ref())
    }

    /// Return a vector of history versions of a regular file.
    ///
    /// `path` must be an absolute path to a regular file.
    pub fn history<P: AsRef<Path>>(&self, path: P) -> Result<Vec<Version>> {
        let fs = self.fs.read().unwrap();
        fs.history(path.as_ref())
    }

    /// Copies the content of one file to another.
    ///
    /// This function will overwrite the content of `to`.
    ///
    /// If `from` and `to` both point to the same file, then this function will
    /// do nothing.
    ///
    /// `from` and `to` must be absolute paths to regular files.
    pub fn copy<P: AsRef<Path>, Q: AsRef<Path>>(
        &mut self,
        from: P,
        to: Q,
    ) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let mut fs = self.fs.write().unwrap();
        fs.copy(from.as_ref(), to.as_ref())
    }

    /// Removes a regular file from the repository.
    ///
    /// `path` must be an absolute path.
    pub fn remove_file<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let mut fs = self.fs.write().unwrap();
        fs.remove_file(path.as_ref())
    }

    /// Remove an existing empty directory.
    ///
    /// `path` must be an absolute path.
    pub fn remove_dir<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let mut fs = self.fs.write().unwrap();
        fs.remove_dir(path.as_ref())
    }

    /// Removes a directory at this path, after removing all its children.
    /// Use carefully!
    ///
    /// `path` must be an absolute path.
    pub fn remove_dir_all<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let mut fs = self.fs.write().unwrap();
        fs.remove_dir_all(path.as_ref())
    }

    /// Rename a file or directory to a new name, replacing the original file
    /// if to already exists.
    ///
    /// `from` and `to` must be absolute paths.
    pub fn rename<P: AsRef<Path>, Q: AsRef<Path>>(
        &mut self,
        from: P,
        to: Q,
    ) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let mut fs = self.fs.write().unwrap();
        fs.rename(from.as_ref(), to.as_ref())
    }
}
