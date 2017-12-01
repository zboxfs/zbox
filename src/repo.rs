use std::path::Path;
use std::io::SeekFrom;

use error::Error;
use base::{IntoRef, Time, Version as RepoVersion};
use base::crypto::{OpsLimit, MemLimit, Cost, Cipher, Crypto};
use trans::Eid;
use fs::{Fs, FsRef, FileType, Version, Metadata, DirEntry, Fnode};
use super::{Result, File};

/// A builder used to create repository [`Repo`] in various manners.
///
/// [`Repo`]: struct.Repo.html
#[derive(Debug)]
pub struct RepoOpener {
    cost: Cost,
    cipher: Cipher,
    create: bool,
    read_only: bool,
}

impl RepoOpener {
    pub fn new() -> Self {
        RepoOpener::default()
    }

    pub fn ops_limit(&mut self, ops_limit: OpsLimit) -> &mut Self {
        self.cost.ops_limit = ops_limit;
        self
    }

    pub fn mem_limit(&mut self, mem_limit: MemLimit) -> &mut Self {
        self.cost.mem_limit = mem_limit;
        self
    }

    pub fn cipher(&mut self, cipher: Cipher) -> &mut Self {
        self.cipher = cipher;
        self
    }

    pub fn create(&mut self, create: bool) -> &mut Self {
        self.create = create;
        self
    }

    pub fn read_only(&mut self, read_only: bool) -> &mut Self {
        self.read_only = read_only;
        self
    }

    pub fn open(&self, uri: &str, pwd: &str) -> Result<Repo> {
        if self.create {
            if self.read_only {
                return Err(Error::InvalidArgument);
            }
            if Repo::exists(uri)? {
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
            read_only: false,
        }
    }
}

/// Options and flags which can be used to configure how a file is opened.
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

    pub fn read(&mut self, read: bool) -> &mut OpenOptions {
        self.read = read;
        self
    }

    pub fn write(&mut self, write: bool) -> &mut OpenOptions {
        self.write = write;
        self
    }

    pub fn append(&mut self, append: bool) -> &mut OpenOptions {
        self.append = append;
        if append {
            self.write = true;
        }
        self
    }

    pub fn truncate(&mut self, truncate: bool) -> &mut OpenOptions {
        self.truncate = truncate;
        if truncate {
            self.write = true;
        }
        self
    }

    pub fn create(&mut self, create: bool) -> &mut OpenOptions {
        self.create = create;
        if create {
            self.write = true;
        }
        self
    }

    pub fn create_new(&mut self, create_new: bool) -> &mut OpenOptions {
        self.create_new = create_new;
        if create_new {
            self.create = true;
            self.write = true;
        }
        self
    }

    pub fn version_limit(&mut self, version_limit: u8) -> &mut OpenOptions {
        self.version_limit = version_limit;
        self
    }

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
    /// Unique volume id
    pub volume_id: Eid,

    /// Repository version number
    pub ver: RepoVersion,

    /// Location URI string
    pub uri: String,

    /// Password hashing cost
    pub cost: Cost,

    /// Crypto cipher
    pub cipher: Cipher,

    /// Creation time
    pub ctime: Time,

    /// Read only mode
    pub read_only: bool,
}

/// An encrypted repository contains the whole file system.
///
/// A `Repo` represents a secure collection which consists of files,
/// directories and their associated data. Similar to [`std::fs`], `Repo`
/// provides methods to manipulate the inner file system.
///
/// [`zbox_init`] should be called before any operations on `Repo`.
///
/// # Create and open `Repo`
///
/// `Repo` can be created on different storages using [`RepoOpener`]. It uses
/// an URI-like string to specify location. Currently two types of storages
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
/// # use zbox::Result;
/// use zbox::{zbox_init, RepoOpener};
///
/// # fn foo() -> Result<()> {
/// zbox_init();
/// let mut repo = RepoOpener::new()
///     .create(true)
///     .open("file:///local/path", "pwd")?;
/// # Ok(())
/// # }
/// ```
///
/// Create a memory based repository.
///
/// ```no_run
/// # use zbox::{Result, RepoOpener};
/// # fn foo() -> Result<()> {
/// let mut repo = RepoOpener::new().create(true).open("mem://foo", "pwd")?;
/// # Ok(())
/// # }
/// ```
///
/// [`std::fs`]: https://doc.rust-lang.org/std/fs/index.html
/// [`zbox_init`]: fn.zbox_init.html
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
    /// Existence check depends on underneath storage, for memory storage, it
    /// always returns true.
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

    /// Get repository info
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

    /// Change repo password
    pub fn change_password(
        &mut self,
        old_pwd: &str,
        new_pwd: &str,
        ops_limit: OpsLimit,
        mem_limit: MemLimit,
    ) -> Result<()> {
        let mut fs = self.fs.write().unwrap();
        let cost = Cost::new(ops_limit, mem_limit);
        fs.change_password(old_pwd, new_pwd, cost)
    }

    /// Check if path exists
    pub fn path_exists<P: AsRef<Path>>(&self, path: P) -> bool {
        let fs = self.fs.read().unwrap();
        fs.resolve(path.as_ref()).map(|_| true).unwrap_or(false)
    }

    /// Check if path is a regular file
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

    /// Check if path is a directory
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
        let mut file = File::new(handle, pos, !opts.write);

        if opts.truncate {
            file.set_len(0)?;
        }

        Ok(file)
    }

    /// Create a file
    pub fn create_file<P: AsRef<Path>>(&mut self, path: P) -> Result<File> {
        OpenOptions::new().create(true).truncate(true).open(
            self,
            path,
        )
    }

    /// Open a regular file in read-only mode
    pub fn open_file<P: AsRef<Path>>(&mut self, path: P) -> Result<File> {
        OpenOptions::new().open(self, path)
    }

    /// Create a directory
    pub fn create_dir<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let mut fs = self.fs.write().unwrap();
        fs.create_fnode(path.as_ref(), FileType::Dir, 0)?;
        Ok(())
    }

    /// Recursively create directories along the path
    pub fn create_dir_all<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let mut fs = self.fs.write().unwrap();
        fs.create_dir_all(path.as_ref())
    }

    /// Read directory entries
    pub fn read_dir<P: AsRef<Path>>(&self, path: P) -> Result<Vec<DirEntry>> {
        let fs = self.fs.read().unwrap();
        fs.read_dir(path.as_ref())
    }

    /// Given a path, query the repository to get information about a file,
    /// directory, etc.
    pub fn metadata<P: AsRef<Path>>(&self, path: P) -> Result<Metadata> {
        let fs = self.fs.read().unwrap();
        fs.metadata(path.as_ref())
    }

    /// Get file history of a path
    pub fn history<P: AsRef<Path>>(&self, path: P) -> Result<Vec<Version>> {
        let fs = self.fs.read().unwrap();
        fs.history(path.as_ref())
    }

    /// Copy a file to another
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

    /// Remove a file
    pub fn remove_file<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let mut fs = self.fs.write().unwrap();
        fs.remove_file(path.as_ref())
    }

    /// Remove an existing empty directory
    pub fn remove_dir<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let mut fs = self.fs.write().unwrap();
        fs.remove_dir(path.as_ref())
    }

    /// Remove an existing directory recursively
    pub fn remove_dir_all<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let mut fs = self.fs.write().unwrap();
        fs.remove_dir_all(path.as_ref())
    }

    /// Rename a file or directory to new name
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
