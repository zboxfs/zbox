use std::path::Path;
use std::io::SeekFrom;

use error::Error;
use base::{IntoRef, Time, Version as RepoVersion};
use base::crypto::{OpsLimit, MemLimit, Cost, Cipher, Crypto};
use trans::Eid;
use fs::{Fs, FsRef, FileType, Version, Metadata, DirEntry, Fnode};
use super::{Result, File};

/// Repo opener
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

/// File open options
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
        if self.version_limit == 0 {
            return Err(Error::InvalidArgument);
        }
        repo.open_file_with_options(path, self)
    }
}

/// Repository info
#[derive(Debug)]
pub struct RepoInfo {
    pub volume_id: Eid,
    pub ver: RepoVersion,
    pub uri: String,
    pub cost: Cost,
    pub cipher: Cipher,
    pub ctime: Time,
    pub read_only: bool,
}

/// Repository
#[derive(Debug)]
pub struct Repo {
    fs: FsRef,
    read_only: bool,
}

impl Repo {
    /// Check if repo exists
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
    pub fn path_is_file<P: AsRef<Path>>(&self, path: P) -> bool {
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
    pub fn path_is_dir<P: AsRef<Path>>(&self, path: P) -> bool {
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

        if fs.resolve(path).is_ok() {
            if opts.create_new {
                return Err(Error::AlreadyExists);
            }
        } else if opts.create {
            fs.create_fnode(path, FileType::File, opts.version_limit)?;
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

    /// Open a regular file in read-only
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

    /// Get metadata of a path
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
