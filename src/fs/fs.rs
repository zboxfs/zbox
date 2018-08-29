use std::io;
use std::path::Path;
use std::sync::{Arc, RwLock};

use bytes::BufMut;

use super::fnode::{
    Cache as FnodeCache, DirEntry, FileType, Fnode, FnodeRef, Metadata,
    Reader as FnodeReader, Version, Writer as FnodeWriter,
};
use super::{Config, Handle};
use base::crypto::Cost;
use base::IntoRef;
use content::{Store, StoreRef};
use error::{Error, Result};
use trans::cow::IntoCow;
use trans::{Eid, Finish, Id, TxMgr, TxMgrRef};
use volume::{Info as VolumeInfo, Volume, VolumeRef};

// default cache size
const FNODE_CACHE_SIZE: usize = 16;

/// File system metadata
#[derive(Debug)]
pub struct Meta {
    pub version_limit: u8,
    pub read_only: bool,
    pub vol_info: VolumeInfo,
}

/// Shutter
#[derive(Debug)]
pub struct Shutter(bool);

impl Shutter {
    fn new() -> ShutterRef {
        Shutter(false).into_ref()
    }

    #[inline]
    pub fn is_closed(&self) -> bool {
        self.0
    }

    #[inline]
    fn close(&mut self) {
        self.0 = true
    }
}

impl IntoRef for Shutter {}

pub type ShutterRef = Arc<RwLock<Shutter>>;

/// File system
#[derive(Debug)]
pub struct Fs {
    root: FnodeRef,
    fcache: FnodeCache,
    store: StoreRef,
    txmgr: TxMgrRef,
    vol: VolumeRef,
    shutter: ShutterRef,
    version_limit: u8,
    read_only: bool,
}

impl Fs {
    /// Check if fs exists
    pub fn exists(uri: &str) -> Result<bool> {
        let vol = Volume::new(uri)?;
        vol.exists()
    }

    /// Create new fs
    pub fn create(uri: &str, pwd: &str, cfg: &Config) -> Result<Fs> {
        debug!("create repo: {}", uri);

        let root_id = Eid::new();
        let walq_id = Eid::new();
        let store_id = Eid::new();

        // super block payload: root_id + wqlq_id + store_id + version_limit
        let mut payload = Vec::new();
        payload.put(root_id.as_ref());
        payload.put(walq_id.as_ref());
        payload.put(store_id.as_ref());
        payload.put(cfg.version_limit);

        // create and initialise volume
        let mut vol = Volume::new(uri)?;
        vol.init(pwd, cfg, &payload)?;

        let vol = vol.into_ref();

        // create tx manager and fnode cache
        let txmgr = TxMgr::new(&walq_id, &vol).into_ref();
        let fcache = FnodeCache::new(FNODE_CACHE_SIZE, &txmgr);

        // the initial transaction to create root fnode and save store,
        // it must be successful
        let mut store_ref: Option<StoreRef> = None;
        let mut root_ref: Option<FnodeRef> = None;
        TxMgr::begin_trans(&txmgr)?.run_all(|| {
            let store_cow =
                Store::new(&txmgr, &vol).into_cow_with_id(&store_id, &txmgr)?;
            let root_cow = Fnode::new(FileType::Dir, 0, &store_cow)
                .into_cow_with_id(&root_id, &txmgr)?;
            root_ref = Some(root_cow);
            store_ref = Some(store_cow);
            Ok(())
        })?;

        debug!("repo created");

        Ok(Fs {
            root: root_ref.unwrap(),
            fcache,
            store: store_ref.unwrap(),
            txmgr,
            vol,
            shutter: Shutter::new(),
            version_limit: cfg.version_limit,
            read_only: false,
        })
    }

    /// Open fs
    pub fn open(uri: &str, pwd: &str, read_only: bool) -> Result<Fs> {
        let mut vol = Volume::new(uri)?;

        debug!("open repo: {}, read_only: {}", uri, read_only);

        // open volume
        let payload = vol.open(pwd)?;

        // decompose super block payload
        let mut iter = payload.chunks(Eid::EID_SIZE);
        let root_id = Eid::from_slice(iter.next().unwrap());
        let walq_id = Eid::from_slice(iter.next().unwrap());
        let store_id = Eid::from_slice(iter.next().unwrap());
        let version_limit = iter.next().unwrap()[0];

        let vol = vol.into_ref();

        // open transaction manager
        let txmgr = TxMgr::open(&walq_id, &vol)?.into_ref();

        // create file sytem components
        let store = Store::open(&store_id, &txmgr, &vol)?;
        let root = Fnode::load_root(&root_id, &txmgr, &store, &vol)?;
        let fcache = FnodeCache::new(FNODE_CACHE_SIZE, &txmgr);

        debug!("repo opened");

        Ok(Fs {
            root,
            fcache,
            store,
            txmgr,
            vol,
            shutter: Shutter::new(),
            version_limit,
            read_only,
        })
    }

    #[inline]
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// Get file system metadata
    pub fn meta(&self) -> Meta {
        let vol = self.vol.read().unwrap();
        Meta {
            version_limit: self.version_limit,
            read_only: self.read_only,
            vol_info: vol.info(),
        }
    }

    /// Reset volume password
    pub fn reset_password(
        &mut self,
        old_pwd: &str,
        new_pwd: &str,
        cost: Cost,
    ) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let mut vol = self.vol.write().unwrap();
        vol.reset_password(old_pwd, new_pwd, cost)
    }

    /// Resolve path
    pub fn resolve(&self, path: &Path) -> Result<FnodeRef> {
        // only resolve absolute path
        if !path.has_root() {
            return Err(Error::InvalidPath);
        }

        let mut fnode = self.root.clone();

        // loop through path component and skip root
        for name in path.iter().skip(1) {
            let name = name.to_str().unwrap();
            fnode = Fnode::child(&fnode, name, &self.fcache, &self.vol)?;
        }
        Ok(fnode)
    }

    // resolve path to parent fnode and child file name
    fn resolve_parent(&self, path: &Path) -> Result<(FnodeRef, String)> {
        let parent_path = path.parent().ok_or(Error::IsRoot)?;
        let file_name = path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or(Error::InvalidPath)?;
        let parent = self.resolve(parent_path)?;
        Ok((parent, file_name.to_string()))
    }

    /// Open fnode
    pub fn open_fnode(&mut self, path: &Path) -> Result<Handle> {
        let fnode = self.resolve(path)?;
        Ok(Handle {
            fnode,
            store: self.store.clone(),
            txmgr: self.txmgr.clone(),
            vol: self.vol.clone(),
            shutter: self.shutter.clone(),
        })
    }

    /// Create fnode
    pub fn create_fnode(
        &mut self,
        path: &Path,
        ftype: FileType,
        version_limit: Option<u8>,
    ) -> Result<FnodeRef> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let (parent, name) = self.resolve_parent(path)?;

        {
            let parent = parent.read().unwrap();
            if !parent.is_dir() {
                return Err(Error::NotDir);
            }
            if parent.has_child(&name) {
                return Err(Error::AlreadyExists);
            }
        }

        let mut fnode = FnodeRef::default();
        let tx_handle = TxMgr::begin_trans(&self.txmgr)?;
        tx_handle.run_all(|| {
            fnode = Fnode::new_under(
                &parent,
                &name,
                ftype,
                version_limit.unwrap_or(self.version_limit),
                &self.txmgr,
            )?;
            Ok(())
        })?;

        Ok(fnode)
    }

    /// Recursively create directories along the path
    pub fn create_dir_all(&mut self, path: &Path) -> Result<()> {
        match self.create_fnode(path, FileType::Dir, None) {
            Ok(_) => return Ok(()),
            Err(ref e) if *e == Error::NotFound => {}
            Err(err) => return Err(err),
        }
        match path.parent() {
            Some(p) => self.create_dir_all(p)?,
            None => return Err(Error::IsRoot),
        }
        self.create_fnode(path, FileType::Dir, None)?;
        Ok(())
    }

    /// Read directory entries
    pub fn read_dir(&self, path: &Path) -> Result<Vec<DirEntry>> {
        let parent = self.resolve(path)?;
        Fnode::read_dir(parent, path, &self.fcache, &self.vol)
    }

    /// Get metadata of specified path
    pub fn metadata(&self, path: &Path) -> Result<Metadata> {
        let fnode_ref = self.resolve(path)?;
        let fnode = fnode_ref.read().unwrap();
        Ok(fnode.metadata())
    }

    /// Get file version list of specified path
    pub fn history(&self, path: &Path) -> Result<Vec<Version>> {
        let fnode_ref = self.resolve(path)?;
        let fnode = fnode_ref.read().unwrap();
        if fnode.is_dir() {
            return Err(Error::IsDir);
        }
        Ok(fnode.history())
    }

    /// Copy a regular file to another
    pub fn copy(&mut self, from: &Path, to: &Path) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let src = self.open_fnode(from)?;
        {
            let fnode = src.fnode.read().unwrap();
            if !fnode.is_file() {
                return Err(Error::NotFile);
            }
        }

        let tgt = {
            match self.open_fnode(to) {
                Ok(tgt) => {
                    // if target and source is same fnode, do nothing
                    if Arc::ptr_eq(&tgt.fnode, &src.fnode) {
                        return Ok(());
                    }

                    {
                        let fnode = tgt.fnode.read().unwrap();
                        if !fnode.is_file() {
                            return Err(Error::NotFile);
                        }
                    }
                    tgt
                }
                Err(ref err) if *err == Error::NotFound => {
                    self.create_fnode(to, FileType::File, None)?;
                    self.open_fnode(to)?
                }
                Err(err) => return Err(err),
            }
        };

        // begin and run transaction
        let tx_handle = TxMgr::begin_trans(&self.txmgr)?;
        tx_handle.run_all(|| {
            // truncate target file
            Fnode::set_len(tgt.clone(), 0, tx_handle.txid)?;

            // copy data from source to target
            let mut rdr = FnodeReader::new_current(src.fnode.clone())?;
            let mut wtr = FnodeWriter::new(tgt.clone(), tx_handle.txid)?;
            io::copy(&mut rdr, &mut wtr)?;
            wtr.finish()?;

            Ok(())
        })?;

        Ok(())
    }

    /// Remove a regular file
    pub fn remove_file(&mut self, path: &Path) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let fnode_ref = self.resolve(path)?;
        {
            let fnode = fnode_ref.read().unwrap();
            if !fnode.is_file() {
                return Err(Error::NotFile);
            }
        }

        // begin and run transaction
        let tx_handle = TxMgr::begin_trans(&self.txmgr)?;
        tx_handle.run_all(move || {
            Fnode::unlink(&fnode_ref)?;
            let mut fnode = fnode_ref.write().unwrap();
            fnode.make_mut()?.clear_vers()?;
            fnode.make_del()?;
            self.fcache.remove(fnode.id());
            Ok(())
        })?;

        Ok(())
    }

    /// Remove an existing empty directory
    pub fn remove_dir(&mut self, path: &Path) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let fnode_ref = self.resolve(path)?;
        {
            let fnode = fnode_ref.read().unwrap();
            if !fnode.is_dir() {
                return Err(Error::NotDir);
            }
            if fnode.is_root() {
                return Err(Error::IsRoot);
            }
            if fnode.children_cnt() > 0 {
                return Err(Error::NotEmpty);
            }
        }

        // begin and run transaction
        let tx_handle = TxMgr::begin_trans(&self.txmgr)?;
        tx_handle.run_all(move || {
            Fnode::unlink(&fnode_ref)?;
            let mut fnode = fnode_ref.write().unwrap();
            fnode.make_del()?;
            self.fcache.remove(fnode.id());
            Ok(())
        })?;

        Ok(())
    }

    /// Remove an existing directory recursively
    pub fn remove_dir_all(&mut self, path: &Path) -> Result<()> {
        for child in self.read_dir(path)? {
            let child_path = child.path();
            match child.metadata().file_type() {
                FileType::File => self.remove_file(&child_path)?,
                FileType::Dir => self.remove_dir_all(&child_path)?,
            }
        }
        match self.remove_dir(path) {
            Ok(_) => Ok(()),
            Err(ref err) if *err == Error::IsRoot => Ok(()),
            Err(err) => Err(err),
        }
    }

    /// Rename a file or directory to new name
    pub fn rename(&mut self, from: &Path, to: &Path) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        if from == to {
            return Ok(());
        }

        if to.starts_with(from) {
            return Err(Error::InvalidArgument);
        }

        let src = self.open_fnode(from)?;

        {
            let src_fnode = src.fnode.read().unwrap();
            if src_fnode.is_root() {
                return Err(Error::IsRoot);
            }

            if let Ok(tgt_handle) = self.open_fnode(to) {
                let tgt_fnode = tgt_handle.fnode.read().unwrap();
                if tgt_fnode.is_root() {
                    return Err(Error::IsRoot);
                }
                if src_fnode.is_file() && tgt_fnode.is_dir() {
                    return Err(Error::IsDir);
                }
                if src_fnode.is_dir() {
                    if tgt_fnode.is_file() {
                        return Err(Error::NotDir);
                    }
                    if tgt_fnode.children_cnt() > 0 {
                        return Err(Error::NotEmpty);
                    }
                }
            }
        }

        let (tgt_parent, name) = self.resolve_parent(to)?;

        // begin and run transaction
        TxMgr::begin_trans(&self.txmgr)?.run_all(|| {
            // remove from source
            Fnode::unlink(&src.fnode)?;

            // remove target if it exists
            if let Ok(tgt_handle) = self.open_fnode(to) {
                Fnode::unlink(&tgt_handle.fnode)?;
                let mut tgt_fnode = tgt_handle.fnode.write().unwrap();
                if tgt_fnode.is_file() {
                    tgt_fnode.make_mut()?.clear_vers()?;
                }
                tgt_fnode.make_del()?;
                self.fcache.remove(tgt_fnode.id());
            }

            // and then add to target
            Fnode::add_child(&tgt_parent, &src.fnode, &name)
        })
    }
}

impl Drop for Fs {
    fn drop(&mut self) {
        let mut shutter = self.shutter.write().unwrap();
        shutter.close();

        debug!("repo closed");
    }
}
