#![allow(clippy::module_inception)]

use std::cmp::min;
use std::collections::VecDeque;
use std::fmt::{self, Debug};
use std::io::{Read, Result as IoResult, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use super::{Handle, Options};
use crate::base::lru::{CountMeter, Lru, PinChecker};
use crate::base::Time;
use crate::content::{
    ChunkMap, Content, ContentReader, Store, StoreRef, StoreWeakRef,
    Writer as StoreWriter,
};
use crate::error::{Error, Result};
use crate::trans::cow::{Cow, CowCache, CowRef, CowWeakRef, Cowable, IntoCow};
use crate::trans::trans::{Action, Transable};
use crate::trans::{Eid, Id, TxMgrRef, Txid};
use crate::volume::VolumeRef;

// maximum sub nodes for a fnode
const SUB_NODES_CNT: usize = 8;

/// A structure representing a type of file with accessors for each file type.
#[derive(Debug, Copy, Clone, PartialEq, Deserialize, Serialize)]
pub enum FileType {
    File,
    Dir,
}

impl FileType {
    /// Test whether this file type represents a regular file.
    pub fn is_file(self) -> bool {
        self == FileType::File
    }

    /// Test whether this file type represents a directory.
    pub fn is_dir(self) -> bool {
        self == FileType::Dir
    }
}

impl Default for FileType {
    fn default() -> Self {
        FileType::File
    }
}

impl Into<i32> for FileType {
    fn into(self) -> i32 {
        match self {
            FileType::File => 0,
            FileType::Dir => 1,
        }
    }
}

impl Into<String> for FileType {
    fn into(self) -> String {
        match self {
            FileType::File => String::from("File"),
            FileType::Dir => String::from("Dir"),
        }
    }
}

// fnode child entry
#[derive(Debug, Clone, Deserialize, Serialize)]
struct ChildEntry {
    id: Eid,
    ftype: FileType,
    name: String,
}

impl ChildEntry {
    fn new(id: &Eid, ftype: FileType, name: &str) -> Self {
        ChildEntry {
            id: id.clone(),
            ftype,
            name: name.to_string(),
        }
    }
}

/// A representation of a permanent file content.
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct Version {
    num: usize,      // version number
    content_id: Eid, // content id
    content_len: usize,
    ctime: Time,
}

impl Version {
    fn new(num: usize, content_id: &Eid, len: usize) -> Self {
        Version {
            num,
            content_id: content_id.clone(),
            content_len: len,
            ctime: Time::now(),
        }
    }

    /// Returns the version number of this content.
    ///
    /// The version number starts from 1 and continuously increases by 1.
    pub fn num(&self) -> usize {
        self.num
    }

    /// Returns the byte length of this version of content.
    pub fn content_len(&self) -> usize {
        self.content_len
    }

    /// Returns the creation time of this version of content.
    pub fn created_at(&self) -> SystemTime {
        self.ctime.to_system_time()
    }
}

/// Metadata information about a file or a directory.
///
/// This structure is returned from the [`File::metadata`] and
/// [`Repo::metadata`] represents known metadata about a file such as its type,
/// size, modification times and etc.
///
/// [`File::metadata`]: struct.File.html#method.metadata
/// [`Repo::metadata`]: struct.Repo.html#method.metadata
#[derive(Debug, Copy, Clone)]
pub struct Metadata {
    ftype: FileType,
    content_len: usize,
    curr_version: usize,
    ctime: Time,
    mtime: Time,
}

impl Metadata {
    /// Returns the file type for this metadata.
    pub fn file_type(&self) -> FileType {
        self.ftype
    }

    /// Returns whether this metadata is for a directory.
    pub fn is_dir(&self) -> bool {
        self.ftype == FileType::Dir
    }

    /// Returns whether this metadata is for a regular file.
    pub fn is_file(&self) -> bool {
        self.ftype == FileType::File
    }

    /// Returns the size of the current version of file, in bytes, this
    /// metadata is for.
    pub fn content_len(&self) -> usize {
        self.content_len
    }

    /// Returns current version number of file listed in this metadata.
    pub fn curr_version(&self) -> usize {
        self.curr_version
    }

    /// Returns the creation time listed in this metadata.
    pub fn created_at(&self) -> SystemTime {
        self.ctime.to_system_time()
    }

    /// Returns the last modification time listed in this metadata.
    pub fn modified_at(&self) -> SystemTime {
        self.mtime.to_system_time()
    }
}

/// Entries returned by the [`read_dir`] function.
///
/// An instance of `DirEntry` represents an entry inside of a directory in the
/// repository. Each entry can be inspected via methods to learn about the
/// absolute path or other metadata.
///
/// [`read_dir`]: struct.Repo.html#method.read_dir
#[derive(Debug)]
pub struct DirEntry {
    path: PathBuf,
    name: String,
    metadata: Metadata,
}

impl DirEntry {
    /// Returns the absolute path to the file that this entry represents.
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    /// Returns the bare file name of this directory entry without any other
    /// leading path component.
    pub fn file_name(&self) -> &str {
        &self.name
    }

    /// Return the metadata for the file that this entry points at.
    pub fn metadata(&self) -> Metadata {
        self.metadata
    }
}

type SubNodes = Lru<
    String,
    FnodeWeakRef,
    CountMeter<FnodeWeakRef>,
    PinChecker<FnodeWeakRef>,
>;

/// File node
#[derive(Default, Clone, Deserialize, Serialize)]
pub struct Fnode {
    ftype: FileType,
    opts: Options,
    ctime: Time,
    mtime: Time,
    kids: Vec<ChildEntry>,
    vers: VecDeque<Version>,
    chk_map: ChunkMap,

    // parent fnode
    #[serde(skip_serializing, skip_deserializing, default)]
    parent: Option<FnodeRef>,

    #[serde(
        skip_serializing,
        skip_deserializing,
        default = "Fnode::default_sub_nodes"
    )]
    sub_nodes: SubNodes,
}

impl Fnode {
    pub fn new(ftype: FileType, opts: Options) -> Self {
        Fnode {
            ftype,
            opts,
            ctime: Time::now(),
            mtime: Time::now(),
            kids: Vec::new(),
            vers: VecDeque::new(),
            chk_map: ChunkMap::new(opts.dedup_chunk),
            parent: None,
            sub_nodes: Self::default_sub_nodes(),
        }
    }

    /// Create new fnode under parent
    pub fn new_under(
        parent: &FnodeRef,
        name: &str,
        ftype: FileType,
        opts: Options,
        txmgr: &TxMgrRef,
        store: &StoreRef,
    ) -> Result<FnodeRef> {
        let kid = {
            let mut pfnode_cow = parent.write().unwrap();
            let pfnode = pfnode_cow.make_mut(txmgr)?;
            if !pfnode.is_dir() {
                return Err(Error::NotDir);
            }

            // create child fnode and add the initial version
            let mut kid = Fnode::new(ftype, opts);
            if kid.is_file() {
                kid.add_version(Content::new(), store, txmgr)?;
            }

            kid.into_cow(txmgr)?
        };

        // add child to parent
        Fnode::add_child(parent, &kid, name, txmgr)?;

        Ok(kid)
    }

    #[inline]
    fn default_sub_nodes() -> SubNodes {
        Lru::new(SUB_NODES_CNT)
    }

    /// Check if fnode is regular file
    #[inline]
    pub fn is_file(&self) -> bool {
        self.ftype == FileType::File
    }

    /// Check if fnode is directory
    #[inline]
    pub fn is_dir(&self) -> bool {
        self.ftype == FileType::Dir
    }

    /// Check if fnode is root
    #[inline]
    pub fn is_root(&self) -> bool {
        self.parent.is_none()
    }

    /// Get fnode metadata
    pub fn metadata(&self) -> Metadata {
        Metadata {
            ftype: self.ftype,
            content_len: self.curr_len(),
            curr_version: self.curr_ver_num(),
            ctime: self.ctime,
            mtime: self.mtime,
        }
    }

    /// Get size of fnode current version
    #[inline]
    pub fn curr_len(&self) -> usize {
        match self.ftype {
            FileType::File => self.curr_ver().content_len(),
            FileType::Dir => 0,
        }
    }

    /// Get fnode version list
    #[inline]
    pub fn history(&self) -> Vec<Version> {
        Vec::from(self.vers.clone())
    }

    /// Get fnode options
    #[inline]
    pub fn get_opts(&self) -> Options {
        self.opts
    }

    /// Load root fnode
    #[inline]
    pub fn load_root(root_id: &Eid, vol: &VolumeRef) -> Result<FnodeRef> {
        let root = Cow::<Fnode>::load(root_id, vol)?;
        Ok(root)
    }

    // load one child fnode
    fn load_child(
        &mut self,
        name: &str,
        self_ref: FnodeRef,
        cache: &Cache,
        vol: &VolumeRef,
    ) -> Result<FnodeRef> {
        // get child fnode from sub node list first
        if let Some(fnode) = self
            .sub_nodes
            .get_refresh(name)
            .and_then(|sub| sub.upgrade())
        {
            return Ok(fnode);
        }

        // if child is not in sub node list, load it from fnode cache
        self.kids
            .iter()
            .find(|ref c| c.name == name)
            .ok_or(Error::NotFound)
            .and_then(|child| cache.get(&child.id, vol).map_err(Error::from))
            .and_then(|child| {
                // set parent for the child
                {
                    let mut child_cow = child.write().unwrap();
                    let c = child_cow.make_mut_naive();
                    c.parent = Some(self_ref);
                }

                // add to parent's sub node list
                self.sub_nodes
                    .insert(name.to_string(), Arc::downgrade(&child));
                Ok(child)
            })
    }

    #[inline]
    pub fn has_child(&self, name: &str) -> bool {
        self.kids.iter().any(|ref c| c.name == name)
    }

    #[inline]
    pub fn children_cnt(&self) -> usize {
        self.kids.len()
    }

    /// Get single child fnode
    pub fn child(
        parent: &FnodeRef,
        name: &str,
        cache: &Cache,
        vol: &VolumeRef,
    ) -> Result<FnodeRef> {
        let mut par = parent.write().unwrap();
        par.make_mut_naive()
            .load_child(name, parent.clone(), cache, vol)
    }

    fn children_names(&self) -> Vec<String> {
        self.kids.iter().map(|ref k| k.name.clone()).collect()
    }

    /// Get children dir entry list
    pub fn read_dir(
        parent: FnodeRef,
        path: &Path,
        cache: &Cache,
        vol: &VolumeRef,
    ) -> Result<Vec<DirEntry>> {
        let mut par = parent.write().unwrap();
        let par = par.make_mut_naive();
        if !par.is_dir() {
            return Err(Error::NotDir);
        }

        let parent_path = {
            #[cfg(windows)]
            {
                let mut path_str = path.to_str().unwrap().to_string();
                if !path_str.ends_with("/") {
                    path_str.push_str("/");
                }
                PathBuf::from(path_str)
            }
            #[cfg(not(windows))]
            {
                path
            }
        };

        let mut ret = Vec::new();
        let child_names = par.children_names();

        for name in child_names.iter() {
            let child_ref =
                par.load_child(&name, parent.clone(), cache, vol)?;
            let child = child_ref.read().unwrap();
            ret.push(DirEntry {
                path: parent_path.join(name),
                metadata: child.metadata(),
                name: name.clone(),
            });
        }

        Ok(ret)
    }

    /// Add child to parent fnode
    pub fn add_child(
        parent: &FnodeRef,
        child: &FnodeRef,
        name: &str,
        txmgr: &TxMgrRef,
    ) -> Result<()> {
        let mut parent_cow = parent.write().unwrap();
        let par = parent_cow.make_mut(txmgr)?;

        // add to child to parent's children list
        let mut kid = child.write().unwrap();
        par.kids.push(ChildEntry::new(kid.id(), kid.ftype, name));

        // update child's parent
        kid.make_mut(txmgr)?.parent = Some(parent.clone());

        // add to parent's sub node list and update modified time
        par.sub_nodes
            .insert(name.to_string(), Arc::downgrade(child));
        par.mtime = Time::now();

        Ok(())
    }

    /// Remove child fnode from parent
    pub fn remove_from_parent(
        fnode: &FnodeRef,
        txmgr: &TxMgrRef,
    ) -> Result<()> {
        let child = fnode.read().unwrap();
        match child.parent {
            Some(ref parent) => {
                let mut par = parent.write().unwrap();
                let par = par.make_mut(txmgr)?;
                let child_idx = par
                    .kids
                    .iter()
                    .position(|ref c| c.id == *child.id())
                    .ok_or(Error::NotFound)?;
                {
                    let name = &par.kids[child_idx].name;
                    par.sub_nodes.remove(name);
                }
                par.kids.remove(child_idx);
                Ok(())
            }
            None => Err(Error::IsRoot),
        }
    }

    /// get a specified version
    pub fn ver(&self, ver_num: usize) -> Option<&Version> {
        self.vers.iter().find(|v| v.num == ver_num)
    }

    // get current version
    fn curr_ver(&self) -> &Version {
        self.vers.back().unwrap()
    }

    /// Get current version number
    pub fn curr_ver_num(&self) -> usize {
        if self.vers.is_empty() {
            return 0;
        }
        self.curr_ver().num
    }

    // remove a specified version and its associated content
    fn remove_version(
        &mut self,
        ver_num: usize,
        store: &StoreRef,
        txmgr: &TxMgrRef,
    ) -> Result<()> {
        let idx = self
            .vers
            .iter()
            .position(|v| v.num == ver_num)
            .ok_or(Error::NoVersion)?;
        let ver = self.vers.remove(idx).unwrap();

        if let Some(ctn) = Store::deref_content(store, &ver.content_id)? {
            // content is not used anymore, remove it
            let mut content = ctn.write().unwrap();
            content.unlink(&mut self.chk_map, store, txmgr)?;
            content.make_del(txmgr)?;
        }

        Ok(())
    }

    pub fn clear_versions(
        &mut self,
        store: &StoreRef,
        txmgr: &TxMgrRef,
    ) -> Result<()> {
        let ver_nums: Vec<usize> = self.vers.iter().map(|v| v.num).collect();
        for ver_num in ver_nums {
            self.remove_version(ver_num, store, txmgr)?;
        }
        Ok(())
    }

    // add a new content version to fnode
    // return true if the content is not duplicated, otherwise return false
    pub fn add_version(
        &mut self,
        content: Content,
        store: &StoreRef,
        txmgr: &TxMgrRef,
    ) -> Result<bool> {
        assert!(self.is_file());

        // try to dedup content in store
        let (no_dup, deduped_id) = Store::dedup_content(store, &content)?;

        // create a new version and append to version list
        let ver =
            Version::new(self.curr_ver_num() + 1, &deduped_id, content.len());
        self.mtime = ver.ctime;
        self.vers.push_back(ver);

        // if content is not duplicated, link the content
        if no_dup {
            content.link(store, txmgr)?;
        }

        // evict retired version if any
        if self.vers.len() > self.opts.version_limit as usize {
            let retire = self.vers.front().unwrap().num;
            self.remove_version(retire, store, txmgr)?;
        }

        Ok(no_dup)
    }

    /// Get reader for sepcified version number
    pub fn version_reader(
        &self,
        ver_num: usize,
        store: &StoreWeakRef,
    ) -> Result<ContentReader> {
        let ver = self.ver(ver_num).ok_or(Error::NoVersion)?;
        let content = {
            let store = store.upgrade().ok_or(Error::RepoClosed)?;
            let st = store.read().unwrap();
            let ctn_ref = st.get_content(&ver.content_id)?;
            let ctn = ctn_ref.read().unwrap();
            ctn.clone()
        };
        Ok(ContentReader::new(content, store))
    }

    /// Clone a new current content
    pub fn clone_current_content(&self, store: &StoreRef) -> Result<Content> {
        let store = store.read().unwrap();
        let curr_ctn = store.get_content(&self.curr_ver().content_id)?;
        let content = curr_ctn.read().unwrap();
        Ok(content.clone())
    }

    /// Set file to specified length
    ///
    /// if new length is equal to old length, do nothing
    pub fn set_len(handle: Handle, len: usize, txid: Txid) -> Result<()> {
        let curr_len = {
            let fnode = handle.fnode.read().unwrap();
            fnode.curr_len()
        };

        if curr_len < len {
            // append
            let mut size = len - curr_len;
            let buf = vec![0u8; min(size, 16 * 1024)];
            let mut wtr = Writer::new(handle.clone(), txid)?;
            wtr.seek(SeekFrom::Start(curr_len as u64))?;

            while size > 0 {
                let write_len = min(size, buf.len());
                let written = wtr.write(&buf[..write_len])?;
                size -= written;
            }
            wtr.finish()?;
        } else if curr_len > len {
            // truncate
            let store = handle.store.upgrade().ok_or(Error::RepoClosed)?;
            let txmgr = handle.txmgr.upgrade().ok_or(Error::RepoClosed)?;
            let mut fnode_cow = handle.fnode.write().unwrap();
            let new_ctn = {
                let mut ctn = fnode_cow.clone_current_content(&store)?;
                ctn.truncate(len, &store)?;
                ctn
            };

            // dedup content, if it is not duplicated then link the content
            let fnode = fnode_cow.make_mut(&txmgr)?;
            fnode.add_version(new_ctn, &store, &txmgr)?;
        }

        Ok(())
    }
}

impl Debug for Fnode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Fnode")
            .field("ftype", &self.ftype)
            .field("opts", &self.opts)
            .field("ctime", &self.ctime)
            .field("mtime", &self.mtime)
            .field("kids", &self.kids)
            .field("vers", &self.vers)
            .field("chk_map", &self.chk_map)
            .field("sub_nodes", &self.sub_nodes)
            .finish()
    }
}

impl Cowable for Fnode {
    fn on_commit(&mut self, _vol: &VolumeRef) -> Result<()> {
        // remove deleted fnode from sub nodes cache
        self.sub_nodes
            .entries()
            .filter(|ent| {
                ent.get()
                    .upgrade()
                    .map(|fnode_ref| {
                        let cow = fnode_ref.read().unwrap();
                        cow.in_trans() && cow.action() == Action::Delete
                    })
                    .unwrap_or(false)
            })
            .for_each(|ent| {
                ent.remove();
            });
        Ok(())
    }
}

impl<'de> IntoCow<'de> for Fnode {}

/// Fnode reference type
pub type FnodeRef = CowRef<Fnode>;

/// Fnode weak reference type
pub type FnodeWeakRef = CowWeakRef<Fnode>;

/// Fnode Reader
#[derive(Debug)]
pub struct Reader {
    ver: usize,
    rdr: ContentReader,
}

impl Reader {
    /// Create a reader for specified version
    pub fn new(
        fnode: FnodeRef,
        ver: usize,
        store: &StoreWeakRef,
    ) -> Result<Self> {
        let fnode = fnode.read().unwrap();
        let rdr = fnode.version_reader(ver, store)?;
        Ok(Reader { ver, rdr })
    }

    /// Create a reader for current version
    pub fn new_current(fnode: FnodeRef, store: &StoreWeakRef) -> Result<Self> {
        let fnode = fnode.read().unwrap();
        let ver = fnode.curr_ver_num();
        let rdr = fnode.version_reader(ver, store)?;
        Ok(Reader { ver, rdr })
    }

    #[inline]
    pub fn version_num(&self) -> usize {
        self.ver
    }
}

impl Read for Reader {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        self.rdr.read(buf)
    }
}

impl Seek for Reader {
    #[inline]
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        self.rdr.seek(pos)
    }
}

/// Fnode Writer
#[derive(Debug)]
pub struct Writer {
    inner: StoreWriter,
    handle: Handle,
}

impl Writer {
    pub fn new(handle: Handle, txid: Txid) -> Result<Self> {
        let chk_map = {
            let f = handle.fnode.read().unwrap();
            f.chk_map.clone()
        };
        let inner =
            StoreWriter::new(txid, chk_map, &handle.txmgr, &handle.store)?;
        Ok(Writer { inner, handle })
    }

    pub fn finish(self) -> Result<usize> {
        let store = self.handle.store.upgrade().ok_or(Error::RepoClosed)?;
        let txmgr = self.handle.txmgr.upgrade().ok_or(Error::RepoClosed)?;
        let (stg_ctn, chk_map) = self.inner.finish()?;
        let handle = &self.handle;

        let mut fnode_cow = handle.fnode.write().unwrap();

        // merge stage content to current content
        let merged_ctn = {
            let mut ctn = fnode_cow.clone_current_content(&store)?;
            ctn.merge_from(&stg_ctn, &store)?;
            ctn
        };

        // dedup content and add deduped content as a new version
        let fnode = fnode_cow.make_mut(&txmgr)?;
        if !fnode.add_version(merged_ctn, &store, &txmgr)? {
            // content is duplicated, weak unlink the stage content
            stg_ctn.unlink_weak(&mut fnode.chk_map, &store, &txmgr)?;
        }

        // udpate fnode chunk map
        fnode.chk_map = chk_map;

        Ok(stg_ctn.end_offset())
    }
}

impl Write for Writer {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        self.inner.write(buf)
    }

    #[inline]
    fn flush(&mut self) -> IoResult<()> {
        self.inner.flush()
    }
}

impl Seek for Writer {
    #[inline]
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        self.inner.seek(pos)
    }
}

/// Fnode cache
pub type Cache = CowCache<Fnode>;
