use std::cmp::Ordering;
use std::fmt::{self, Debug};
use std::iter::FromIterator;
use std::ops::Deref;

use linked_hash_map::LinkedHashMap;

use base::crypto::{Crypto, Key};
use base::lru::{CountMeter, Lru, PinChecker};
use error::{Error, Result};
use trans::{Eid, Id};
use volume::{Arm, ArmAccess, Seq};

pub trait Accessor: Send + Sync {
    type Item;

    fn set_crypto_ctx(&mut self, crypto: Crypto, key: Key);
    fn load(&self, id: &Eid) -> Result<Self::Item>;
    fn save(&self, item: &mut Self::Item) -> Result<()>;
    fn remove(&self, id: &Eid) -> Result<()>;
}

type LsmtArmor = Box<Accessor<Item = Lsmt>>;
type MemTabArmor = Box<Accessor<Item = MemTab>>;
type TabArmor = Box<Accessor<Item = Tab>>;

#[derive(Clone, Default, Eq, Deserialize, Serialize)]
pub struct TabItem((Eid, Vec<u8>));

impl TabItem {
    #[inline]
    fn id(&self) -> &Eid {
        &(self.0).0
    }

    #[inline]
    fn addr(&self) -> &[u8] {
        &(self.0).1
    }
}

impl Ord for TabItem {
    fn cmp(&self, other: &TabItem) -> Ordering {
        self.id().cmp(other.id())
    }
}

impl PartialOrd for TabItem {
    #[inline]
    fn partial_cmp(&self, other: &TabItem) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for TabItem {
    #[inline]
    fn eq(&self, other: &TabItem) -> bool {
        self.id() == other.id()
    }
}

impl Debug for TabItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("TabItem")
            .field("eid", self.id())
            .field("addr.len", &self.addr().len())
            .finish()
    }
}

#[derive(Clone, Default, Deserialize, Serialize)]
pub struct Tab {
    id: Eid,
    seq: u64,
    arm: Arm,
    items: Vec<TabItem>,
}

impl Tab {
    #[inline]
    fn new() -> Self {
        Tab {
            id: Eid::new(),
            seq: 0,
            arm: Arm::default(),
            items: Vec::new(),
        }
    }

    #[inline]
    fn with_capacity(cap: usize) -> Self {
        Tab {
            id: Eid::new(),
            seq: 0,
            arm: Arm::default(),
            items: Vec::with_capacity(cap),
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.items.len()
    }

    #[inline]
    fn append(&mut self, other: &mut Tab) {
        self.items.append(&mut other.items)
    }

    #[inline]
    fn extend_from_slice(&mut self, other: &[TabItem]) {
        self.items.extend_from_slice(other)
    }

    #[inline]
    fn sort_unstable(&mut self) {
        // sort by eid
        self.items.sort_unstable_by(|a, b| a.0.cmp(&b.0))
    }

    fn search(&self, id: &Eid) -> Option<Vec<u8>> {
        self.items
            .binary_search_by(|item| item.id().cmp(id))
            .map(|idx| self[idx].addr().to_vec())
            .ok()
    }

    // divide tab to equal-sized tabs
    fn divide(&self, chunk_size: usize) -> Vec<Tab> {
        let mut ret = Vec::new();
        for chunk in self.items.chunks(chunk_size) {
            let mut tab = Tab::with_capacity(chunk_size);
            tab.extend_from_slice(chunk);
            ret.push(tab);
        }
        ret
    }

    // merge sorted tabs
    fn merge(&self, other: &Tab) -> Tab {
        let mut merged = Tab::with_capacity(self.len() + other.len());
        let (mut i, mut j) = (0, 0);

        // sorted merge with the other tab
        while i < self.len() && j < other.len() {
            let low = &self[i];
            let high = &other[j];

            match low.cmp(high) {
                Ordering::Less => {
                    merged.items.push(low.clone());
                    i += 1;
                }
                Ordering::Equal => {
                    // empty address is deletion mark, if the address is
                    // deleted, skip it
                    if !low.addr().is_empty() {
                        merged.items.push(low.clone());
                    }

                    i += 1;
                    j += 1;
                }
                Ordering::Greater => {
                    merged.items.push(high.clone());
                    j += 1;
                }
            }
        }

        if i < self.len() {
            merged.extend_from_slice(&self[i..]);
        }
        if j < other.len() {
            merged.extend_from_slice(&other[j..]);
        }

        merged
    }
}

impl Id for Tab {
    #[inline]
    fn id(&self) -> &Eid {
        &self.id
    }

    #[inline]
    fn id_mut(&mut self) -> &mut Eid {
        &mut self.id
    }
}

impl Seq for Tab {
    #[inline]
    fn seq(&self) -> u64 {
        self.seq
    }

    #[inline]
    fn inc_seq(&mut self) {
        self.seq += 1
    }
}

impl<'de> ArmAccess<'de> for Tab {
    #[inline]
    fn arm(&self) -> Arm {
        self.arm
    }

    #[inline]
    fn arm_mut(&mut self) -> &mut Arm {
        &mut self.arm
    }
}

impl Deref for Tab {
    type Target = [TabItem];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.items.deref()
    }
}

impl FromIterator<TabItem> for Tab {
    fn from_iter<I: IntoIterator<Item = TabItem>>(iter: I) -> Self {
        let mut ret = Tab::new();
        for i in iter {
            ret.items.push(i);
        }
        ret
    }
}

impl Debug for Tab {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Tab")
            .field("id", &self.id)
            .field("seq", &self.seq)
            .field("arm", &self.arm)
            .field("items.len", &self.items.len())
            .finish()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct TabInfo {
    id: Eid,
    begin: Eid,
    end: Eid,
    cnt: usize,
}

impl TabInfo {
    fn new(tab: &Tab) -> Self {
        TabInfo {
            id: tab.id().clone(),
            begin: tab.first().unwrap().id().clone(),
            end: tab.last().unwrap().id().clone(),
            cnt: tab.len(),
        }
    }

    #[inline]
    fn contains(&self, id: &Eid) -> bool {
        self.begin <= *id && *id <= self.end
    }

    #[inline]
    fn is_overlapping(&self, begin: &Eid, end: &Eid) -> bool {
        !(*end < self.begin || self.end < *begin)
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct Level {
    num: usize,
    tabs: Vec<TabInfo>,
}

impl Level {
    const ITEM_CAP_BASE: usize = 4096;

    fn new(num: usize) -> Self {
        Level {
            num,
            tabs: Vec::new(),
        }
    }

    // calculate item number cap for a specified level
    // level 0 is 4k and each other level is 10 times of previous level
    #[inline]
    fn item_cap(lvl_num: usize) -> usize {
        Self::ITEM_CAP_BASE * 10_usize.pow(lvl_num as u32)
    }

    #[inline]
    fn item_cnt(&self) -> usize {
        self.tabs.iter().map(|t| t.cnt).sum()
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.item_cnt() >= Self::item_cap(self.num)
    }

    fn find_tabs_contain(&self, id: &Eid) -> Vec<Eid> {
        self.tabs
            .iter()
            .rev()
            .filter(|t| t.contains(id))
            .map(|t| t.id.clone())
            .collect()
    }

    #[inline]
    fn push(&mut self, tab: &Tab) {
        let tab_info = TabInfo::new(tab);
        self.tabs.push(tab_info);
    }

    #[inline]
    fn remove(&mut self, tab_id: &Eid) {
        let pos = self.tabs.iter().position(|t| t.id == *tab_id).unwrap();
        self.tabs.remove(pos);
    }

    fn clear(&mut self, tab_armor: &TabArmor) -> Result<()> {
        for tab_info in self.tabs.iter() {
            tab_armor.remove(&tab_info.id)?;
        }
        self.tabs.clear();
        Ok(())
    }
}

// Log Structured Merge Tree
//
// Inspired from LevelDB implementation.
// https://github.com/google/leveldb/blob/master/doc/impl.md
#[derive(Deserialize, Serialize)]
pub struct Lsmt {
    id: Eid,
    seq: u64,
    arm: Arm,
    lvls: Vec<Level>,

    #[serde(skip_serializing, skip_deserializing, default)]
    tab_cache: Lru<Eid, Tab, CountMeter<Tab>, PinChecker<Tab>>,
}

impl Lsmt {
    const TAB_CNT_BASE: usize = 4;
    const TAB_CACHE_SIZE: usize = 4;

    fn new() -> Self {
        Lsmt {
            id: Eid::new_empty(),
            seq: 0,
            arm: Arm::default(),
            lvls: vec![Level::new(0)],
            tab_cache: Lru::new(Self::TAB_CACHE_SIZE),
        }
    }

    fn open(&mut self, lsmt_armor: &LsmtArmor) -> Result<()> {
        let lsmt = lsmt_armor.load(&self.id)?;
        self.seq = lsmt.seq;
        self.arm = lsmt.arm;
        self.lvls = lsmt.lvls;
        Ok(())
    }

    fn get_address(
        &mut self,
        id: &Eid,
        tab_armor: &TabArmor,
    ) -> Result<Vec<u8>> {
        for lvl_idx in 0..self.lvls.len() {
            let lvl = &self.lvls[lvl_idx];

            for tab_id in lvl.find_tabs_contain(id) {
                if !self.tab_cache.contains_key(&tab_id) {
                    // load tab into cache
                    let tab = tab_armor.load(&tab_id)?;
                    self.tab_cache.insert(tab_id.clone(), tab);
                }

                let tab = self.tab_cache.get_refresh(&tab_id).unwrap();

                if let Some(addr) = tab.search(id) {
                    // empty address is deletion mark
                    if addr.is_empty() {
                        return Err(Error::NotFound);
                    }
                    return Ok(addr.clone());
                }
            }
        }
        Err(Error::NotFound)
    }

    // read all tabs in specified level
    fn read_all_tabs_in_level(
        &self,
        lvl_num: usize,
        tab_armor: &TabArmor,
    ) -> Result<Tab> {
        let lvl = &self.lvls[lvl_num];
        let mut ret = Tab::with_capacity(lvl.item_cnt());

        for tab_info in lvl.tabs.iter() {
            let mut tab = tab_armor.load(&tab_info.id)?;
            ret.append(&mut tab);
        }

        // if it is level 0, tabs may be overlapping so we need to
        // sort all items
        if lvl_num == 0 {
            ret.sort_unstable();
        }

        Ok(ret)
    }

    // read and combine specified tabs
    fn combine_tabs(
        &self,
        tabs: &[TabInfo],
        tab_armor: &TabArmor,
    ) -> Result<Tab> {
        let mut ret = Tab::new();
        for tab_info in tabs.iter() {
            let mut tab = tab_armor.load(&tab_info.id)?;
            ret.append(&mut tab);
        }
        Ok(ret)
    }

    // find all overlapping tabs in specified level
    fn find_overlapping(&self, lvl_num: usize, tab: &Tab) -> Vec<TabInfo> {
        let lvl = &self.lvls[lvl_num];
        let begin = tab.items.first().unwrap();
        let end = tab.items.last().unwrap();
        lvl.tabs
            .iter()
            .filter(|t| t.is_overlapping(begin.id(), end.id()))
            .cloned()
            .collect()
    }

    // compact current level tab against next level
    fn compact(&mut self, curr: usize, tab_armor: &TabArmor) -> Result<()> {
        // combine all tabs in current level
        let mut tab = self.read_all_tabs_in_level(curr, tab_armor)?;

        let next = curr + 1;

        // next level is not created yet
        if next >= self.lvls.len() {
            debug!(
                "compaction: {} -> {} (new), tab.len: {}",
                curr,
                next,
                tab.len()
            );
            // save merged tab and clear current level
            tab_armor.save(&mut tab)?;
            self.lvls[curr].clear(tab_armor)?;

            // create the next level
            let mut new_lvl = Level::new(next);
            new_lvl.push(&tab);
            self.lvls.push(new_lvl);

            return Ok(());
        }

        debug!("compaction: {} -> {}, tab.len: {}", curr, next, tab.len());

        // read overlapping tabs from next level and merge with the combined
        // tab from current level
        let overlap = self.find_overlapping(next, &tab);
        let overlap_tab = self.combine_tabs(&overlap, tab_armor)?;
        let merged = tab.merge(&overlap_tab);

        // remove overlapping tabs in next level
        for tab_info in overlap.iter() {
            tab_armor.remove(&tab_info.id)?;
            self.lvls[next].remove(&tab_info.id);
        }

        // save merged tab to next level
        let item_cap =
            Level::item_cap(next) / (Self::TAB_CNT_BASE * (next + 1));
        for mut tab in merged.divide(item_cap) {
            tab_armor.save(&mut tab)?;
            self.lvls[next].push(&tab);
        }

        // clear current level
        self.lvls[curr].clear(tab_armor)?;

        Ok(())
    }

    // add young tab to lsmt
    fn push_young(
        &mut self,
        young: &mut Tab,
        tab_armor: &TabArmor,
    ) -> Result<()> {
        // save young tab and push young tab to level 0
        tab_armor.save(young)?;
        self.lvls[0].push(young);

        // iterate all levels and try to do compaction
        let lvl_cnt = self.lvls.len();
        for curr in 0..lvl_cnt {
            // if current level is full then do compaction
            if self.lvls[curr].is_full() {
                self.compact(curr, tab_armor)?;
            }
        }

        Ok(())
    }
}

impl Id for Lsmt {
    #[inline]
    fn id(&self) -> &Eid {
        &self.id
    }

    #[inline]
    fn id_mut(&mut self) -> &mut Eid {
        &mut self.id
    }
}

impl Seq for Lsmt {
    #[inline]
    fn seq(&self) -> u64 {
        self.seq
    }

    #[inline]
    fn inc_seq(&mut self) {
        self.seq += 1
    }
}

impl<'de> ArmAccess<'de> for Lsmt {
    #[inline]
    fn arm(&self) -> Arm {
        self.arm
    }

    #[inline]
    fn arm_mut(&mut self) -> &mut Arm {
        &mut self.arm
    }
}

impl Debug for Lsmt {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Lsmt")
            .field("id", &self.id)
            .field("seq", &self.seq)
            .field("arm", &self.arm)
            .field("lvls", &self.lvls)
            .finish()
    }
}

// memory table
#[derive(Deserialize, Serialize)]
pub struct MemTab {
    id: Eid,
    seq: u64,
    arm: Arm,
    map: LinkedHashMap<Eid, Vec<u8>>,

    #[serde(skip_serializing, skip_deserializing, default)]
    is_changed: bool,
}

impl MemTab {
    // memory table total capacity
    const CAPACITY: usize = 4 * 1024;

    // number of items resident in memory
    const RESIDENCE_CAP: usize = 3 * 1024;

    fn new() -> Self {
        MemTab {
            id: Eid::new_empty(),
            seq: 0,
            arm: Arm::default(),
            map: LinkedHashMap::new(),
            is_changed: false,
        }
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.map.len() >= Self::CAPACITY
    }

    #[inline]
    fn get_address(&mut self, id: &Eid) -> Option<&mut Vec<u8>> {
        self.map.get_refresh(id)
    }

    #[inline]
    fn insert(&mut self, id: &Eid, addr: &[u8]) {
        self.map.insert(id.clone(), addr.to_owned());
        self.is_changed = true;
    }

    // extract young tab, the memory table must be full
    fn extract_young(&self) -> Tab {
        let mut young: Tab = self
            .map
            .iter()
            .take(self.map.len() - Self::RESIDENCE_CAP)
            .map(|ent| TabItem((ent.0.clone(), ent.1.clone())))
            .collect();
        young.sort_unstable();
        young
    }

    // evict young tab from memory table
    fn evict_young(&mut self, young: &Tab) {
        for item in young.iter() {
            self.map.remove(item.id());
        }
        self.is_changed = true;
    }
}

impl Id for MemTab {
    #[inline]
    fn id(&self) -> &Eid {
        &self.id
    }

    #[inline]
    fn id_mut(&mut self) -> &mut Eid {
        &mut self.id
    }
}

impl Seq for MemTab {
    #[inline]
    fn seq(&self) -> u64 {
        self.seq
    }

    #[inline]
    fn inc_seq(&mut self) {
        self.seq += 1
    }
}

impl<'de> ArmAccess<'de> for MemTab {
    #[inline]
    fn arm(&self) -> Arm {
        self.arm
    }

    #[inline]
    fn arm_mut(&mut self) -> &mut Arm {
        &mut self.arm
    }
}

impl Debug for MemTab {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("MemTab")
            .field("id", &self.id)
            .field("seq", &self.seq)
            .field("arm", &self.arm)
            .field("map.len", &self.map.len())
            .finish()
    }
}

// Index manager
pub struct IndexMgr {
    lsmt: Lsmt,
    memtab: MemTab,
    lsmt_armor: LsmtArmor,
    memtab_armor: MemTabArmor,
    tab_armor: TabArmor,
}

impl IndexMgr {
    // subkey ids
    const SUBKEY_ID_LSMT: u64 = 17;
    const SUBKEY_ID_MEMTAB: u64 = 18;
    const SUBKEY_ID_TAB: u64 = 19;

    pub fn new(
        lsmt_armor: LsmtArmor,
        memtab_armor: MemTabArmor,
        tab_armor: TabArmor,
    ) -> Self {
        IndexMgr {
            lsmt: Lsmt::new(),
            memtab: MemTab::new(),
            lsmt_armor,
            memtab_armor,
            tab_armor,
        }
    }

    pub fn set_crypto_ctx(&mut self, crypto: Crypto, key: Key) {
        let sub_key = key.derive(Self::SUBKEY_ID_LSMT);
        *self.lsmt.id_mut() = Eid::from_slice(sub_key.derive(0).as_slice());
        self.lsmt_armor.set_crypto_ctx(crypto.clone(), sub_key);

        let sub_key = key.derive(Self::SUBKEY_ID_MEMTAB);
        *self.memtab.id_mut() = Eid::from_slice(sub_key.derive(0).as_slice());
        self.memtab_armor.set_crypto_ctx(crypto.clone(), sub_key);

        let sub_key = key.derive(Self::SUBKEY_ID_TAB);
        self.tab_armor.set_crypto_ctx(crypto.clone(), sub_key);
    }

    pub fn init(&mut self) -> Result<()> {
        self.lsmt_armor.save(&mut self.lsmt)?;
        self.memtab_armor.save(&mut self.memtab)?;
        Ok(())
    }

    pub fn open(&mut self) -> Result<()> {
        self.lsmt.open(&self.lsmt_armor)?;
        self.memtab = self.memtab_armor.load(self.memtab.id())?;
        Ok(())
    }

    pub fn get(&mut self, id: &Eid) -> Result<Vec<u8>> {
        match self.memtab.get_address(id) {
            Some(addr) => {
                // empty address is a deletion mark
                if addr.is_empty() {
                    Err(Error::NotFound)
                } else {
                    Ok(addr.clone())
                }
            }
            None => self.lsmt.get_address(id, &self.tab_armor),
        }
    }

    pub fn insert(&mut self, id: &Eid, addr: &[u8]) -> Result<()> {
        self.memtab.insert(id, addr);

        if !self.memtab.is_full() {
            return Ok(());
        }

        // extract young tab from memtable
        let mut young = self.memtab.extract_young();

        // push young tab to lsmt and save lsmt
        self.lsmt.push_young(&mut young, &self.tab_armor)?;
        self.lsmt_armor.save(&mut self.lsmt)?;

        // evict young tab from memtable
        self.memtab.evict_young(&young);

        Ok(())
    }

    #[inline]
    pub fn delete(&mut self, id: &Eid) -> Result<()> {
        self.insert(id, &[])
    }

    #[inline]
    pub fn flush(&mut self) -> Result<()> {
        if self.memtab.is_changed {
            self.memtab_armor.save(&mut self.memtab)?;
            self.memtab.is_changed = false;
        }
        Ok(())
    }
}

impl Debug for IndexMgr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("IndexMgr")
            .field("lsmt", &self.lsmt)
            .field("memtab", &self.memtab)
            .finish()
    }
}
