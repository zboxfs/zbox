use std::fmt::Debug;
use std::marker::PhantomData;

use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};

use super::local_cache::LocalCacheRef;
use base::crypto::{Crypto, Key};
use error::Result;
use trans::Eid;
use volume::storage::index_mgr::Accessor;
use volume::ArmAccess;

#[derive(Debug, Default)]
pub struct IndexAccessor<T> {
    local_cache: LocalCacheRef,
    crypto: Crypto,
    key: Key,
    _t: PhantomData<T>,
}

impl<T> IndexAccessor<T> {
    const DIR_NAME: &'static str = "index";

    pub fn new(local_cache: &LocalCacheRef) -> Self {
        IndexAccessor {
            local_cache: local_cache.clone(),
            crypto: Crypto::default(),
            key: Key::new_empty(),
            _t: PhantomData,
        }
    }
}

impl<'de, T> Accessor for IndexAccessor<T>
where
    T: ArmAccess<'de> + Debug + Sync + Send,
{
    type Item = T;

    #[inline]
    fn set_crypto_ctx(&mut self, crypto: Crypto, key: Key) {
        self.crypto = crypto;
        self.key = key;
    }

    fn load(&self, id: &Eid) -> Result<Self::Item> {
        let mut local_cache = self.local_cache.write().unwrap();
        let rel_path = id.to_path_buf(Self::DIR_NAME);
        let buf = local_cache.get_index(&rel_path)?;
        let buf = self.crypto.decrypt(&buf, &self.key)?;
        let mut de = Deserializer::new(&buf[..]);
        let ret: Self::Item = Deserialize::deserialize(&mut de)?;
        Ok(ret)
    }

    fn save(&self, item: &mut Self::Item) -> Result<()> {
        let mut local_cache = self.local_cache.write().unwrap();
        let rel_path = item.id().to_path_buf(Self::DIR_NAME);
        let mut buf = Vec::new();
        item.serialize(&mut Serializer::new(&mut buf)).unwrap();
        let buf = self.crypto.encrypt(&buf, &self.key)?;
        local_cache.put_index(&rel_path, &buf)
    }

    fn remove(&self, id: &Eid) -> Result<()> {
        let mut local_cache = self.local_cache.write().unwrap();
        let rel_path = id.to_path_buf(Self::DIR_NAME);
        local_cache.del_index(&rel_path)
    }
}
