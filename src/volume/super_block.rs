use log::debug;
use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};

use super::storage::Storage;
use super::BLK_SIZE;
use crate::base::crypto::{Cipher, Cost, Crypto, Key, Salt, SALT_SIZE};
use crate::base::{Time, Version};
use crate::error::{Error, Result};
use crate::trans::Eid;

/// Super block head, not encrypted
#[derive(Debug, Default)]
pub(super) struct Head {
    pub salt: Salt,
    pub cost: Cost,
    pub cipher: Cipher,
}

impl Head {
    const BYTES_LEN: usize = SALT_SIZE + Cost::BYTES_LEN + Cipher::BYTES_LEN;

    fn seri(&self) -> Vec<u8> {
        let mut pos = 0;
        let mut buf = vec![0u8; Self::BYTES_LEN];
        buf[..SALT_SIZE].copy_from_slice(self.salt.as_ref());
        pos += SALT_SIZE;
        buf[pos] = self.cost.to_u8();
        pos += Cost::BYTES_LEN;
        buf[pos] = self.cipher.into();
        buf
    }

    fn deseri(buf: &[u8]) -> Result<Self> {
        if buf.len() < Self::BYTES_LEN {
            return Err(Error::InvalidSuperBlk);
        }

        let mut pos = 0;
        let salt = Salt::from_slice(&buf[..SALT_SIZE]);
        pos += SALT_SIZE;
        let cost = Cost::from_u8(buf[pos])?;
        pos += Cost::BYTES_LEN;
        let cipher = Cipher::from_u8(buf[pos])?;

        Ok(Head { salt, cost, cipher })
    }
}

/// Super block body, encrypted
#[derive(Debug, Default, Deserialize, Serialize)]
pub(super) struct Body {
    seq: u64,
    pub volume_id: Eid,
    pub ver: Version,
    pub key: Key,
    pub uri: String,
    pub compress: bool,
    pub ctime: Time,
    pub mtime: Time,
    pub payload: Vec<u8>,
}

impl Body {
    fn seri(&mut self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        self.seq += 1;
        self.mtime = Time::now();
        self.serialize(&mut Serializer::new(&mut buf))?;
        Ok(buf)
    }

    fn deseri(buf: &[u8]) -> Result<Self> {
        let mut de = Deserializer::new(buf);
        let body: Body = Deserialize::deserialize(&mut de)?;
        Ok(body)
    }
}

/// Super block
#[derive(Debug, Default)]
pub(super) struct SuperBlk {
    pub head: Head,
    pub body: Body,
}

impl SuperBlk {
    // magic numbers for body AEAD encryption
    const MAGIC: [u8; 4] = [233, 239, 241, 251];

    // save super blocks
    pub fn save(&mut self, pwd: &str, storage: &mut Storage) -> Result<()> {
        let crypto = Crypto::new(self.head.cost, self.head.cipher)?;

        // hash user specified plaintext password
        let pwd_hash = crypto.hash_pwd(pwd, &self.head.salt)?;
        let vkey = &pwd_hash.value;

        // serialize head and body
        let head_buf = self.head.seri();
        let body_buf = self.body.seri()?;

        // compose buffer: body buffer length + body buffer + padding
        let mut comp_buf = vec![0u8; 8 + body_buf.len()];
        comp_buf[..8].copy_from_slice(&((body_buf.len() as u64).to_le_bytes()));
        comp_buf[8..8 + body_buf.len()].copy_from_slice(&body_buf);

        // resize the compose buffer to make it can exactly fit in a block
        let new_len = crypto.decrypted_len(BLK_SIZE - head_buf.len());
        if comp_buf.len() > new_len {
            return Err(Error::InvalidSuperBlk);
        }
        comp_buf.resize(new_len, 0);

        // encrypt composed buffer using the volume key, which is the user
        // password hash
        let enc_buf = crypto.encrypt_with_ad(&comp_buf, vkey, &Self::MAGIC)?;

        // combine head and compose buffer and save 2 copies to storage
        let mut pos = 0;
        let mut buf = vec![0u8; head_buf.len() + enc_buf.len()];
        buf[..head_buf.len()].copy_from_slice(&head_buf);
        pos += head_buf.len();
        buf[pos..pos + enc_buf.len()].copy_from_slice(&enc_buf);
        storage
            .put_super_block(&buf, 0)
            .and(storage.put_super_block(&buf, 1))
    }

    // load a specific super block arm
    fn load_arm(suffix: u64, pwd: &str, storage: &mut Storage) -> Result<Self> {
        // read raw bytes
        let buf = storage.get_super_block(suffix)?;

        // read header
        let head = Head::deseri(&buf)?;

        // create crypto
        let crypto = Crypto::new(head.cost, head.cipher)?;

        // derive volume key and use it to decrypt body
        let pwd_hash = crypto.hash_pwd(pwd, &head.salt)?;
        let vkey = &pwd_hash.value;

        // read encryped body
        let comp_buf = crypto.decrypt_with_ad(
            &buf[Head::BYTES_LEN..],
            vkey,
            &Self::MAGIC,
        )?;
        let mut buf: [u8; 8] = Default::default();
        buf.copy_from_slice(&comp_buf[..8]);
        let body_buf_len = u64::from_le_bytes(buf) as usize;
        let body = Body::deseri(&comp_buf[8..8 + body_buf_len])?;

        Ok(SuperBlk { head, body })
    }

    // load super block from both left and right arm
    pub fn load(pwd: &str, storage: &mut Storage) -> Result<Self> {
        let left = Self::load_arm(0, pwd, storage)?;
        let right = Self::load_arm(1, pwd, storage)?;

        if left.body.seq == right.body.seq {
            Ok(left)
        } else {
            Err(Error::InvalidSuperBlk)
        }
    }

    // try to repair super block using at least one valid
    pub fn repair(pwd: &str, storage: &mut Storage) -> Result<()> {
        let left_arm = Self::load_arm(0, pwd, storage);
        let right_arm = Self::load_arm(1, pwd, storage);

        match left_arm {
            Ok(mut left) => match right_arm {
                Ok(mut right) => {
                    if left.body.volume_id != right.body.volume_id
                        || left.body.key != right.body.key
                    {
                        return Err(Error::InvalidSuperBlk);
                    }
                    if left.body.seq > right.body.seq {
                        left.save(pwd, storage)?;
                    } else if left.body.seq < right.body.seq {
                        right.save(pwd, storage)?;
                    } else {
                        debug!("super block all good, no need repair");
                        return Ok(());
                    }
                }
                Err(_) => left.save(pwd, storage)?,
            },
            Err(err) => {
                if let Ok(mut right) = right_arm {
                    right.save(pwd, storage)?;
                } else {
                    return Err(err);
                }
            }
        }

        debug!("super block repaired");

        Ok(())
    }
}
