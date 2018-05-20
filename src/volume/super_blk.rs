use bytes::{BufMut, ByteOrder, LittleEndian};

use error::{Error, Result};
use base::{Time, Version};
use base::crypto::{Cipher, Cost, Crypto, Key, Salt, KEY_SIZE, SALT_SIZE};
use trans::Eid;
use super::storage::StorageRef;

// super block static left and right arm id
static LEFT_ARM_ID: [u8; Eid::EID_SIZE] = [0xFE; Eid::EID_SIZE];
static RIGHT_ARM_ID: [u8; Eid::EID_SIZE] = [0xFC; Eid::EID_SIZE];

/// Super block
#[derive(Debug)]
pub(super) struct SuperBlk {
    seq: u64,
    pub volume_id: Eid,
    pub key: Key,
    pub crypto: Crypto,
    pub ver: Version,
    pub ctime: Time,
    pub payload: Vec<u8>,
}

impl SuperBlk {
    // header: salt + cost + cipher
    const HEADER_LEN: usize = SALT_SIZE + Cost::BYTES_LEN + Cipher::BYTES_LEN;

    // body: seq(u64) + volume id + version + ctime + master key
    const BODY_LEN: usize =
        8 + Eid::EID_SIZE + Version::BYTES_LEN + Time::BYTES_LEN + KEY_SIZE;

    pub fn new(
        volume_id: &Eid,
        key: &Key,
        crypto: &Crypto,
        payload: &[u8],
    ) -> Result<SuperBlk> {
        Ok(SuperBlk {
            seq: 0,
            volume_id: volume_id.clone(),
            key: key.clone(),
            crypto: crypto.clone(),
            ver: Version::current(),
            ctime: Time::now(),
            payload: payload.to_vec(),
        })
    }

    // check if super block exists
    pub fn exists(storage: &StorageRef) -> Result<bool> {
        let left_arm_id = Eid::from_slice(&LEFT_ARM_ID);
        let mut buf = Vec::new();
        let mut storage = storage.write().unwrap();
        match storage.get_all(&mut buf, &left_arm_id) {
            Ok(_) => Ok(true),
            Err(ref err) if *err == Error::NotFound => Ok(false),
            Err(err) => Err(err),
        }
    }

    // save super block
    pub fn save(&mut self, pwd: &str, storage: &StorageRef) -> Result<()> {
        // hash user specified plaintext password
        let pwd_hash = self.crypto.hash_pwd(pwd, &Salt::new())?;
        let vkey = &pwd_hash.value;

        // encrypt body using volume key which is the user password hash
        let mut body = Vec::with_capacity(SuperBlk::BODY_LEN);
        body.put_u64::<LittleEndian>(self.seq);
        body.put(self.volume_id.as_ref());
        body.put(&self.ver.serialize()[..]);
        body.put_u64::<LittleEndian>(self.ctime.as_secs());
        body.put(self.key.as_slice());
        let enc_body =
            self.crypto
                .encrypt_with_ad(&body, vkey, &[Self::BODY_LEN as u8])?;

        // encrypt payload using volume key
        let enc_payload = self.crypto.encrypt(&self.payload, vkey)?;

        // put it together and serialize super block
        let len = Self::HEADER_LEN + enc_body.len() + enc_payload.len();
        let mut buf = Vec::with_capacity(len);
        buf.put(pwd_hash.salt.as_ref());
        buf.put_u8(self.crypto.cost.to_u8());
        buf.put_u8(self.crypto.cipher.into());
        buf.put(&enc_body);
        buf.put(&enc_payload);

        // make arm id
        let arm_id = if self.seq % 2 == 0 {
            Eid::from_slice(&LEFT_ARM_ID)
        } else {
            Eid::from_slice(&RIGHT_ARM_ID)
        };

        // save to storage and increase sequence number
        let mut storage = storage.write().unwrap();
        storage.put_all(&arm_id, &buf)?;
        self.seq += 1;

        Ok(())
    }

    // load a specific super block arm
    fn load_arm(arm_id: &Eid, pwd: &str, storage: &StorageRef) -> Result<Self> {
        let mut buf = Vec::new();
        let mut storage = storage.write().unwrap();

        // get super block bytes from storage
        storage.get_all(&mut buf, arm_id)?;

        // read header
        if buf.len() < Self::HEADER_LEN {
            return Err(Error::InvalidSuperBlk);
        }
        let mut pos = 0;
        let salt = Salt::from_slice(&buf[..SALT_SIZE]);
        pos += SALT_SIZE;
        let cost = Cost::from_u8(buf[pos])?;
        pos += Cost::BYTES_LEN;
        let cipher = Cipher::from_u8(buf[pos])?;
        pos += Cipher::BYTES_LEN;

        // create crypto
        let crypto = Crypto::new(cost, cipher)?;

        // read encryped body
        let enc_body_len = crypto.encrypted_len(Self::BODY_LEN);
        if (buf.len() - pos) < enc_body_len {
            return Err(Error::InvalidSuperBlk);
        }
        let body_buf = &buf[pos..pos + enc_body_len];
        pos += enc_body_len;
        let payload_buf = &buf[pos..];

        // derive volume key and use it to decrypt body
        let pwd_hash = crypto.hash_pwd(pwd, &salt)?;
        let vkey = &pwd_hash.value;
        let body =
            crypto.decrypt_with_ad(body_buf, vkey, &[Self::BODY_LEN as u8])?;
        pos = 8;
        let seq = LittleEndian::read_u64(&body[..pos]);
        let volume_id = Eid::from_slice(&body[pos..pos + Eid::EID_SIZE]);
        pos += Eid::EID_SIZE;
        let ver = Version::deserialize(&body[pos..pos + Version::BYTES_LEN]);
        pos += Version::BYTES_LEN;
        let ctime =
            Time::from_secs(LittleEndian::read_u64(&body[pos..pos + 8]));
        pos += 8;
        let key = Key::from_slice(&body[pos..pos + KEY_SIZE]);

        // decrypt payload using volume key
        let payload = if payload_buf.is_empty() {
            Vec::new()
        } else {
            crypto.decrypt(payload_buf, vkey)?
        };

        Ok(SuperBlk {
            seq,
            volume_id,
            key,
            crypto,
            ver,
            ctime,
            payload,
        })
    }

    // load super block
    pub fn load(pwd: &str, storage: &StorageRef) -> Result<Self> {
        // make left and right arm id
        let left_arm_id = Eid::from_slice(&LEFT_ARM_ID);
        let right_arm_id = Eid::from_slice(&RIGHT_ARM_ID);

        let left_arm = Self::load_arm(&left_arm_id, pwd, storage);
        let right_arm = Self::load_arm(&right_arm_id, pwd, storage);

        match left_arm {
            Ok(left) => match right_arm {
                Ok(right) => {
                    if left.seq > right.seq {
                        Ok(left)
                    } else if left.seq < right.seq {
                        Ok(right)
                    } else {
                        unreachable!();
                    }
                }
                Err(ref err) if *err == Error::NotFound => Ok(left),
                Err(ref err)
                    if *err == Error::Decrypt
                        || *err == Error::InvalidSuperBlk =>
                {
                    warn!("super block right arm corrupted");
                    Ok(left)
                }
                Err(err) => return Err(err),
            },
            Err(ref err) if *err == Error::NotFound => right_arm,
            Err(ref err)
                if *err == Error::Decrypt || *err == Error::InvalidSuperBlk =>
            {
                warn!("super block left arm corrupted");
                right_arm
            }
            Err(err) => return Err(err),
        }
    }
}
