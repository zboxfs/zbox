use bytes::BufMut;
use serde::{Deserialize, Serialize};
use rmp_serde::{Deserializer, Serializer};

use base::crypto::{Crypto, CryptoCtx, HashKey};
use error::{Error, Result};
use trans::{Eid, Id};
use super::storage::StorageRef;

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub enum Arm {
    Left,
    Right,
}

impl Arm {
    fn seri(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.serialize(&mut Serializer::new(&mut buf)).unwrap();
        buf
    }

    fn to_eid(&self, id: &Eid, hash_key: &HashKey) -> Eid {
        let mut buf = Vec::new();
        buf.put(id.as_ref());
        buf.put(&self.seri());
        let hash = Crypto::hash_with_key(&buf, hash_key);
        Eid::from_slice(&hash)
    }

    #[inline]
    fn other(&self) -> Self {
        match *self {
            Arm::Left => Arm::Right,
            Arm::Right => Arm::Left,
        }
    }
}

impl Default for Arm {
    #[inline]
    fn default() -> Self {
        Arm::Right
    }
}

// Seq trait
pub trait Seq {
    fn seq(&self) -> u64;
}

// load a single arm
fn load_one_arm<'de, T: Id + Deserialize<'de> + Serialize>(
    id: &Eid,
    arm: Arm,
    storage: &StorageRef,
    crypto_ctx: &CryptoCtx,
) -> Result<T> {
    let mut buf = Vec::new();
    let arm_id = arm.to_eid(id, &crypto_ctx.hash_key);
    let mut storage = storage.write().unwrap();
    storage.get_all(&mut buf, &arm_id)?;
    crypto_ctx
        .crypto
        .decrypt(&buf, &crypto_ctx.key)
        .and_then(|buf| {
            let mut de = Deserializer::new(&buf[..]);
            let ret: T = Deserialize::deserialize(&mut de)?;
            Ok(ret)
        })
}

// load both left and right arms
fn load_arms<'de, T: Id + Seq + Deserialize<'de> + Serialize>(
    id: &Eid,
    storage: &StorageRef,
    crypto_ctx: &CryptoCtx,
) -> Result<(Option<T>, Option<T>)> {
    let left_arm = load_one_arm::<T>(id, Arm::Left, storage, crypto_ctx);
    let right_arm = load_one_arm::<T>(id, Arm::Right, storage, crypto_ctx);

    match left_arm {
        Ok(left) => match right_arm {
            Ok(right) => {
                assert!(left.seq() != right.seq());
                Ok((Some(left), Some(right)))
            }
            Err(ref err) if *err == Error::NotFound => Ok((Some(left), None)),
            Err(ref err) if *err == Error::Decrypt => {
                warn!("{:?} right arm corrupted", id);
                Ok((Some(left), None))
            }
            Err(err) => return Err(err),
        },
        Err(ref err) if *err == Error::NotFound => match right_arm {
            Ok(right) => Ok((None, Some(right))),
            Err(ref err) if *err == Error::NotFound => Ok((None, None)),
            Err(err) => return Err(err),
        },
        Err(ref err) if *err == Error::Decrypt => {
            warn!("{:?} left arm corrupted", id);
            match right_arm {
                Ok(right) => Ok((None, Some(right))),
                Err(err) => return Err(err),
            }
        }
        Err(err) => return Err(err),
    }
}

// remove both arms without order
fn remove_arms_no_order(
    id: &Eid,
    storage: &StorageRef,
    crypto_ctx: &CryptoCtx,
) -> Result<()> {
    let left_arm_id = Arm::Left.to_eid(id, &crypto_ctx.hash_key);
    let right_arm_id = Arm::Right.to_eid(id, &crypto_ctx.hash_key);
    let mut storage = storage.write().unwrap();
    storage.del(&left_arm_id).and(storage.del(&right_arm_id))
}

// Armor trait
pub trait Armor<'de>: Id + Seq + Deserialize<'de> + Serialize {
    fn arm(&self) -> Arm;
    fn arm_mut(&mut self) -> &mut Arm;

    fn load(
        id: &Eid,
        storage: &StorageRef,
        crypto_ctx: &CryptoCtx,
    ) -> Result<Self> {
        let (left_arm, right_arm) = load_arms::<Self>(id, storage, crypto_ctx)?;

        match left_arm {
            Some(left) => match right_arm {
                Some(right) => {
                    if left.seq() > right.seq() {
                        Ok(left)
                    } else {
                        Ok(right)
                    }
                }
                None => Ok(left),
            },
            None => right_arm.ok_or(Error::NotFound),
        }
    }

    fn save(
        &mut self,
        storage: &StorageRef,
        crypto_ctx: &CryptoCtx,
    ) -> Result<()> {
        // make entity id based on the other arm
        let other_arm = self.arm().other().clone();
        let id = other_arm.to_eid(self.id(), &crypto_ctx.hash_key);

        // serialize self
        let mut storage = storage.write().unwrap();
        let mut buf = Vec::new();
        self.serialize(&mut Serializer::new(&mut buf))?;

        // encrypt and then save to storage, switch arm once it completed
        crypto_ctx
            .crypto
            .encrypt(&buf, &crypto_ctx.key)
            .and_then(|buf| storage.put_all(&id, &buf))
            .and_then(|_| {
                *self.arm_mut() = other_arm;
                Ok(())
            })
    }

    // delete current arm
    fn del_arm(&self, storage: &StorageRef, hash_key: &HashKey) -> Result<()> {
        let arm_id = self.arm().to_eid(self.id(), hash_key);
        let mut storage = storage.write().unwrap();
        storage.del(&arm_id)
    }

    // Remove both arms without order, this deletion cannot be recovered. This
    // is differect with Armor::remove().
    #[inline]
    fn remove_no_order(
        id: &Eid,
        storage: &StorageRef,
        crypto_ctx: &CryptoCtx,
    ) -> Result<()> {
        remove_arms_no_order(id, storage, crypto_ctx)
    }

    // Remove both arm in sequence order, this operation can be falled back to
    // undeleted one in case of failure.
    fn remove(
        id: &Eid,
        storage: &StorageRef,
        crypto_ctx: &CryptoCtx,
    ) -> Result<()> {
        let first;
        let mut second = None;
        let (left_arm, right_arm) = load_arms::<Self>(id, storage, crypto_ctx)?;

        // determin deletion order, delete small sequence number arm first
        match left_arm {
            Some(left) => match right_arm {
                Some(right) => {
                    if left.seq() > right.seq() {
                        first = Some(right);
                        second = Some(left);
                    } else {
                        first = Some(left);
                        second = Some(right);
                    }
                }
                None => first = Some(left),
            },
            None => first = right_arm,
        }

        // delete arms in order
        let mut storage = storage.write().unwrap();
        if let Some(arm) = first {
            let arm_id = arm.arm().to_eid(id, &crypto_ctx.hash_key);
            storage.del(&arm_id)?;
        }
        if let Some(arm) = second {
            let arm_id = arm.arm().to_eid(id, &crypto_ctx.hash_key);
            storage.del(&arm_id)?;
        }
        Ok(())
    }
}
