use std::fmt::Debug;
use std::io::{ErrorKind, Read, Write};
use std::marker::PhantomData;

use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};

use super::volume::{self, VolumeRef};
use base::crypto::Crypto;
use error::{Error, Result};
use trans::{Eid, Finish, Id};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize, Serialize)]
pub enum Arm {
    Left = 0,
    Right = 1,
}

impl Arm {
    fn to_eid(self, id: &Eid) -> Eid {
        // serialize arm
        let mut arm_buf = Vec::new();
        self.serialize(&mut Serializer::new(&mut arm_buf)).unwrap();

        // hash eid and arm to make an eid
        let mut buf = vec![0u8; Eid::EID_SIZE + arm_buf.len()];
        buf[..Eid::EID_SIZE].copy_from_slice(id.as_ref());
        buf[Eid::EID_SIZE..].copy_from_slice(&arm_buf);
        let hash = Crypto::hash(&buf);
        Eid::from_slice(&hash)
    }

    #[inline]
    fn both_eid(id: &Eid) -> (Eid, Eid) {
        (Arm::Left.to_eid(id), Arm::Right.to_eid(id))
    }

    #[inline]
    pub fn other(self) -> Self {
        match self {
            Arm::Left => Arm::Right,
            Arm::Right => Arm::Left,
        }
    }

    #[inline]
    pub fn toggle(&mut self) {
        *self = self.other();
    }

    pub fn remove_arm(self, id: &Eid, vol: &VolumeRef) -> Result<()> {
        let mut vol = vol.write().unwrap();
        let arm_id = self.to_eid(id);
        vol.del(&arm_id)
    }

    pub fn remove_all(id: &Eid, vol: &VolumeRef) -> Result<()> {
        let mut vol = vol.write().unwrap();
        let (left_arm_id, right_arm_id) = Arm::both_eid(id);
        vol.del(&left_arm_id).and(vol.del(&right_arm_id))
    }
}

impl Default for Arm {
    #[inline]
    fn default() -> Self {
        Arm::Left
    }
}

/// Seq trait
pub trait Seq {
    fn seq(&self) -> u64;
    fn inc_seq(&mut self);
}

/// Arm access trait
pub trait ArmAccess<'de>: Id + Seq + Deserialize<'de> + Serialize {
    fn arm(&self) -> Arm;
    fn arm_mut(&mut self) -> &mut Arm;
}

/// Armor trait
pub trait Armor<'de> {
    type Item: ArmAccess<'de> + Debug;
    type ItemReader: Read;
    type ItemWriter: Write + Finish;

    fn get_item_reader(&self, arm_id: &Eid) -> Result<Self::ItemReader>;
    fn get_item_writer(&self, arm_id: &Eid) -> Result<Self::ItemWriter>;
    fn del_arm(&self, arm_id: &Eid) -> Result<()>;

    fn load_one_arm(&self, id: &Eid, arm: Arm) -> Result<Self::Item> {
        let arm_id = arm.to_eid(id);
        let mut rdr = self.get_item_reader(&arm_id)?;
        let mut buf = Vec::new();
        from_io_err!(rdr.read_to_end(&mut buf))?;
        let mut de = Deserializer::new(&buf[..]);
        let item: Self::Item = Deserialize::deserialize(&mut de)?;
        Ok(item)
    }

    // try to load both left and right arms
    #[allow(clippy::type_complexity)]
    fn load_arms(
        &self,
        id: &Eid,
    ) -> Result<(Option<Self::Item>, Option<Self::Item>)> {
        // load left and right arms
        let left_arm = self.load_one_arm(id, Arm::Left);
        let right_arm = self.load_one_arm(id, Arm::Right);

        match left_arm {
            Ok(left) => match right_arm {
                Ok(right) => {
                    assert!(left.seq() != right.seq());
                    Ok((Some(left), Some(right)))
                }
                Err(ref err) if *err == Error::NotFound => {
                    Ok((Some(left), None))
                }
                Err(err) => Err(err),
            },
            Err(ref err) if *err == Error::NotFound => match right_arm {
                Ok(right) => Ok((None, Some(right))),
                Err(ref err) if *err == Error::NotFound => Ok((None, None)),
                Err(err) => Err(err),
            },
            Err(err) => Err(err),
        }
    }

    fn load_item(&self, id: &Eid) -> Result<Self::Item> {
        let (left_arm, right_arm) = self.load_arms(id)?;

        let item = match left_arm {
            Some(left) => match right_arm {
                Some(right) => {
                    if left.seq() > right.seq() {
                        left
                    } else {
                        right
                    }
                }
                None => left,
            },
            None => right_arm.ok_or(Error::NotFound)?,
        };

        Ok(item)
    }

    // save item
    fn save_item(&self, item: &mut Self::Item) -> Result<()> {
        // increase sequence and toggle arm
        item.inc_seq();
        item.arm_mut().toggle();

        let arm_id = item.arm().to_eid(item.id());

        // save to writer
        (|| {
            let mut buf = Vec::new();
            item.serialize(&mut Serializer::new(&mut buf))?;
            let mut wtr = self.get_item_writer(&arm_id)?;
            wtr.write_all(&buf[..])?;
            wtr.finish()?;
            Ok(())
        })()
        .or_else(|err| {
            // if save item failed, revert its arm back
            item.arm_mut().toggle();
            Err(err)
        })
    }

    // remove the other arm
    fn remove_other_arm(&self, item: &Self::Item) -> Result<()> {
        let arm_id = item.arm().other().to_eid(item.id());
        self.del_arm(&arm_id)
    }

    // Remove all arms without order, this deletion cannot be recovered.
    fn remove_all_arms(&self, id: &Eid) -> Result<()> {
        let (left_arm_id, right_arm_id) = Arm::both_eid(id);
        self.del_arm(&left_arm_id).and(self.del_arm(&right_arm_id))
    }
}

/// Volume Wal Armor
#[derive(Default, Clone)]
pub struct VolumeWalArmor<T> {
    vol: VolumeRef,
    _t: PhantomData<T>,
}

impl<T> VolumeWalArmor<T> {
    pub fn new(vol: &VolumeRef) -> Self {
        VolumeWalArmor {
            vol: vol.clone(),
            _t: PhantomData,
        }
    }
}

impl<'de, T> Armor<'de> for VolumeWalArmor<T>
where
    T: ArmAccess<'de> + Debug,
{
    type Item = T;
    type ItemReader = volume::WalReader;
    type ItemWriter = volume::WalWriter;

    #[inline]
    fn get_item_reader(&self, arm_id: &Eid) -> Result<Self::ItemReader> {
        Ok(volume::WalReader::new(arm_id, &self.vol))
    }

    #[inline]
    fn get_item_writer(&self, arm_id: &Eid) -> Result<Self::ItemWriter> {
        Ok(volume::WalWriter::new(arm_id, &self.vol))
    }

    #[inline]
    fn del_arm(&self, arm_id: &Eid) -> Result<()> {
        let mut vol = self.vol.write().unwrap();
        vol.del_wal(arm_id)
    }
}

/// Volume Armor
#[derive(Default, Clone)]
pub struct VolumeArmor<T> {
    vol: VolumeRef,
    _t: PhantomData<T>,
}

impl<T> VolumeArmor<T> {
    pub fn new(vol: &VolumeRef) -> Self {
        VolumeArmor {
            vol: vol.clone(),
            _t: PhantomData,
        }
    }
}

impl<'de, T> Armor<'de> for VolumeArmor<T>
where
    T: ArmAccess<'de> + Debug,
{
    type Item = T;
    type ItemReader = volume::Reader;
    type ItemWriter = volume::Writer;

    #[inline]
    fn get_item_reader(&self, arm_id: &Eid) -> Result<Self::ItemReader> {
        Ok(volume::Reader::new(arm_id, &self.vol)?)
    }

    #[inline]
    fn get_item_writer(&self, arm_id: &Eid) -> Result<Self::ItemWriter> {
        Ok(volume::Writer::new(arm_id, &self.vol)?)
    }

    #[inline]
    fn del_arm(&self, arm_id: &Eid) -> Result<()> {
        let mut vol = self.vol.write().unwrap();
        vol.del(arm_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base::{init_env, IntoRef};
    use fs::Config;
    use volume::Volume;

    #[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
    struct Item {
        seq: u64,
        arm: Arm,
        id: Eid,
    }

    impl Item {
        fn new() -> Self {
            Item {
                seq: 0,
                arm: Arm::default(),
                id: Eid::new(),
            }
        }
    }

    impl Id for Item {
        #[inline]
        fn id(&self) -> &Eid {
            &self.id
        }

        #[inline]
        fn id_mut(&mut self) -> &mut Eid {
            &mut self.id
        }
    }

    impl Seq for Item {
        #[inline]
        fn seq(&self) -> u64 {
            self.seq
        }

        #[inline]
        fn inc_seq(&mut self) {
            self.seq += 1
        }
    }

    impl<'de> ArmAccess<'de> for Item {
        #[inline]
        fn arm(&self) -> Arm {
            self.arm
        }

        #[inline]
        fn arm_mut(&mut self) -> &mut Arm {
            &mut self.arm
        }
    }

    #[test]
    fn volume_armor() {
        init_env();
        let mut vol = Volume::new("mem://foo").unwrap();
        vol.init("pwd", &Config::default(), &Vec::new()).unwrap();
        let varm = VolumeArmor::<Item>::new(&vol.into_ref());

        let mut item = Item::new();
        let mut item2 = Item::new();

        // test item save and load
        for _i in 0..5 {
            let item_bk = item.clone();
            varm.save_item(&mut item).unwrap();
            assert_eq!(item.seq, item_bk.seq + 1);
            assert_eq!(item.arm, item_bk.arm.other());
            let item_loaded = varm.load_item(item.id()).unwrap();
            assert_eq!(item_loaded.seq, item_bk.seq + 1);
            assert_eq!(item_loaded.arm, item_bk.arm.other());

            varm.save_item(&mut item2).unwrap();
        }

        // test item remove
        varm.remove_all_arms(item.id()).unwrap();
        varm.remove_all_arms(item.id()).unwrap();
        varm.remove_all_arms(item2.id()).unwrap();
        assert_eq!(varm.load_item(item.id()).unwrap_err(), Error::NotFound);
        assert_eq!(varm.load_item(item2.id()).unwrap_err(), Error::NotFound);
    }
}
