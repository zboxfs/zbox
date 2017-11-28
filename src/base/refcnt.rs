use error::{Error, Result};

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct RefCnt(u32);

impl RefCnt {
    pub fn new() -> Self {
        RefCnt::default()
    }

    #[inline]
    pub fn val(&self) -> u32 {
        self.0
    }

    pub fn inc_ref(&mut self) -> Result<u32> {
        self.0.checked_add(1).ok_or(Error::RefOverflow).and_then(
            |r| {
                self.0 = r;
                Ok(r)
            },
        )
    }

    pub fn dec_ref(&mut self) -> Result<u32> {
        self.0.checked_sub(1).ok_or(Error::RefUnderflow).and_then(
            |r| {
                self.0 = r;
                Ok(r)
            },
        )
    }
}
