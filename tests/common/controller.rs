// random error controller
#[cfg(feature = "storage-faulty")]
pub mod imp {
    use std::fmt::{self, Debug};

    use super::super::crypto;
    use zbox::FaultyController;

    pub struct Controller {
        ctl: FaultyController,
        prob: f32,
    }

    impl Controller {
        pub fn new() -> Self {
            Controller {
                ctl: FaultyController::new(),
                prob: 0.05, // set the error probability
            }
        }

        #[inline]
        pub fn reset(&self, seed: &crypto::RandomSeed) {
            self.ctl.reset(&seed.0, self.prob);
        }

        #[inline]
        pub fn turn_on(&self) {
            self.ctl.turn_on();
            //self.ctl.turn_off();
        }

        #[inline]
        pub fn turn_off(&self) {
            self.ctl.turn_off();
        }
    }

    impl Debug for Controller {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.debug_struct("Controller").finish()
        }
    }
}

#[cfg(not(feature = "storage-faulty"))]
#[allow(dead_code)]
pub mod imp {
    use super::super::crypto;

    #[derive(Debug)]
    pub struct Controller {}

    impl Controller {
        pub fn new() -> Self {
            Controller {}
        }

        pub fn reset(&self, _seed: &crypto::RandomSeed) {}

        pub fn turn_on(&self) {}

        pub fn turn_off(&self) {}
    }
}
