use std::io::{Error as IoError, ErrorKind, Result as IoResult};
use std::sync::{Arc, RwLock};

use base::crypto::{Crypto, RandomSeed};

lazy_static! {
    // static variable to store random samples
    static ref ERR_CONTEXT: Arc<RwLock<ErrorContext>> =
        { Arc::new(RwLock::new(ErrorContext::default())) };
}

// random error generator context
#[derive(Default)]
struct ErrorContext {
    is_on: bool,
    prob: f32, // error occur probability
    threshold: u8,
    samples: Vec<u8>,
    sample_seq: usize,
}

// controller for random error generation
#[derive(Default)]
pub struct Controller {}

impl Controller {
    const ERR_SAMPLE_SIZE: usize = 256;

    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn turn_on(&self) {
        let mut context = ERR_CONTEXT.write().unwrap();
        context.is_on = true;
    }

    pub fn turn_off(&self) {
        let mut context = ERR_CONTEXT.write().unwrap();
        context.is_on = false;
    }

    pub fn reset(&self, seed: &[u8], prob: f32) {
        let seed = RandomSeed::from(seed);
        let mut context = ERR_CONTEXT.write().unwrap();
        context.samples.resize(Self::ERR_SAMPLE_SIZE, 0);
        Crypto::random_buf_deterministic(&mut context.samples[..], &seed);
        context.is_on = false;
        context.prob = prob;
        context.threshold = ((Self::ERR_SAMPLE_SIZE - 1) as f32 * prob) as u8;
        context.sample_seq = 0;
    }

    // make a IO error based on the random sample
    pub fn make_random_error(&self) -> IoResult<()> {
        let mut context = ERR_CONTEXT.write().unwrap();
        if !context.is_on {
            return Ok(());
        }

        assert!(!context.samples.is_empty());
        let idx = context.sample_seq % context.samples.len();
        context.sample_seq += 1;

        let sample = context.samples[idx];
        match sample {
            _ if sample <= context.threshold => {
                Err(IoError::new(ErrorKind::Other, "Faulty error"))
            }
            _ => Ok(()),
        }
    }
}
