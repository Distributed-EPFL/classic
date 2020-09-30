use std::convert::TryInto;
use std::marker::PhantomData;
use std::num::TryFromIntError;
use std::sync::Arc;

use crate::{Message, Sender};

use drop::crypto::key::exchange::PublicKey;

use peroxide::fuga::*;

use snafu::{ResultExt, Snafu};

#[derive(Snafu, Debug)]
pub enum SampleError {
    #[snafu(display("expected size too big"))]
    TooBig { source: TryFromIntError },
}

/// Struct used to sample a `Sender` to produce a set of `PublicKey`s
pub struct Sampler<S, M> {
    sender: Arc<S>,
    _m: PhantomData<M>,
}

impl<M: Message + 'static, S: Sender<M>> Sampler<S, M> {
    /// Create a new `Sampler` that will provide an easy way to produce a
    /// sample from the given `Sender`
    pub fn new(sender: Arc<S>) -> Self {
        Self {
            sender,
            _m: PhantomData,
        }
    }

    /// Sample the set of known peers to produce a set of keys of size
    /// `expected`.
    pub async fn sample(
        &self,
        expected: usize,
    ) -> Result<impl Iterator<Item = PublicKey>, SampleError> {
        let expected: u32 = expected.try_into().context(TooBig)?;
        let keys = self.sender.keys().await;
        let size: u32 = keys.len().try_into().context(TooBig)?;

        let prob = expected as f64 / size as f64;
        let sampler = Bernoulli(prob);
        let mut sample = sampler.sample(size as usize);

        Ok(keys.into_iter().filter(move |_| {
            if let Some(x) = sample.pop() {
                (x - 1.0).abs() < f64::EPSILON
            } else {
                false
            }
        }))
    }
}
