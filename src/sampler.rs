use std::collections::HashSet;

use drop::async_trait;
use drop::crypto::key::exchange::PublicKey;

use peroxide::fuga::*;

use snafu::{OptionExt, Snafu};

#[derive(Snafu, Debug)]
/// Error returned when sampling fails
pub enum SampleError {
    #[snafu(display("unable to compute size"))]
    /// Unable to compute size from supplied iterator
    BadIterator,
}

/// Trait used when sampling a set of known peers
#[async_trait]
pub trait Sampler: Send + Sync {
    /// Take a sample of keys from the provided `Sender`
    async fn sample<I: IntoIterator<Item = PublicKey> + Send>(
        &self,
        keys: I,
        expected_size: usize,
    ) -> Result<HashSet<PublicKey>, SampleError>;
}

/// A naive sampler using Poisson sampling
#[derive(Clone, Copy)]
pub struct PoissonSampler {}

impl Default for PoissonSampler {
    fn default() -> Self {
        Self {}
    }
}

#[async_trait]
impl Sampler for PoissonSampler {
    async fn sample<I: IntoIterator<Item = PublicKey> + Send>(
        &self,
        keys: I,
        expected: usize,
    ) -> Result<HashSet<PublicKey>, SampleError> {
        let iter = keys.into_iter();
        let size: usize = iter.size_hint().1.context(BadIterator)?;

        let prob = expected as f64 / size as f64;
        let sampler = Bernoulli(prob);
        let mut sample = sampler.sample(size as usize);

        Ok(iter
            .filter(move |_| {
                if let Some(x) = sample.pop() {
                    (x - 1.0).abs() < f64::EPSILON
                } else {
                    false
                }
            })
            .collect())
    }
}

#[derive(Clone, Copy)]
/// Sampler that selects all known keys instead of sampling randomly
pub struct AllSampler {}

impl Default for AllSampler {
    fn default() -> Self {
        Self {}
    }
}

#[async_trait]
impl Sampler for AllSampler {
    async fn sample<I: IntoIterator<Item = PublicKey> + Send>(
        &self,
        keys: I,
        _: usize,
    ) -> Result<HashSet<PublicKey>, SampleError> {
        Ok(keys.into_iter().collect())
    }
}
