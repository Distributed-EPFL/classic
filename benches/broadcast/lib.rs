use std::future::Future;
use std::iter;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

use classic::{Message, Sender, SenderError};

use cpuprofiler::PROFILER;

use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion,
    Throughput,
};

use drop::async_trait;
use drop::crypto::key::exchange::{KeyPair, PublicKey};
use drop::net::ConnectionWrite;

use tokio::runtime::{Builder, Runtime};

/// Number of bytes in a megabyte
pub static MB: usize = 1024 ^ 2;

pub static SIZE: usize = 100;

/// Dummy sender that doesn't do anything
pub struct NoopSender<M: Message + 'static> {
    keys: Vec<PublicKey>,
    _m: PhantomData<M>,
}

impl<M: Message + 'static> NoopSender<M> {
    pub fn new<I: IntoIterator<Item = PublicKey>>(keys: I) -> Self {
        Self {
            keys: keys.into_iter().collect(),
            _m: PhantomData,
        }
    }
}

#[async_trait]
impl<M: Message + 'static> Sender<M> for NoopSender<M> {
    async fn add_connection(&self, _: ConnectionWrite) {}

    async fn keys(&self) -> Vec<PublicKey> {
        self.keys.clone()
    }

    async fn send(
        &self,
        _: Arc<M>,
        key: &PublicKey,
    ) -> Result<(), SenderError> {
        if self.keys.contains(key) {
            black_box(Ok(()))
        } else {
            Err(SenderError::NoSuchPeer)
        }
    }
}

pub fn key_sequence(count: usize) -> impl Iterator<Item = PublicKey> {
    (0..count).map(|_| *KeyPair::random().public())
}

pub fn generate_message_sequence<F: Fn() -> M, M: Message>(
    count: usize,
    message: F,
) -> impl Iterator<Item = (PublicKey, Arc<M>)> {
    key_sequence(count).zip(iter::repeat(Arc::new((message)())))
}

pub fn run_future<F: Future<Output = O>, O>(future: F) -> O {
    let mut runtime = build_runtime();

    runtime.block_on(future)
}

#[macro_export]
macro_rules! bench_all_size {
    ($group:expr, $count:expr, $setup: expr) => {
        let mut group = $group;

        for size in [MB, 2 * MB, 4 * MB, 8 * MB, 16 * MB].iter() {
            group.throughput(Throughput::Bytes(*size as u64 * ($count) as u64));

            group.bench_with_input(
                BenchmarkId::from_parameter(format!("{}MB", size / MB)),
                size,
                $setup,
            );
        }
        group.finish();
    };
}

fn profiled() -> Criterion {
    Criterion::default().with_profiler(Profiler {})
}

pub fn build_runtime() -> Runtime {
    Builder::new()
        .threaded_scheduler()
        .core_threads(4)
        .enable_all()
        .build()
        .expect("runtime creation failed")
}

struct Profiler {}

impl criterion::profiler::Profiler for Profiler {
    fn start_profiling(&mut self, bench_id: &str, _: &Path) {
        let name = format!("{}.profile", bench_id).replace("/", "");
        PROFILER
            .lock()
            .expect("access to profiler failed")
            .start(name)
            .expect("profiling start failed");
    }
    fn stop_profiling(&mut self, _: &str, _: &Path) {
        PROFILER
            .lock()
            .expect("access to profiler failed")
            .stop()
            .expect("profiling stop failed");
    }
}

mod murmur;
mod sieve;

criterion_group! {
    name = benches;
    config = profiled();
    targets = sieve::criterion_benchmark, murmur::criterion_benchmark,
}
criterion_main!(benches);
