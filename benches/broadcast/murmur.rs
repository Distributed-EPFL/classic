use std::sync::Arc;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};

use classic::broadcast::{Murmur, MurmurMessage};
use classic::{PoissonSampler, Processor};

use drop::crypto::sign::{KeyPair, Signer};

use super::*;

/// Criterion benchmark of message processing time by the murmur algorithm
pub fn criterion_benchmark(c: &mut Criterion) {
    let group = c.benchmark_group("murmur");

    bench_all_size!(group, SIZE, |b, &size| {
        b.iter_batched_ref(
            || {
                let keypair = KeyPair::random();
                let content = (0..size as u8).collect::<Vec<_>>();
                let signature = Signer::new(keypair.clone())
                    .sign(&content)
                    .expect("sign failed");

                let (keys, messages): (Vec<_>, Vec<_>) =
                    generate_message_sequence(SIZE, || {
                        MurmurMessage::Gossip(signature, content.clone())
                    })
                    .unzip();

                let sender = Arc::new(NoopSender::new(keys.clone()));
                let sampler = Arc::new(PoissonSampler::default());

                let mut processor = Arc::new(Murmur::new_receiver(
                    keypair.public().clone(),
                    Arc::new(keypair.clone()),
                    keys.len() / 5,
                ));

                let mut runtime = build_runtime();
                let _ = runtime.block_on({
                    Arc::get_mut(&mut processor)
                        .expect("bug")
                        .output(sampler, sender.clone())
                });

                (runtime, messages, keys, processor, sender)
            },
            |(runtime, messages, keys, processor, sender)| {
                for (key, message) in keys.iter().zip(messages.iter()) {
                    runtime.block_on(processor.clone().process(
                        message.clone(),
                        *key,
                        sender.clone(),
                    ));
                }
            },
            BatchSize::SmallInput,
        );
    });
}
