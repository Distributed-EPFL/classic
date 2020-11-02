use criterion::{BatchSize, Criterion};

use classic::broadcast::{MurmurMessage, Sieve, SieveMessage};
use classic::Processor;

use drop::crypto::sign::{KeyPair, Signer};

use super::*;

pub fn criterion_benchmark(c: &mut Criterion) {
    let group = c.benchmark_group("sieve");

    bench_all_size!(group, SIZE, |b, &size| {
        b.iter_batched_ref(
            || {
                let keypair = KeyPair::random();
                let message = (0..size as u8).collect::<Vec<_>>();
                let signature = Signer::new(keypair.clone())
                    .sign(&message)
                    .expect("sign failed");
                let runtime = build_runtime();
                let (keys, messages): (Vec<_>, Vec<_>) =
                    generate_message_sequence(SIZE, || {
                        SieveMessage::Echo(message.clone(), signature)
                    })
                    .unzip();

                let inner = (message, signature);
                let outer_signature = Signer::new(keypair.clone())
                    .sign(&inner)
                    .expect("sign failed");
                let murmur_message =
                    MurmurMessage::Gossip(outer_signature, inner);

                let (_, murmur): (Vec<_>, Vec<_>) = keys
                    .iter()
                    .copied()
                    .zip(iter::repeat(Arc::new(SieveMessage::Probabilistic(
                        murmur_message,
                    ))))
                    .unzip();

                let messages = murmur.into_iter().chain(messages);

                let processor = Arc::new(Sieve::new_receiver(
                    keypair.public().clone(),
                    Arc::new(KeyPair::random()),
                    SIZE / 5,
                    SIZE / 5,
                ));
                let sender = Arc::new(NoopSender::new(keys.clone()));

                (messages, keys, processor, sender, runtime)
            },
            |(messages, keys, processor, sender, runtime)| {
                runtime.block_on(async {
                    for (key, message) in keys.iter().cycle().zip(messages) {
                        processor
                            .clone()
                            .process(message.clone(), *key, sender.clone())
                            .await;
                    }
                })
            },
            BatchSize::SmallInput,
        )
    });
}
