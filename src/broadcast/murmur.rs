use std::collections::HashSet;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::Arc;

use crate::{implement_handle, Handle, Message, Processor, Sampler, Sender};
use classic_derive::message;

use drop::async_trait;
use drop::crypto::key::exchange::PublicKey;
use drop::crypto::sign::{self, KeyPair, Signature, Signer};

use serde::{Deserialize, Serialize};

use snafu::{OptionExt, ResultExt, Snafu};

use tokio::sync::{oneshot, Mutex, RwLock};

use tracing::{debug, debug_span, error};
use tracing_futures::Instrument;

#[message]
/// Message exchanged by the murmur algorithm
pub enum MurmurMessage<M: Message> {
    #[serde(bound(deserialize = "M: Message"))]
    /// Message used during gossiping by the murmur algorithm
    Gossip(Signature, M),
    /// Subscription request for the murmur algorithm
    Subscribe,
}

implement_handle!(
    MurmurHandle,
    MurmurError,
    MurmurMessage,
    |message, signature| { MurmurMessage::Gossip(signature, message) }
);

/// A probabilistic broadcast using Erdös-Rényi Gossip
pub struct Murmur<M: Message + 'static> {
    keypair: Arc<KeyPair>,
    sender: sign::PublicKey,
    delivered_tx: Mutex<Option<oneshot::Sender<M>>>,
    delivery: Option<oneshot::Receiver<M>>,
    delivered: Mutex<Option<M>>,
    gossip: Arc<RwLock<HashSet<PublicKey>>>,
    expected: usize,
}

impl<M: Message + 'static> Murmur<M> {
    /// Create a new probabilistic broadcast sender with your local `KeyPair`
    pub fn new_sender(keypair: Arc<KeyPair>, expected: usize) -> Self {
        let (delivered_tx, delivered_rx) = oneshot::channel();
        let sender = keypair.public().clone();

        Self {
            keypair,
            sender,
            delivered_tx: Mutex::new(Some(delivered_tx)),
            delivery: Some(delivered_rx),
            delivered: Mutex::new(None),
            gossip: Arc::new(RwLock::new(HashSet::with_capacity(expected))),
            expected,
        }
    }

    /// Create a new probabilistic broadcast receiver with designated sender and
    /// the local `KeyPair`
    pub fn new_receiver(
        sender: sign::PublicKey,
        keypair: Arc<KeyPair>,
        expected: usize,
    ) -> Self {
        let (delivered_tx, delivered_rx) = oneshot::channel();

        Self {
            expected,
            sender,
            keypair,
            delivered_tx: Mutex::new(Some(delivered_tx)),
            delivery: Some(delivered_rx),
            delivered: Mutex::new(None),
            gossip: Arc::new(RwLock::new(HashSet::with_capacity(expected))),
        }
    }
}

#[async_trait]
impl<M: Message + 'static, S: Sender<MurmurMessage<M>> + 'static>
    Processor<MurmurMessage<M>, M, S> for Murmur<M>
{
    type Handle = MurmurHandle<M>;

    async fn process(
        self: Arc<Self>,
        message: Arc<MurmurMessage<M>>,
        from: PublicKey,
        sender: Arc<S>,
    ) {
        match message.deref() {
            MurmurMessage::Subscribe => {
                debug!("new peer {} subscribed to us", from);
                self.gossip.write().await.insert(from);
            }
            MurmurMessage::Gossip(signature, content) => {
                let mut signer = Signer::new(self.keypair.deref().clone());
                let mut delivered = self.delivered.lock().await;

                if delivered.is_some() {
                    debug!("already delivered a message");
                    return;
                }

                if signer
                    .verify(&signature, &self.sender, content.deref())
                    .is_ok()
                {
                    debug!("good signature for {:?}", message);

                    delivered.replace(content.clone());

                    std::mem::drop(delivered);

                    if let Some(chan) = self.delivered_tx.lock().await.take() {
                        if chan.send(content.clone()).is_err() {
                            error!("handle removed before delivery");
                        }
                    } else {
                        debug!("late murmur message");
                    }

                    let dest = self.gossip.read().await;

                    sender.send_many(message, dest.iter()).await;
                }
            }
        }
    }

    async fn output<SA: Sampler>(
        &mut self,
        sampler: Arc<SA>,
        msg_sender: Arc<S>,
    ) -> Self::Handle {
        let sample = sampler
            .sample(msg_sender.keys().await, self.expected)
            .await
            .expect("sampling failed");
        self.gossip.write().await.extend(sample);

        let gossip = self.gossip.clone();

        let sender = if self.keypair.public() == &self.sender {
            let (sender, receiver) = oneshot::channel();

            tokio::task::spawn(
                async move {
                    let sender = msg_sender;
                    let gossip = gossip;

                    if let Ok(out) = receiver.await {
                        let out = Arc::new(out);

                        sender.send_many(out, gossip.read().await.iter()).await;
                    }
                }
                .instrument(debug_span!("murmur_gossip")),
            );

            Some(sender)
        } else {
            None
        };

        let delivery = self
            .delivery
            .take()
            .expect("double setup of processor detected");

        MurmurHandle::new(self.keypair.clone(), delivery, sender)
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::AllSampler;

    use std::time::Duration;

    use crate::test::*;
    use crate::{CollectingSender, PoissonSampler, SystemManager};

    use lazy_static::lazy_static;

    const COUNT: usize = 50;
    const SAMPLE_SIZE: usize = 15;

    pub(crate) fn murmur_message_sequence<M: Message + 'static>(
        message: M,
        keypair: &KeyPair,
        peer_count: usize,
    ) -> impl Iterator<Item = MurmurMessage<M>> {
        let signature = Signer::new(keypair.clone())
            .sign(&message)
            .expect("sign failed");
        (0..peer_count)
            .map(move |_| MurmurMessage::Gossip(signature, message.clone()))
    }

    #[tokio::test(threaded_scheduler)]
    async fn delivery() {
        lazy_static! {
            static ref KEYPAIR: RwLock<Option<KeyPair>> = RwLock::new(None);
        }

        let sampler = PoissonSampler {};
        let keypair = KeyPair::random();
        let sender = keypair.public().clone();

        *KEYPAIR.write().await = Some(keypair);

        let (_, handles, system) =
            create_system(COUNT, |mut connection| async move {
                let mut signer = Signer::new(
                    KEYPAIR.read().await.as_ref().expect("bug").clone(),
                );
                let message = 0usize;
                let signature = signer.sign(&message).expect("failed to sign");
                let message = MurmurMessage::Gossip(signature, message);

                connection.send(&message).await.expect("send failed");
            })
            .await;

        let manager = SystemManager::new(system);
        let self_keypair = Arc::new(KeyPair::random());
        let processor = Murmur::<usize>::new_receiver(sender, self_keypair, 25);

        let mut handle = manager.run(processor, sampler).await;

        let message = handle.deliver().await.expect("no message delivered");

        assert_eq!(message, 0usize, "wrong message received");

        handles.await.expect("sender panicked");
    }

    #[tokio::test]
    async fn broadcast() {
        const MESSAGE: usize = 0;
        lazy_static! {
            static ref KEYPAIR: Arc<KeyPair> = Arc::new(KeyPair::random());
        };
        let keypair = KEYPAIR.clone();
        let sender = Murmur::new_sender(keypair.clone(), SAMPLE_SIZE);
        let (_, handles, system) =
            create_system(COUNT, |mut connection| async move {
                while let Ok(msg) = tokio::time::timeout(
                    Duration::from_millis(100),
                    connection.receive::<MurmurMessage<usize>>(),
                )
                .await
                {
                    match msg.expect("recv failed") {
                        MurmurMessage::Gossip(ref signature, ref message) => {
                            let mut signer = Signer::random();

                            signer
                                .verify(signature, KEYPAIR.public(), message)
                                .expect("failed to verify message");

                            assert_eq!(0usize, *message, "wrong message");
                        }
                        MurmurMessage::Subscribe => continue,
                    }
                }
            })
            .await;
        let sampler = AllSampler::default();

        let manager = SystemManager::new(system);
        let mut handle = manager.run(sender, sampler).await;

        handle.broadcast(&MESSAGE).await.expect("broadcast failed");

        handles.await.expect("panic error");
    }

    #[tokio::test]
    async fn subscribe() {
        let keypair = Arc::new(KeyPair::random());
        let keys = keyset(COUNT).collect::<HashSet<_>>();
        let subscribes = keys
            .iter()
            .zip((0..COUNT).map(|_| MurmurMessage::Subscribe));

        let processor =
            Arc::new(Murmur::<usize>::new_sender(keypair, SAMPLE_SIZE));
        let sender = Arc::new(CollectingSender::new(keys.iter().cloned()));

        use futures::future;

        future::join_all(subscribes.map(|(key, sub)| {
            processor
                .clone()
                .process(Arc::new(sub), *key, sender.clone())
        }))
        .await;

        let (skeys, messages): (Vec<_>, Vec<_>) =
            sender.messages().await.into_iter().unzip();

        assert!(skeys.into_iter().all(|x| keys.contains(&x)));
        messages
            .into_iter()
            .for_each(|msg| assert_eq!(&MurmurMessage::Subscribe, msg.deref()));
    }
}
