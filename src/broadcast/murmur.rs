use std::collections::HashSet;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::Arc;

use crate::{Message, Processor, Sender};
use classic_derive::message;

use drop::async_trait;
use drop::crypto::key::exchange::PublicKey;
use drop::crypto::sign::{self, KeyPair, Signature, Signer};

use serde::{Deserialize, Serialize};

use snafu::{ResultExt, Snafu};

use peroxide::fuga::*;

use tokio::sync::{mpsc, watch, Mutex, RwLock};

use tracing::{debug, debug_span, error};
use tracing_futures::Instrument;

#[derive(Snafu, Debug)]
/// Error type returned by `Probablistic` broadcast instances
pub enum BroadcastError {
    #[snafu(display("already sent a message using this instance"))]
    /// Since `Probabilistic` broadcast is a "one-shot" primitive
    /// this error is returned when the instance has already been
    /// used to broadcast or receive a message.
    AlreadyUsed,
    #[snafu(display("this broadcast instance is a receiver"))]
    /// This error is returned when trying to broadcast a message using
    /// a `Probabilistic` instance that was created using
    /// `Probabilistic::new_receiver`
    NotASender,
    #[snafu(display("couldn't sign message"))]
    SignError,
}

#[message]
/// Message exchanged by the murmur algorithm
pub enum MurmurMessage<M: Message> {
    #[serde(bound(deserialize = "M: Message"))]
    /// Message used during gossiping by the murmur algorithm
    Gossip(Signature, M),
    /// Subscription request for the murmur algorithm
    Subscribe,
}

/// A probabilistic broadcast using Erdös-Rényi Gossip
pub struct Murmur<M: Message + 'static> {
    keypair: Arc<KeyPair>,
    sender: sign::PublicKey,
    delivered_tx: watch::Sender<Option<M>>,
    delivery: Option<watch::Receiver<Option<M>>>,
    delivered: Mutex<Option<M>>,
    gossip: Arc<RwLock<HashSet<PublicKey>>>,
    expected: usize,
}

impl<M: Message + 'static> Murmur<M> {
    /// Create a new probabilistic broadcast sender with your local `KeyPair`
    pub fn new_sender(keypair: Arc<KeyPair>, expected: usize) -> Self {
        let (delivered_tx, delivered_rx) = watch::channel(None);
        let sender = keypair.public().clone();

        Self {
            keypair,
            sender,
            delivered_tx,
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
        let (delivered_tx, delivered_rx) = watch::channel(None);

        Self {
            expected,
            sender,
            keypair,
            delivered_tx,
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

                    if let Err(e) =
                        self.delivered_tx.broadcast(Some(content.clone()))
                    {
                        error!("failed to deliver message: {}", e);
                    }

                    let dest = self.gossip.read().await;

                    sender.send_many(message, dest.iter()).await;
                }
            }
        }
    }

    async fn output(&mut self, msg_sender: Arc<S>) -> Self::Handle {
        let (sender, mut receiver) = mpsc::channel(128);
        let keys = msg_sender.keys().await;
        let count = keys.len();
        let prob = self.expected as f64 / count as f64;
        let sampler = Bernoulli(prob);

        let sample = msg_sender
            .keys()
            .await
            .into_iter()
            .zip(sampler.sample(count))
            .filter_map(
                |(key, select)| if select >= 1.0 { Some(key) } else { None },
            )
            .collect::<Vec<_>>();

        self.gossip.write().await.extend(sample);

        let gossip = self.gossip.clone();

        tokio::task::spawn(
            async move {
                let sender = msg_sender;
                let gossip = gossip;

                if let Some(out) = receiver.recv().await {
                    sender.send_many(out, gossip.read().await.iter()).await;
                }
            }
            .instrument(debug_span!("murmur_gossip")),
        );

        let delivery = self
            .delivery
            .take()
            .expect("double setup of processor detected");

        MurmurHandle {
            sender,
            delivery,
            keypair: self.keypair.clone(),
        }
    }
}

/// A `Handle` for interacting with a `Probabilistic` instance
/// to broadcast or deliver a message
pub struct MurmurHandle<M: Message + 'static> {
    delivery: watch::Receiver<Option<M>>,
    sender: mpsc::Sender<Arc<MurmurMessage<M>>>,
    keypair: Arc<KeyPair>,
}

impl<M: Message + 'static> MurmurHandle<M> {
    /// Deliver a `Message` using probabilistic broadcast. Calling this method
    /// will consume the `Handle` since probabilistic broadcast is a one use
    /// primitive
    pub async fn deliver(mut self) -> Option<M> {
        match self.delivery.recv().await {
            Some(None) => self.delivery.recv().await.flatten(),
            Some(msg) => msg,
            None => {
                error!("no message to deliver");
                None
            }
        }
    }

    /// Check to see if a message is available to delivered using this handle.
    /// Returns `None` if no message is available right now.
    pub fn try_deliver(&mut self) -> Option<M> {
        if let Some(message) = self.delivery.borrow().deref() {
            Some(message.clone())
        } else {
            None
        }
    }

    /// Broadcast a `Message` using the associated probabilistic broadcast
    /// instance. This will consume the `Handle` since this primitive is a
    /// one-shot.
    pub async fn broadcast(
        mut self,
        message: &M,
    ) -> Result<(), BroadcastError> {
        debug!("broadcasting {:?}", message);

        let mut signer = Signer::new(self.keypair.deref().clone());
        let signature = signer
            .sign(message)
            .map_err(|_| snafu::NoneError)
            .context(SignError)?;
        let message = MurmurMessage::Gossip(signature, message.clone());

        self.sender
            .send(Arc::new(message.clone()))
            .await
            .map_err(|_| snafu::NoneError)
            .context(NotASender)
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;

    use std::time::Duration;

    use crate::test::*;
    use crate::{CollectingSender, SystemManager};

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

        let handle = manager.run(processor).await;

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

        let manager = SystemManager::new(system);
        let handle = manager.run(sender).await;

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
