use std::collections::HashSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;

use crate::{Message, Processor, Sender};

use drop::async_trait;
use drop::crypto::key::exchange::PublicKey;
use drop::crypto::sign::{self, KeyPair, Signature, Signer};

use serde::{Deserialize, Serialize};

use snafu::{ResultExt, Snafu};

use peroxide::fuga::*;

use tokio::sync::{mpsc, watch, Mutex, RwLock};

use tracing::{debug, error};

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

#[derive(Deserialize, Serialize, Clone, Eq, PartialEq)]
enum PcbMessage<M: Message> {
    #[serde(bound(deserialize = "M: Message"))]
    Gossip(sign::PublicKey, Signature, M),
    Subscribe,
}

impl<M: Message> Hash for PcbMessage<M> {
    fn hash<H: Hasher>(&self, h: &mut H) {
        match self {
            PcbMessage::Gossip(pkey, signature, message) => {
                signature.hash(h);
                message.hash(h);
            }
            PcbMessage::Subscribe => 0.hash(h),
        }
    }
}

impl<M: Message> Message for PcbMessage<M> {}

impl<M: Message> fmt::Debug for PcbMessage<M> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PcbMessage::Gossip(_, _, msg) => {
                write!(f, "gossip message {:?}", msg)
            }
            PcbMessage::Subscribe => write!(f, "gossip subscribe message"),
        }
    }
}

/// A probabilistic broadcast using Erdös-Rényi Gossip
pub struct Probabilistic<M: Message + 'static> {
    keypair: Arc<KeyPair>,
    sender: sign::PublicKey,
    delivered_tx: watch::Sender<Option<M>>,
    delivery: Option<watch::Receiver<Option<M>>>,
    delivered: Mutex<Option<M>>,
    gossip: Arc<RwLock<HashSet<PublicKey>>>,
    expected: usize,
}

impl<M: Message + 'static> Probabilistic<M> {
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
impl<M: Message + 'static> Processor<PcbMessage<M>, M> for Probabilistic<M> {
    type Handle = ProbabilisticHandle<M>;

    async fn process(
        self: Arc<Self>,
        message: &PcbMessage<M>,
        from: PublicKey,
        sender: Arc<Sender<PcbMessage<M>>>,
    ) {
        match message {
            PcbMessage::Subscribe => {
                debug!("new peer {} subscribed to us", from);
                self.gossip.write().await.insert(from);
            }
            PcbMessage::Gossip(pkey, signature, content) => {
                if *pkey == self.sender {
                    let mut signer = Signer::new(self.keypair.deref().clone());
                    let mut delivered = self.delivered.lock().await;

                    if signer.verify(&signature, pkey, content).is_ok() {
                        debug!("good signature for {:?}", message);

                        delivered.replace(content.clone());

                        std::mem::drop(delivered);

                        if let Err(e) =
                            self.delivered_tx.broadcast(Some(content.clone()))
                        {
                            error!("failed to deliver message: {}", e);
                        }

                        let dest = self.gossip.read().await;

                        sender
                            .send_many(Arc::new(message.clone()), dest.iter())
                            .await;
                    }
                }
            }
        }
    }

    async fn output(
        &mut self,
        msg_sender: Arc<Sender<PcbMessage<M>>>,
    ) -> Self::Handle {
        let (sender, mut receiver) = mpsc::channel(128);
        let count = msg_sender.keys().count();
        let prob = self.expected as f64 / count as f64;
        let sampler = Bernoulli(prob);

        let sample = msg_sender
            .keys()
            .zip(sampler.sample(count))
            .filter_map(
                |(key, select)| if select >= 1.0 { Some(key) } else { None },
            )
            .collect::<Vec<_>>();

        self.gossip.write().await.extend(sample);

        let gossip = self.gossip.clone();

        tokio::task::spawn(async move {
            let sender = msg_sender;
            let gossip = gossip;

            if let Some(out) = receiver.recv().await {
                sender.send_many(out, gossip.read().await.iter()).await;
            }
        });

        let delivery = self
            .delivery
            .take()
            .expect("double setup of processor detected");

        ProbabilisticHandle {
            sender,
            delivery,
            keypair: self.keypair.clone(),
        }
    }
}

/// A `Handle` for interacting with a `Probabilistic` instance
/// to broadcast or deliver a message
pub struct ProbabilisticHandle<M: Message + 'static> {
    delivery: watch::Receiver<Option<M>>,
    sender: mpsc::Sender<Arc<PcbMessage<M>>>,
    keypair: Arc<KeyPair>,
}

impl<M: Message + 'static> ProbabilisticHandle<M> {
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
        let message = PcbMessage::Gossip(
            self.keypair.public().clone(),
            signature,
            message.clone(),
        );

        self.sender
            .send(Arc::new(message.clone()))
            .await
            .map_err(|_| snafu::NoneError)
            .context(NotASender)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::time::Duration;

    use crate::test::*;
    use crate::SystemManager;

    use lazy_static::lazy_static;

    const COUNT: usize = 50;
    const SAMPLE_SIZE: usize = 15;

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
                let sender = signer.public().clone();
                let signature = signer.sign(&message).expect("failed to sign");
                let message =
                    PcbMessage::Gossip(sender.clone(), signature, message);

                connection.send(&message).await.expect("send failed");
            })
            .await;

        let manager = SystemManager::new(system);
        let self_keypair = Arc::new(KeyPair::random());
        let processor =
            Probabilistic::<usize>::new_receiver(sender, self_keypair, 25);

        let handle = manager.run(processor).await;

        let message = handle.deliver().await.expect("no message delivered");

        assert_eq!(message, 0usize, "wrong message received");

        handles.await.expect("sender panicked");
    }

    #[tokio::test]
    async fn broadcast() {
        const MESSAGE: usize = 0;
        let keypair = Arc::new(KeyPair::random());
        let sender = Probabilistic::new_sender(keypair.clone(), SAMPLE_SIZE);
        let (_, handles, system) =
            create_system(COUNT, |mut connection| async move {
                while let Ok(msg) = tokio::time::timeout(
                    Duration::from_millis(100),
                    connection.receive::<PcbMessage<usize>>(),
                )
                .await
                {
                    match msg.expect("recv failed") {
                        PcbMessage::Gossip(
                            ref sender,
                            ref signature,
                            ref message,
                        ) => {
                            let mut signer = Signer::random();

                            signer
                                .verify(signature, sender, message)
                                .expect("failed to verify message");

                            assert_eq!(0usize, *message, "wrong message");
                        }
                        PcbMessage::Subscribe => continue,
                    }
                }
            })
            .await;

        let manager = SystemManager::new(system);
        let handle = manager.run(sender).await;

        handle.broadcast(&MESSAGE).await.expect("broadcast failed");

        handles.await.expect("panic error");
    }
}
