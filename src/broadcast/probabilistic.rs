use std::collections::HashSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::sync::Arc;

use crate::{Message, Processor, Sender};

use drop::async_trait;
use drop::crypto::key::exchange::PublicKey;
use drop::crypto::sign::{self, Signature, Signer};

use peroxide::fuga::*;

use serde::{Deserialize, Serialize};

use snafu::{OptionExt, ResultExt, Snafu};

use tokio::sync::{mpsc, watch, Mutex, RwLock};

use tracing::{debug, debug_span, error, warn};
use tracing_futures::Instrument;

#[derive(Snafu, Debug)]
pub enum BroadcastError {
    #[snafu(display("already sent a message using this instance"))]
    AlreadyUsed,
    #[snafu(display("this broadcast instance is a receiver"))]
    NotASender,
    #[snafu(display("libsodium failure"))]
    Sodium,
}

#[derive(Deserialize, Serialize, Clone, Eq, PartialEq)]
pub enum PcbMessage<M: Message> {
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
    keypair: sign::KeyPair,
    sender: sign::PublicKey,
    delivered_tx: watch::Sender<Option<M>>,
    delivery: Option<watch::Receiver<Option<M>>>,
    delivered: Mutex<Option<M>>,
    gossip: RwLock<HashSet<PublicKey>>,
}

impl<M: Message + 'static> Probabilistic<M> {
    /// Create a new probabilistic broadcast sender with your local `KeyPair`
    pub fn new_sender(keypair: sign::KeyPair) -> Self {
        todo!()
    }

    /// Create a new probabilistic broadcast receiver with designated sender and
    /// the local `KeyPair`
    pub fn new_receiver(
        sender: sign::PublicKey,
        keypair: sign::KeyPair,
        expected: usize,
    ) -> Self {
        let (delivered_tx, delivered_rx) = watch::channel(None);

        Self {
            sender,
            keypair,
            delivered_tx,
            delivery: Some(delivered_rx),
            delivered: Mutex::new(None),
            gossip: RwLock::new(HashSet::with_capacity(expected)),
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
                self.gossip.write().await.insert(from);
            }
            PcbMessage::Gossip(pkey, signature, content) => {
                if *pkey == self.sender {
                    let mut signer = Signer::new(self.keypair.clone());
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
        sender: Arc<Sender<PcbMessage<M>>>,
    ) -> Self::Handle {
        let (sender, mut receiver) = mpsc::channel(128);

        tokio::task::spawn(async move {
            if let Some(out) = receiver.recv().await {
                todo!("sign and broadcast message");
            }
        });

        let delivery = self
            .delivery
            .take()
            .expect("double setup of processor detected");

        ProbabilisticHandle { sender, delivery }
    }
}

/// A `Handle` for interacting with a `Probabilistic` instance
/// to broadcast or deliver a message
pub struct ProbabilisticHandle<M: Message + 'static> {
    delivery: watch::Receiver<Option<M>>,
    sender: mpsc::Sender<M>,
}

impl<M: Message + 'static> ProbabilisticHandle<M> {
    /// Deliver a `Message` using probabilistic broadcast
    pub async fn deliver(mut self) -> Option<M> {
        match self.delivery.recv().await {
            Some(None) => self.delivery.recv().await.flatten(),
            Some(msg) => msg,
            None => {
                error!("double use of handle detected");
                None
            }
        }
    }

    /// Broadcast a `Message` using the associated probabilistic broadcast
    /// instance
    pub fn broadcast(mut self, message: &M) -> Result<(), BroadcastError> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::test::*;
    use crate::SystemManager;

    static COUNT: usize = 50;

    #[tokio::test]
    async fn single_delivery() {
        let (pkeys, handles, system) =
            create_system(COUNT, |mut connection| async move { todo!() }).await;

        let manager = SystemManager::new(system);
        let local = sign::KeyPair::random();
        let processor = Probabilistic::new_receiver();
    }
}
