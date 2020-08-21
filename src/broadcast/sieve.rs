use std::collections::HashSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;

use super::probabilistic::{PcbMessage, Probabilistic, ProbabilisticHandle};
use crate::{Message, Processor, Sender};

use drop::async_trait;
use drop::crypto::key::exchange::PublicKey;
use drop::crypto::sign::{self, KeyPair, Signature, Signer};

use serde::{Deserialize, Serialize};

use snafu::{OptionExt, ResultExt, Snafu};

use tokio::sync::{mpsc, Mutex, RwLock};

use tracing::{debug, error};

#[derive(Snafu, Debug)]
pub enum SieveError {
    #[snafu(display("this handle is not a sender handle"))]
    NotASender,

    #[snafu(display("the associated sender died too early"))]
    SenderDied,
}

#[derive(Clone, Eq, PartialEq, Deserialize, Serialize)]
/// Type of message exchanged for probabilistic double echo
enum PdeMessage<M: Message> {
    #[serde(bound(deserialize = "M: Message"))]
    Probabilistic(PcbMessage<(M, Signature)>),
    #[serde(bound(deserialize = "M: Message"))]
    Echo(sign::PublicKey, M, Signature),
    /// Subscribe to the `Echo` set of a remote peer
    EchoSubscribe,
}

#[allow(clippy::derive_hash_xor_eq)]
impl<M: Message> Hash for PdeMessage<M> {
    fn hash<H: Hasher>(&self, _hasher: &mut H) {
        todo!()
    }
}

impl<M: Message> fmt::Debug for PdeMessage<M> {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
    }
}

impl<M: Message> Message for PdeMessage<M> {}

impl<M: Message> Message for (M, Signature) {}

/// An implementation of probabilistic double echo
/// broadcast.
pub struct Sieve<M: Message + 'static> {
    deliverer: Option<mpsc::Sender<M>>,
    sender: sign::PublicKey,
    keypair: Arc<KeyPair>,

    echo: Mutex<Option<(M, Signature)>>,
    echo_set: RwLock<HashSet<PublicKey>>,
    echo_replies: RwLock<HashSet<PublicKey>>,
    echo_threshold: usize,

    probabilistic: Arc<Probabilistic<(M, Signature)>>,
    handle: Option<ProbabilisticHandle<(M, Signature)>>,
}

impl<M: Message> Sieve<M> {
    /// Create a new double echo receiver for the given sender.
    /// * Arguments
    /// `sender` designated sender's public key for this instance
    /// `keypair` local keypair used  for signing
    /// `echo_threshold` number of echo messages to wait for before delivery
    /// `pb_size` expected sample size
    pub fn new_receiver(
        sender: sign::PublicKey,
        keypair: Arc<KeyPair>,
        echo_threshold: usize,
        pb_size: usize,
    ) -> Self {
        let probabilistic = Probabilistic::new_receiver(
            sender.clone(),
            keypair.clone(),
            pb_size,
        );

        Self {
            sender,
            deliverer: None,
            keypair,

            echo: Mutex::new(None),
            echo_threshold,
            echo_set: RwLock::new(HashSet::with_capacity(pb_size)),
            echo_replies: RwLock::new(HashSet::with_capacity(echo_threshold)),

            probabilistic: Arc::new(probabilistic),
            handle: None,
        }
    }

    /// Create a new double echo receiver.
    pub fn new_sender(
        keypair: Arc<KeyPair>,
        echo_threshold: usize,
        pb_size: usize,
    ) -> Self {
        let probabilistic = Probabilistic::new_sender(keypair.clone(), pb_size);

        Self {
            sender: keypair.public().clone(),
            keypair,
            deliverer: None,

            echo: Mutex::new(None),
            echo_threshold,
            echo_set: RwLock::new(HashSet::with_capacity(pb_size)),
            echo_replies: RwLock::new(HashSet::with_capacity(echo_threshold)),

            probabilistic: Arc::new(probabilistic),
            handle: None,
        }
    }

    async fn check_set(
        from: PublicKey,
        signature: &Signature,
        message: &M,
        set: &RwLock<HashSet<PublicKey>>,
        delivered: &Mutex<Option<(M, Signature)>>,
        threshold: usize,
    ) -> bool {
        if let Some((recv_message, recv_signature)) =
            delivered.lock().await.deref()
        {
            if recv_signature != signature || recv_message != message {
                return false;
            } else {
                set.write().await.insert(from);
            }
        }

        set.read().await.len() >= threshold
    }

    async fn check_echo_set(
        &self,
        from: PublicKey,
        signature: &Signature,
        message: &M,
    ) -> bool {
        if self.echo_set.read().await.contains(&from) {
            Self::check_set(
                from,
                signature,
                message,
                &self.echo_replies,
                &self.echo,
                self.echo_threshold,
            )
            .await
        } else {
            false
        }
    }

    fn signer(&self) -> Signer {
        Signer::new(self.keypair.deref().clone())
    }
}

#[async_trait]
impl<M: Message> Processor<PdeMessage<M>, M> for Sieve<M> {
    type Handle = SieveHandle<M>;

    async fn process(
        self: Arc<Self>,
        message: &PdeMessage<M>,
        from: PublicKey,
        sender: Arc<Sender>,
    ) {
        match message {
            PdeMessage::Echo(pkey, message, signature)
                if *pkey == self.sender =>
            {
                let mut signer = self.signer();

                if signer.verify(signature, pkey, message).is_ok()
                    && self.check_echo_set(from, signature, message).await
                {
                    let message = PdeMessage::Echo(
                        pkey.clone(),
                        message.clone(),
                        *signature,
                    );

                    sender
                        .send_many(
                            Arc::new(message),
                            self.echo_set.read().await.iter(),
                        )
                        .await;
                }
            }
            PdeMessage::EchoSubscribe => {
                if let Some((message, signature)) =
                    self.echo.lock().await.deref()
                {
                    let message = PdeMessage::Echo(
                        self.sender.clone(),
                        message.clone(),
                        *signature,
                    );

                    if let Err(e) =
                        sender.send_to(&from, Arc::new(message)).await
                    {
                        error!("failed to echo message to {}: {}", from, e);
                    }
                }

                self.echo_set.write().await.insert(from);
            }

            PdeMessage::Probabilistic(msg) => {
                self.probabilistic.clone().process(msg, from, sender).await;
            }

            e => debug!("ignoring {:?}", e),
        }
    }

    async fn output(&mut self, sender: Arc<Sender>) -> Self::Handle {
        let (outgoing_tx, _outgoing_rx) = mpsc::channel(1);
        let (incoming_tx, incoming_rx) = mpsc::channel(1);

        let handle = Arc::get_mut(&mut self.probabilistic)
            .expect("setup error")
            .output(sender)
            .await;

        self.handle.replace(handle);
        self.deliverer.replace(incoming_tx);

        let outgoing_tx = if self.sender == *self.keypair.public() {
            Some(outgoing_tx)
        } else {
            None
        };

        SieveHandle::new(self.keypair.clone(), incoming_rx, outgoing_tx)
    }
}

/// A handle to interact with a `Sieve` instance once it has been scheduled to
/// run on a `SystemManager`
pub struct SieveHandle<M: Message> {
    incoming: mpsc::Receiver<M>,
    outgoing: Option<mpsc::Sender<PdeMessage<M>>>,
    signer: Signer,
}

impl<M: Message> SieveHandle<M> {
    fn new(
        keypair: Arc<KeyPair>,
        incoming: mpsc::Receiver<M>,
        outgoing: Option<mpsc::Sender<PdeMessage<M>>>,
    ) -> Self {
        Self {
            signer: Signer::new(keypair.deref().clone()),
            incoming,
            outgoing,
        }
    }
    /// Deliver a `Message` using probabilistic double echo.
    /// Value receiver prevents double use of this Handle since the primitive is
    /// one-shot
    pub async fn deliver(&mut self) -> Option<M> {
        self.incoming.recv().await
    }

    /// Broadcast a message using the associated `ProbabilisticDoubleEcho`.
    /// This will return an error if the `Sieve` was created using
    /// `Sieve::new_receiver`
    pub async fn broadcast(&mut self, message: M) -> Result<(), SieveError> {
        let mut sender = self.outgoing.take().context(NotASender)?;
        let signature =
            self.signer.sign(&message).expect("failed to sign message");
        let message =
            PdeMessage::Echo(self.signer.public().clone(), message, signature);

        sender
            .send(message)
            .await
            .map_err(|_| snafu::NoneError)
            .context(SenderDied)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    #[tokio::test]
    async fn delivery() {
        todo!()
    }

    #[tokio::test]
    async fn broadcast() {
        todo!()
    }
}
