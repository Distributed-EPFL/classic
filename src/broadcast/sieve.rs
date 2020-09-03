use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;

use super::probabilistic::{PcbMessage, Probabilistic, ProbabilisticHandle};
use crate::{Message, Processor, Sampler, Sender};

use drop::async_trait;
use drop::crypto::key::exchange::PublicKey;
use drop::crypto::sign::{self, KeyPair, Signature, Signer};

use serde::{Deserialize, Serialize};

use snafu::{OptionExt, ResultExt, Snafu};

use tokio::sync::{mpsc, Mutex, RwLock};

use tracing::{debug, error};

#[derive(Snafu, Debug)]
/// Type of errors returned by `SieveHandle`
pub enum SieveError {
    #[snafu(display("this handle is not a sender handle"))]
    /// Tried to call `SieveHandle::broadcast` on an instance created using
    /// `Sieve::new_receiver`
    NotASender,

    #[snafu(display("the associated sender died too early"))]
    /// The associated `Sieve` instance does not exist anymore, and the
    /// message couldn't be broadcasted
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
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PdeMessage::EchoSubscribe => write!(f, "sieve echo subscribe"),
            PdeMessage::Probabilistic(msg) => {
                write!(f, "murmur message {:?}", msg)
            }
            PdeMessage::Echo(_, msg, _) => {
                write!(f, "sieve echo message {:?}", msg)
            }
        }
    }
}

impl<M: Message> Message for PdeMessage<M> {}

impl<M: Message> Message for (M, Signature) {}

/// An implementation of the `Sieve` probabilistic consistent broadcast
/// algorithm. `Sieve` is a single-shot shot broadcast algorithm using
/// a designated sender for each instance.
pub struct Sieve<M: Message + 'static> {
    deliverer: Mutex<Option<mpsc::Sender<M>>>,
    sender: sign::PublicKey,
    keypair: Arc<KeyPair>,

    expected: usize,

    echo: Mutex<Option<(M, Signature)>>,
    echo_set: RwLock<HashSet<PublicKey>>,
    echo_replies: RwLock<HashMap<PublicKey, (M, Signature)>>,
    echo_threshold: usize,

    probabilistic: Arc<Probabilistic<(M, Signature)>>,
    handle: Mutex<Option<ProbabilisticHandle<(M, Signature)>>>,
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
            deliverer: Mutex::new(None),
            keypair,
            expected: pb_size,

            echo: Mutex::new(None),
            echo_threshold,
            echo_set: RwLock::new(HashSet::with_capacity(pb_size)),
            echo_replies: RwLock::new(HashMap::with_capacity(echo_threshold)),

            probabilistic: Arc::new(probabilistic),
            handle: Mutex::new(None),
        }
    }

    /// Create a new `Sieve` receiver.
    pub fn new_sender(
        keypair: Arc<KeyPair>,
        echo_threshold: usize,
        pb_size: usize,
    ) -> Self {
        let probabilistic = Probabilistic::new_sender(keypair.clone(), pb_size);

        Self {
            sender: keypair.public().clone(),
            keypair,
            deliverer: Mutex::new(None),
            expected: pb_size,

            echo: Mutex::new(None),
            echo_threshold,
            echo_set: RwLock::new(HashSet::with_capacity(pb_size)),
            echo_replies: RwLock::new(HashMap::with_capacity(echo_threshold)),

            probabilistic: Arc::new(probabilistic),
            handle: Mutex::new(None),
        }
    }

    async fn check_set(
        from: PublicKey,
        signature: &Signature,
        message: &M,
        set: &RwLock<HashMap<PublicKey, (M, Signature)>>,
        delivered: &Mutex<Option<(M, Signature)>>,
        threshold: usize,
    ) -> bool {
        if let Some((recv_message, recv_signature)) =
            delivered.lock().await.deref()
        {
            if recv_signature == signature && recv_message == message {
                debug!("registered correct echo from {}", from);
                match set.write().await.entry(from) {
                    Entry::Occupied(_) => return false,
                    Entry::Vacant(e) => {
                        e.insert((message.clone(), *signature));
                    }
                }
            }
        }

        set.read()
            .await
            .values()
            .filter(|(stored_message, stored_signature)| {
                stored_message == message && stored_signature == signature
            })
            .count()
            >= threshold
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
                debug!("echo message from {}", from);
                let mut signer = self.signer();

                if signer.verify(signature, pkey, message).is_ok()
                    && self.check_echo_set(from, signature, message).await
                {
                    debug!("reached delivery threshold");
                    if let Some(mut sender) = self.deliverer.lock().await.take()
                    {
                        if sender.send(message.clone()).await.is_err() {
                            error!("handle doesn't exist anymore for delivery");
                        }
                    } else {
                        debug!("already delivered a message");
                    }
                }
            }
            PdeMessage::EchoSubscribe => {
                if let Some((message, signature)) =
                    self.echo.lock().await.deref()
                {
                    debug!("echo subscription from {}", from);
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
                debug!("processing murmur message {:?}", msg);
                self.probabilistic.clone().process(msg, from, sender).await;

                let mut guard = self.handle.lock().await;

                if let Some(mut handle) = guard.take() {
                    if let Some((message, signature)) = handle.try_deliver() {
                        debug!("delivered {:?} using murmur", message);
                        *self.echo.lock().await = Some((message, signature));
                    } else {
                        guard.replace(handle);
                    }
                } else {
                    debug!("late message for probabilistic broadcast");
                }
            }

            e => debug!("ignoring {:?}", e),
        }
    }

    async fn output(&mut self, sender: Arc<Sender>) -> Self::Handle {
        let (outgoing_tx, _outgoing_rx) = mpsc::channel(1);
        let (incoming_tx, incoming_rx) = mpsc::channel(1);
        let sampler = Sampler::new(sender.clone());
        let subscribe_sender = sender.clone();

        let handle = Arc::get_mut(&mut self.probabilistic)
            .expect("setup error")
            .output(sender)
            .await;

        debug!("sampling for echo set");

        let sample = sampler
            .sample(self.expected)
            .await
            .expect("sampling failed");

        self.echo_set.write().await.extend(sample);

        subscribe_sender
            .send_many(
                Arc::new(PdeMessage::<M>::EchoSubscribe),
                self.echo_set.read().await.iter(),
            )
            .await;

        self.handle.lock().await.replace(handle);
        self.deliverer.lock().await.replace(incoming_tx);

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
    /// Deliver a `Message` using the `Sieve` algorithm. Since `Sieve` is a
    /// one-shot algorithm, a `SieveHandle` can only deliver one message.
    /// All subsequent calls to this method will return `None`.
    pub async fn deliver(&mut self) -> Option<M> {
        self.incoming.recv().await
    }

    /// Broadcast a message using the associated `Sieve` instance. <br />
    /// This will return an error if the `Sieve` was created using
    /// `Sieve::new_receiver` or if this is not the first time this method is
    /// called
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
    use super::*;

    use crate::test::*;
    use crate::SystemManager;

    const SIZE: usize = 50;
    const MESSAGE: usize = 0;

    #[tokio::test]
    async fn delivery() {
        let keypair = KeyPair::random();
        let sender_keypair = KeyPair::random();
        let sender_pkey = sender_keypair.public().clone();
        let orig_signature = Signer::new(sender_keypair.clone())
            .sign(&MESSAGE)
            .expect("failed to sign");
        let echo_message =
            PdeMessage::Echo(sender_pkey, MESSAGE, orig_signature);

        let (_, handle, system) = create_system(SIZE, move |mut connection| {
            let echo_message = echo_message.clone();

            async move {
                while let Ok(message) =
                    connection.receive::<PdeMessage<usize>>().await
                {
                    match message {
                        PdeMessage::EchoSubscribe => {
                            connection
                                .send(&echo_message)
                                .await
                                .expect("send failed");
                        }
                        PdeMessage::Echo(_, message, signature) => {
                            assert_eq!(
                                message, MESSAGE,
                                "wrong message echoed"
                            );
                            assert_eq!(
                                signature, orig_signature,
                                "wrong signature"
                            );
                        }
                        PdeMessage::Probabilistic(_) => unreachable!(),
                    }
                }
            }
        })
        .await;
        let manager = SystemManager::new(system);

        let sieve = Sieve::new_receiver(
            sender_keypair.public().clone(),
            Arc::new(keypair),
            SIZE / 4,
            SIZE / 2,
        );

        let mut sieve = manager.run(sieve).await;

        let message: usize =
            sieve.deliver().await.expect("no message delivered");

        assert_eq!(MESSAGE, message, "wrong message delivered");

        handle.await.expect("system failure");
    }

    #[tokio::test]
    async fn broadcast() {
        todo!()
    }
}
