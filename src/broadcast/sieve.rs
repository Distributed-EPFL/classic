use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::ops::Deref;
use std::sync::Arc;

use super::probabilistic::{PcbMessage, Probabilistic, ProbabilisticHandle};
use crate::{ConvertSender, Message, Processor, Sampler, Sender};

use classic_derive::message;

use drop::async_trait;
use drop::crypto::key::exchange::PublicKey;
use drop::crypto::sign::{self, KeyPair, Signature, Signer};

use serde::{Deserialize, Serialize};

use snafu::{OptionExt, ResultExt, Snafu};

use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task;

use tracing::{debug, error, warn};

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

/// Type of message exchanged for probabilistic double echo
#[message]
pub enum SieveMessage<M: Message> {
    #[serde(bound(deserialize = "M: Message"))]
    /// Wraps a message from Murmur into a SieveMessage
    Probabilistic(PcbMessage<(M, Signature)>),
    #[serde(bound(deserialize = "M: Message"))]
    /// Message used during echo rounds
    Echo(M, Signature),
    /// Subscribe to the `Echo` set of a remote peer
    EchoSubscribe,
}

impl<M: Message> From<PcbMessage<(M, Signature)>> for SieveMessage<M> {
    fn from(v: PcbMessage<(M, Signature)>) -> Self {
        SieveMessage::Probabilistic(v)
    }
}

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

    async fn check_echo_set(&self, signature: &Signature, message: &M) -> bool {
        let count = self
            .echo_replies
            .read()
            .await
            .values()
            .filter(|(stored_message, stored_signature)| {
                stored_message == message && stored_signature == signature
            })
            .count();

        if count >= self.echo_threshold {
            debug!("reached delivery threshold, checking correctness");

            if let Some((recv_msg, recv_signature)) = &*self.echo.lock().await {
                if recv_msg == message && recv_signature == signature {
                    true
                } else {
                    warn!("mismatched message received");
                    false
                }
            } else {
                false
            }
        } else {
            debug!(
                "still waiting for {} messages",
                self.echo_threshold - count
            );
            false
        }
    }

    async fn update_echo_set(
        &self,
        from: PublicKey,
        signature: &Signature,
        message: &M,
    ) -> bool {
        if self.echo_set.read().await.contains(&from) {
            match self.echo_replies.write().await.entry(from) {
                Entry::Occupied(_) => false,
                Entry::Vacant(e) => {
                    debug!("registered correct echo message from {}", from);
                    e.insert((message.clone(), *signature));
                    true
                }
            }
        } else {
            false
        }
    }

    fn signer(&self) -> Signer {
        Signer::new(self.keypair.deref().clone())
    }
}

#[async_trait]
impl<M, S> Processor<SieveMessage<M>, M, S> for Sieve<M>
where
    S: Sender<SieveMessage<M>> + 'static,
    M: Message + 'static,
{
    type Handle = SieveHandle<M>;

    async fn process(
        self: Arc<Self>,
        message: Arc<SieveMessage<M>>,
        from: PublicKey,
        sender: Arc<S>,
    ) {
        match message.deref() {
            SieveMessage::Echo(message, signature) => {
                debug!("echo message from {}", from);
                let mut signer = self.signer();

                if signer.verify(signature, &self.sender, message).is_ok()
                    && self.update_echo_set(from, signature, message).await
                    && self.check_echo_set(signature, message).await
                {
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
            SieveMessage::EchoSubscribe => {
                if let Some((message, signature)) =
                    self.echo.lock().await.deref()
                {
                    debug!("echo subscription from {}", from);
                    let message =
                        SieveMessage::Echo(message.clone(), *signature);

                    if let Err(e) = sender.send(Arc::new(message), &from).await
                    {
                        error!("failed to echo message to {}: {}", from, e);
                    }
                }

                self.echo_set.write().await.insert(from);
            }

            SieveMessage::Probabilistic(msg) => {
                debug!("processing murmur message {:?}", msg);

                let murmur_sender =
                    Arc::new(ConvertSender::new(sender.clone()));

                self.probabilistic
                    .clone()
                    .process(Arc::new(msg.clone()), from, murmur_sender)
                    .await;

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
        }
    }

    async fn output(&mut self, sender: Arc<S>) -> Self::Handle {
        let (outgoing_tx, mut outgoing_rx) = mpsc::channel(1);
        let (incoming_tx, incoming_rx) = mpsc::channel(1);
        let sampler = Sampler::new(sender.clone());
        let subscribe_sender = sender.clone();

        let murmur_sender = Arc::new(ConvertSender::new(sender));
        let handle = Arc::get_mut(&mut self.probabilistic)
            .expect("setup error")
            .output(murmur_sender)
            .await;

        debug!("sampling for echo set");

        let sample = sampler
            .sample(self.expected)
            .await
            .expect("sampling failed");

        self.echo_set.write().await.extend(sample);

        subscribe_sender
            .send_many(
                Arc::new(SieveMessage::<M>::EchoSubscribe),
                self.echo_set.read().await.iter(),
            )
            .await;

        self.handle.lock().await.replace(handle);
        self.deliverer.lock().await.replace(incoming_tx);

        let outgoing_tx = if self.sender == *self.keypair.public() {
            task::spawn(async move {
                if let Some(msg) = outgoing_rx.recv().await {
                    todo!("use murmur on {:?}", msg);
                } else {
                    error!("broadcast sender not used");
                }
            });

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
    outgoing: Option<mpsc::Sender<SieveMessage<M>>>,
    signer: Signer,
}

impl<M: Message> SieveHandle<M> {
    fn new(
        keypair: Arc<KeyPair>,
        incoming: mpsc::Receiver<M>,
        outgoing: Option<mpsc::Sender<SieveMessage<M>>>,
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

    /// Attempts delivery of a `Message` using the `Sieve` algorithm.
    /// This method returns `None` immediately if no `Message` is ready for
    /// delivery
    pub fn try_deliver(&mut self) -> Option<M> {
        self.incoming.try_recv().ok()
    }

    /// Broadcast a message using the associated `Sieve` instance. <br />
    /// This will return an error if the `Sieve` was created using
    /// `Sieve::new_receiver` or if this is not the first time this method is
    /// called
    pub async fn broadcast(&mut self, message: M) -> Result<(), SieveError> {
        let mut sender = self.outgoing.take().context(NotASender)?;
        let signature =
            self.signer.sign(&message).expect("failed to sign message");
        let message = SieveMessage::Echo(message, signature);

        sender
            .send(message)
            .await
            .map_err(|_| snafu::NoneError)
            .context(SenderDied)?;

        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;

    use super::super::probabilistic::test::murmur_message_sequence;
    use crate::test::*;

    const SIZE: usize = 50;
    const MESSAGE: usize = 0;

    macro_rules! sieve_test {
        ($message:expr, $count:expr) => {
            init_logger();

            let keypair = Arc::new(KeyPair::random());
            let message = $message;
            let sender = Arc::new(KeyPair::random());
            let (manager, signature) =
                create_sieve_manager(&sender, message.clone(), $count);

            let processor = Sieve::new_receiver(
                sender.public().clone(),
                keypair.clone(),
                SIZE / 5,
                SIZE / 3,
            );

            let mut handle = manager.run(processor).await;

            let received = handle.deliver().await.expect("deliver failed");

            let mut signer = Signer::new(keypair.deref().clone());

            assert_eq!(message, received, "wrong message delivered");
            assert!(
                signer
                    .verify(&signature, sender.public(), &received)
                    .is_ok(),
                "bad signature"
            );
        };
    }

    /// Create a `DummyManager` that will deliver the correct sequence of
    /// messages required for delivery of one sieve message
    fn create_sieve_manager<T: Message + Clone + 'static>(
        keypair: &KeyPair,
        message: T,
        peer_count: usize,
    ) -> (DummyManager<SieveMessage<T>, T>, Signature) {
        let mut signer = Signer::new(keypair.clone());
        let signature = signer.sign(&message).expect("sign failed");
        let echos = sieve_message_sequence(keypair, message, peer_count)
            .collect::<Vec<_>>();
        let keys = keyset(peer_count).collect::<Vec<_>>();
        let messages = keys
            .iter()
            .chain(keys.iter())
            .cloned()
            .zip(echos)
            .collect::<Vec<_>>();

        (DummyManager::with_key(messages, keys), signature)
    }

    pub(crate) fn sieve_message_sequence<M: Message + 'static>(
        keypair: &KeyPair,
        message: M,
        peer_count: usize,
    ) -> impl Iterator<Item = SieveMessage<M>> {
        let signature = Signer::new(keypair.clone())
            .sign(&message)
            .expect("sign failed");

        let gossip = murmur_message_sequence(
            (message.clone(), signature),
            keypair,
            peer_count,
        )
        .map(SieveMessage::Probabilistic);

        gossip.chain(
            (0..peer_count)
                .map(move |_| SieveMessage::Echo(message.clone(), signature)),
        )
    }

    #[test]
    fn sequence_generation() {
        let keypair = KeyPair::random();
        let message = 0usize;
        let count = 25;
        let messages = sieve_message_sequence(&keypair, message, count);

        assert_eq!(messages.count(), count * 2);
    }

    #[tokio::test]
    async fn deliver_vec_no_network() {
        let msg = vec![0u64, 1, 2, 3, 4, 5, 6, 7];

        sieve_test!(msg, SIZE);
    }

    #[tokio::test]
    async fn delivery_usize_no_network() {
        sieve_test!(0, SIZE);
    }

    #[tokio::test]
    async fn delivery_enum_no_network() {
        #[message]
        enum T {
            Ok,
            Error,
            Other,
        }

        let msg = T::Error;

        sieve_test!(msg, SIZE);
    }

    #[tokio::test]
    async fn broadcast_no_network() {
        init_logger();

        let keypair = Arc::new(KeyPair::random());
        let (manager, signature) =
            create_sieve_manager(&keypair, MESSAGE, SIZE);

        let processor = Sieve::new_sender(keypair.clone(), SIZE / 5, SIZE / 3);

        let mut handle = manager.run(processor).await;

        handle.broadcast(MESSAGE).await.expect("broadcast failed");

        let message = handle.deliver().await.expect("deliver failed");

        Signer::random()
            .verify(&signature, keypair.public(), &MESSAGE)
            .expect("bad signature");
        assert_eq!(message, MESSAGE, "wrong message delivered");
    }
}
