use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;

use super::sampler::Sampler;
use super::{Message, System};

use drop::async_trait;
use drop::crypto::key::exchange::PublicKey;
use drop::net::{Connection, ConnectionRead, ConnectionWrite};

use futures::future::{self, FutureExt};
use futures::{Stream, StreamExt};

use snafu::{ensure, OptionExt, ResultExt, Snafu};

use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task;

use tracing::{debug, debug_span, error, info, warn};
use tracing_futures::Instrument;

#[async_trait]
/// Trait used to process incoming messages from a `SystemManager`
pub trait Processor<M: Message + 'static, O: Message, S: Sender<M>>:
    Send + Sync
{
    /// The handle used to send and receive messages from the `Processor`
    type Handle: Handle<O>;

    /// Process an incoming message using this `Processor`
    async fn process(
        self: Arc<Self>,
        message: Arc<M>,
        from: PublicKey,
        sender: Arc<S>,
    );

    /// Setup the `Processor` using the given sender map and returns a `Handle`
    /// for the user to use.
    async fn output<SA: Sampler>(
        &mut self,
        sampler: Arc<SA>,
        sender: Arc<S>,
    ) -> Self::Handle;
}

/// An asbtract `Handle` type that allows interacting with a `Processor` once it
/// has been scheduled to run on a `SystemManager`. This type will usually be
/// obtained by calling SystemManager::run on a previously created `Processor`
#[async_trait]
pub trait Handle<M>: Send + Sync {
    /// Type of errors returned by this `Handle` type
    type Error: std::error::Error;

    /// Deliver a message using this `Handle`. This method will block until
    /// either: <br />
    /// 1. the delivery fails, then the function will return `None`
    /// 2. the delivery suceeds then the function will return `Some`(message)
    async fn deliver(&mut self) -> Result<M, Self::Error>;

    /// Poll this `Handle` for delivery, returning immediately with `None` if no
    /// message is available for delivery or `Some`
    fn try_deliver(&mut self) -> Result<Option<M>, Self::Error>;

    /// Starts broadcasting a message using this `Handle`
    async fn broadcast(&mut self, message: &M) -> Result<(), Self::Error>;
}

/// A macro to create a Handle for some `Processor` and `Message` type
#[macro_export]
macro_rules! implement_handle {
    ($name:ident, $error:ident, $msg:ident) => {
        #[derive(Snafu, Debug)]
        /// Error type for $name
        pub enum $error {
            #[snafu(display("this handle is not a sender handle"))]
            /// Not a sender handle
            NotASender,

            #[snafu(display("associated sender was destroyed"))]
            /// The sender associatex with this handle doesn't exist anymore
            SenderDied,

            #[snafu(display("this handle was already used once"))]
            /// The handle was already used to broadcast or deliver
            AlreadyUsed,

            #[snafu(display("unable to deliver a message"))]
            /// No message could be delivered from this `Handle`
            NoMessage,
        }

        /// A `Handle` used to interact with a `Processor`
        pub struct $name<M: Message> {
            incoming: Option<tokio::sync::oneshot::Receiver<M>>,
            outgoing: Option<tokio::sync::oneshot::Sender<(M, Signature)>>,
            signer: Signer,
        }

        impl<M: Message> $name<M> {
            fn new(
                keypair: Arc<KeyPair>,
                incoming: oneshot::Receiver<M>,
                outgoing: Option<oneshot::Sender<(M, Signature)>>,
            ) -> Self {
                Self {
                    signer: Signer::new(keypair.deref().clone()),
                    incoming: Some(incoming),
                    outgoing,
                }
            }
        }

        #[allow(clippy::redundant_closure_call)]
        #[async_trait]
        impl<M: Message> Handle<M> for $name<M> {
            type Error = $error;

            /// Deliver a `Message` using the algorithm associated with this
            /// `$name`. Since this is a one-shot algorithm, a `$name` can only
            /// deliver one message.
            /// All subsequent calls to this method will return `None`
            async fn deliver(&mut self) -> Result<M, Self::Error> {
                self.incoming
                    .take()
                    .context(AlreadyUsed)?
                    .await
                    .map_err(|_| snafu::NoneError)
                    .context(NoMessage)
            }

            /// Attempts delivery of a `Message` using the `Sieve` algorithm.
            /// This method returns `Ok(None)` immediately if no `Message` is
            /// ready for delivery. `Ok(Some(message))` if a message is ready.
            /// And finally `Err` if no message can be delivered using this
            /// handle
            fn try_deliver(&mut self) -> Result<Option<M>, Self::Error> {
                let mut deliver = self.incoming.take().context(AlreadyUsed)?;

                match deliver.try_recv() {
                    Ok(message) => Ok(Some(message)),
                    Err(oneshot::error::TryRecvError::Empty) => {
                        self.incoming.replace(deliver);
                        Ok(None)
                    }
                    _ => NoMessage.fail(),
                }
            }

            /// Broadcast a message using the associated broadcast instance. <br />
            /// This will return an error if the instance was created using
            /// `new_receiver` or if this is not the first time this method is
            /// called
            async fn broadcast(
                &mut self,
                message: &M,
            ) -> Result<(), Self::Error> {
                let sender = self.outgoing.take().context(NotASender)?;
                let signature =
                    self.signer.sign(message).expect("failed to sign message");

                sender
                    .send((message.clone(), signature))
                    .map_err(|_| snafu::NoneError)
                    .context(SenderDied)?;

                Ok(())
            }
        }
    };
}

/// Handles sending and receiving messages from all known peers.
/// Also forwards them to relevant destination for processing
pub struct SystemManager<M: Message + 'static> {
    _m: PhantomData<M>,
    reads: Vec<ConnectionRead>,
    writes: Vec<ConnectionWrite>,
    /// `Stream` of incoming `Connection`s
    incoming: Box<dyn Stream<Item = Connection> + Send + Unpin>,
}

impl<M: Message + 'static> SystemManager<M> {
    /// Create a new `SystemManager` using some previously created `System`
    pub fn new(mut system: System) -> Self {
        debug!("creating manager");

        let (reads, writes): (Vec<_>, Vec<_>) = system
            .connections()
            .into_iter()
            .filter_map(|connection| connection.split())
            .unzip();

        let incoming = Box::new(system.peer_source());

        Self {
            reads,
            writes,
            incoming,
            _m: PhantomData,
        }
    }

    /// Start the `SystemManager`. <br />
    /// Call this after you registered one `Processor`s to begin processing and
    /// sending messages. This will return a `Handle` that allows interaction
    /// with the `Processor`.
    pub async fn run<
        S: Sampler,
        P: Processor<M, O, NetworkSender<M>, Handle = H> + 'static,
        O: Message,
        H: Handle<O>,
    >(
        self,
        mut processor: P,
        sampler: S,
    ) -> H {
        let mut receiver = Self::receive(self.reads);

        info!("beginning system setup");

        debug!("setting up dispatcher...");

        let sampler = Arc::new(sampler);
        let sender = Arc::new(NetworkSender::new(self.writes));
        let sender_add = sender.clone();
        let mut incoming = self.incoming;

        task::spawn(async move {
            while let Some(connection) = incoming.next().await {
                // TODO: send the read end of the connection to receive task
                if let Some((_, write)) = connection.split() {
                    debug!(
                        "new incoming connection from {}",
                        write.remote_pkey()
                    );
                    sender_add.add_connection(write).await;
                }
            }
        });

        let handle = processor.output(sampler, sender.clone()).await;
        let processor = Arc::new(processor);

        task::spawn(async move {
            loop {
                match receiver.recv().await {
                    Some((pkey, message)) => {
                        debug!("incoming message, dispatching...");
                        let sender = sender.clone();
                        let processor = processor.clone();

                        task::spawn(async move {
                            processor.process(message, pkey, sender).await;
                        });
                    }
                    None => {
                        warn!("no more incoming messages, dispatcher exiting");
                        break;
                    }
                }
            }
        });

        debug!("done setting up dispatcher! system now running");

        handle
    }

    fn receive(
        read: Vec<ConnectionRead>,
    ) -> mpsc::Receiver<(PublicKey, Arc<M>)> {
        let (mut sender, receiver) = mpsc::channel(64);

        let task = async move {
            debug!("receive task started for {} conections", read.len());

            let mut future =
                Some(future::select_all(read.into_iter().map(|mut x| {
                    async move {
                        let m: Result<M, _> = x.receive().await;

                        (m, x)
                    }
                    .boxed()
                })));
            let mut next = None;

            loop {
                let (res, _, mut others) = if let Some(initial) = future.take()
                {
                    initial
                } else {
                    next.take()?
                }
                .await;

                let output = match res {
                    (Ok(msg), mut connection) => {
                        let message = Arc::new(msg);
                        let pkey = *connection.remote_pkey();

                        debug!("received message {:?} from {}", message, pkey);

                        others.push(
                            async move {
                                let message = connection.receive().await;

                                (message, connection)
                            }
                            .boxed(),
                        );

                        next = Some(future::select_all(others));

                        (pkey, message)
                    }
                    (Err(e), connection) => {
                        error!(
                            "connection broken with {}, {}",
                            e,
                            connection.remote_pkey()
                        );

                        if others.is_empty() {
                            warn!("no more active connections, exiting task");
                            break;
                        } else {
                            debug!("still {} connections alive", others.len());
                            next = Some(future::select_all(others));
                            continue;
                        }
                    }
                };

                if sender.send(output).await.is_err() {
                    error!("system manager not running, exiting...");
                    break;
                }
            }
            None::<usize>
        }
        .instrument(debug_span!("manager_receive"));

        task::spawn(task);

        receiver
    }
}

#[derive(Debug, Snafu)]
/// Error returned by `Sender` when attempting to send `Message`s
pub enum SenderError {
    #[snafu(display("peer is unknown"))]
    /// The destination `PublicKey` was not known by this `Sender`
    NoSuchPeer,
    #[snafu(display("connection is broken"))]
    /// The `Connection` encountered an error while sending
    ConnectionError,
}

#[async_trait]
/// Trait used when sending messages out from `Processor`s.
pub trait Sender<M: Message + 'static>: Send + Sync {
    /// Add a new `ConnectionWrite` to this `Sender`
    async fn add_connection(&self, write: ConnectionWrite);

    /// Get the keys of  peers known by this `Sender`
    async fn keys(&self) -> Vec<PublicKey>;

    /// Send a message to a given peer using this `Sender`
    async fn send(
        &self,
        message: Arc<M>,
        pkey: &PublicKey,
    ) -> Result<(), SenderError>;

    /// Send the same message to many different peers.
    async fn send_many<'a, I: Iterator<Item = &'a PublicKey> + Send>(
        self: Arc<Self>,
        message: Arc<M>,
        keys: I,
    ) -> Option<Vec<SenderError>> {
        let result = future::join_all(keys.map(|x| {
            let message = message.clone();
            let nself = self.clone();
            async move { nself.send(message, &x).await }
        }))
        .await;

        let result = result
            .into_iter()
            .filter_map(|x| x.err())
            .collect::<Vec<_>>();

        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }
}

/// A handle to send messages to other known processes
pub struct NetworkSender<M: Message> {
    connections: RwLock<HashMap<PublicKey, Mutex<ConnectionWrite>>>,
    _m: PhantomData<M>,
}

impl<M: Message> NetworkSender<M> {
    /// Create a new `Sender` from a `Vec` of `ConnectionWrite`
    pub fn new<I: IntoIterator<Item = ConnectionWrite>>(writes: I) -> Self {
        let connections = writes
            .into_iter()
            .map(|x| (*x.remote_pkey(), Mutex::new(x)))
            .collect::<HashMap<_, _>>();

        Self {
            connections: RwLock::new(connections),
            _m: PhantomData,
        }
    }

    /// Get an `Iterator` of all known keys in this `Sender`.
    pub async fn keys(&self) -> Vec<PublicKey> {
        self.connections.read().await.keys().copied().collect()
    }
}

#[async_trait]
impl<M: Message + 'static> Sender<M> for NetworkSender<M> {
    async fn send(
        &self,
        message: Arc<M>,
        pkey: &PublicKey,
    ) -> Result<(), SenderError> {
        self.connections
            .read()
            .await
            .get(pkey)
            .context(NoSuchPeer)?
            .lock()
            .await
            .send(message.deref())
            .await
            .map_err(|_| snafu::NoneError) // FIXME: once drop has erros merged
            .context(ConnectionError)
    }

    /// Add a new `ConnectionWrite` to this `Sender`
    async fn add_connection(&self, write: ConnectionWrite) {
        if let Some(conn) = self
            .connections
            .write()
            .await
            .insert(*write.remote_pkey(), Mutex::new(write))
        {
            let pkey = *conn.lock().await.remote_pkey();
            warn!("replaced connection to {}, messages may be dropped", pkey);
        }
    }

    async fn keys(&self) -> Vec<PublicKey> {
        self.connections
            .read()
            .await
            .iter()
            .map(|(key, _)| *key)
            .collect()
    }
}

/// A `Sender` that uses an input messages type I and implements an output `Sender`
/// using the `Into` trait
pub struct ConvertSender<I, O, S>
where
    I: Message + 'static + Into<O>,
    O: Message + 'static,
    S: Sender<O>,
{
    sender: Arc<S>,
    _i: PhantomData<I>,
    _o: PhantomData<O>,
}

impl<I, O, S> ConvertSender<I, O, S>
where
    I: Message + 'static + Into<O>,
    O: Message + 'static,
    S: Sender<O>,
{
    /// Create a new `ConvertSender` using the provided underlying `Sender`
    /// to actually send messages
    pub fn new(sender: Arc<S>) -> Self {
        Self {
            sender,
            _i: PhantomData,
            _o: PhantomData,
        }
    }
}

#[async_trait]
impl<I, O, S> Sender<I> for ConvertSender<I, O, S>
where
    I: Message + 'static + Into<O>,
    O: Message + 'static,
    S: Sender<O>,
{
    async fn send(
        &self,
        message: Arc<I>,
        to: &PublicKey,
    ) -> Result<(), SenderError> {
        let message = message.deref().clone().into();

        self.sender.send(Arc::new(message), to).await
    }

    async fn keys(&self) -> Vec<PublicKey> {
        self.sender.keys().await
    }

    async fn add_connection(&self, write: ConnectionWrite) {
        self.sender.add_connection(write).await
    }
}

/// A `Sender` that can be use to transform messages before passing them to
/// an underlying `Sneder`.
pub struct WrappingSender<F, I, O, S>
where
    I: Message + 'static,
    O: Message + 'static,
    S: Sender<O>,
    F: Fn(&I) -> Result<O, SenderError>,
{
    sender: Arc<S>,
    closure: F,
    _i: PhantomData<I>,
    _o: PhantomData<O>,
}

impl<F, I, O, S> WrappingSender<F, I, O, S>
where
    I: Message + 'static,
    O: Message + 'static,
    S: Sender<O>,
    F: Fn(&I) -> Result<O, SenderError> + Send + Sync,
{
    /// Create a new `WrappingSender` that will pass each message through
    /// the specified closure before passing it on to the underlying `Sender`
    pub fn new(sender: Arc<S>, closure: F) -> Self {
        Self {
            closure,
            sender,
            _i: PhantomData,
            _o: PhantomData,
        }
    }
}

#[async_trait]
impl<F, I, O, S> Sender<I> for WrappingSender<F, I, O, S>
where
    I: Message + 'static,
    O: Message + 'static,
    S: Sender<O>,
    F: Fn(&I) -> Result<O, SenderError> + Send + Sync,
{
    async fn send(
        &self,
        message: Arc<I>,
        to: &PublicKey,
    ) -> Result<(), SenderError> {
        let new = (self.closure)(message.deref())
            .map_err(|_| snafu::NoneError)
            .context(ConnectionError)?;
        self.sender.send(Arc::new(new), to).await
    }

    async fn add_connection(&self, write: ConnectionWrite) {
        self.sender.add_connection(write).await
    }

    async fn keys(&self) -> Vec<PublicKey> {
        self.sender.keys().await
    }
}

/// A `Sender` that only collects messages instead of sending them
pub struct CollectingSender<M: Message> {
    messages: Mutex<Vec<(PublicKey, Arc<M>)>>,
    keys: Mutex<HashSet<PublicKey>>,
}

impl<M: Message> CollectingSender<M> {
    /// Create a new `CollectingSender` using a specified set of `PublicKey`
    /// destinations
    pub fn new<I: IntoIterator<Item = PublicKey>>(keys: I) -> Self {
        Self {
            messages: Mutex::new(Vec::new()),
            keys: Mutex::new(keys.into_iter().collect()),
        }
    }

    /// Retrieve the set of messages that was sent using this `CollectingSender`
    pub async fn messages(&self) -> Vec<(PublicKey, Arc<M>)> {
        self.messages.lock().await.iter().cloned().collect()
    }
}

#[async_trait]
impl<M: Message + 'static> Sender<M> for CollectingSender<M> {
    async fn send(
        &self,
        message: Arc<M>,
        key: &PublicKey,
    ) -> Result<(), SenderError> {
        ensure!(self.keys.lock().await.contains(key), NoSuchPeer);

        self.messages.lock().await.push((*key, message));

        Ok(())
    }

    async fn add_connection(&self, write: ConnectionWrite) {
        self.keys.lock().await.insert(*write.remote_pkey());
    }

    async fn keys(&self) -> Vec<PublicKey> {
        self.keys.lock().await.clone().iter().copied().collect()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::*;
    use crate::AllSampler;

    use std::sync::atomic::{AtomicUsize, Ordering};

    #[async_trait]
    impl<M: Message> Handle<(PublicKey, M)> for mpsc::Receiver<(PublicKey, M)> {
        type Error = mpsc::error::RecvError;

        async fn deliver(&mut self) -> Result<(PublicKey, M), Self::Error> {
            Ok(self.recv().await.expect("no message"))
        }

        fn try_deliver(
            &mut self,
        ) -> Result<Option<(PublicKey, M)>, Self::Error> {
            unreachable!()
        }

        async fn broadcast(
            &mut self,
            _: &(PublicKey, M),
        ) -> Result<(), Self::Error> {
            unreachable!()
        }
    }

    impl Message for PublicKey {}
    impl<M: Message> Message for (PublicKey, M) {}

    #[tokio::test(threaded_scheduler)]
    async fn receive_from_manager() {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        const COUNT: usize = 50;

        #[derive(Default)]
        struct Dummy {
            sender: Option<mpsc::Sender<(PublicKey, usize)>>,
        }

        #[async_trait]
        impl Processor<usize, (PublicKey, usize), NetworkSender<usize>> for Dummy {
            type Handle = mpsc::Receiver<(PublicKey, usize)>;

            async fn process(
                self: Arc<Self>,
                message: Arc<usize>,
                key: PublicKey,
                _sender: Arc<NetworkSender<usize>>,
            ) {
                self.sender
                    .as_ref()
                    .expect("not setup")
                    .clone()
                    .send((key, *message))
                    .await
                    .expect("channel failure");
            }

            async fn output<SA: Sampler>(
                &mut self,
                _sampler: Arc<SA>,
                _sender: Arc<NetworkSender<usize>>,
            ) -> Self::Handle {
                let (tx, rx) = mpsc::channel(128);

                self.sender.replace(tx);

                rx
            }
        }

        let (pkeys, handles, system) =
            create_system(COUNT, |mut connection| async move {
                let value = COUNTER.fetch_add(1, Ordering::AcqRel);

                connection.send(&value).await.expect("recv failed");
            })
            .await;

        let sampler = AllSampler::default();
        let processor = Dummy::default();
        let manager = SystemManager::new(system);

        debug!("manager created");

        debug!("registering processor");

        let mut handle = manager.run(processor, sampler).await;
        let mut messages = Vec::with_capacity(COUNT);

        for _ in 0..COUNT {
            let (pkey, message) =
                handle.deliver().await.expect("unexpected error");

            assert!(
                pkeys.iter().any(|(key, _)| *key == pkey),
                "bad message sender"
            );

            messages.push(message);
        }

        messages.sort_unstable();

        assert_eq!(
            messages,
            (0..COUNT).collect::<Vec<_>>(),
            "incorrect message sequence"
        );

        handles.await.expect("system failure");
    }
}
