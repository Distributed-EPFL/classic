use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;

use super::{Message, System};

use drop::async_trait;
use drop::crypto::key::exchange::PublicKey;
use drop::net::{Connection, ConnectionRead, ConnectionWrite};

use futures::future::{self, FutureExt};
use futures::{Stream, StreamExt};

use snafu::{OptionExt, ResultExt, Snafu};

use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task;

use tracing::{debug, debug_span, error, info, warn};
use tracing_futures::Instrument;

#[async_trait]
/// Trait used to process incoming messages from a `SystemManager`
pub trait Processor<M: Message, O: Message>: Send + Sync {
    /// The handle used to send and receive messages from the `Processor`
    type Handle;

    /// Process an incoming message using this `Processor`
    async fn process(
        self: Arc<Self>,
        message: &M,
        from: PublicKey,
        sender: Arc<Sender>,
    );

    /// Setup the `Processor` using the given sender map and returns a `Handle`
    /// for the user to use.
    async fn output(&mut self, sender: Arc<Sender>) -> Self::Handle;
}

#[derive(Snafu, Debug)]
pub enum DeliveryError {
    #[snafu(display("no more messages can be delivered"))]
    NoMoreMessage,
}

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("processor does not exist anymore"))]
    ChannelError,
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
        P: Processor<M, O, Handle = H> + 'static,
        O: Message,
        H,
    >(
        self,
        mut processor: P,
    ) -> H {
        let mut receiver = Self::receive(self.reads);

        info!("beginning system setup");

        debug!("setting up dispatcher...");

        let sender = Arc::new(Sender::new(self.writes));
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

        let handle = processor.output(sender.clone()).await;
        let processor = Arc::new(processor);

        task::spawn(async move {
            loop {
                match receiver.recv().await {
                    Some((pkey, message)) => {
                        debug!("incoming message, dispatching...");
                        let sender = sender.clone();
                        let processor = processor.clone();

                        task::spawn(async move {
                            processor.process(&message, pkey, sender).await;
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

/// A handle to send messages to other known processes
pub struct Sender {
    connections: RwLock<HashMap<PublicKey, Mutex<ConnectionWrite>>>,
}

impl Sender {
    /// Create a new `Sender` from a `Vec` of `ConnectionWrite`
    pub fn new<I: IntoIterator<Item = ConnectionWrite>>(writes: I) -> Self {
        let connections = writes
            .into_iter()
            .map(|x| (*x.remote_pkey(), Mutex::new(x)))
            .collect::<HashMap<_, _>>();

        Self {
            connections: RwLock::new(connections),
        }
    }

    /// Add a new `ConnectionWrite` to this `Sender`
    pub async fn add_connection(&self, write: ConnectionWrite) {
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

    /// Send a message to a single remote process
    pub async fn send_to<M: Message>(
        &self,
        pkey: &PublicKey,
        message: Arc<M>,
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
            .map_err(|_| snafu::NoneError)
            .context(ConnectionError)
        // FIXME: error forwarding once drop has good errors
    }

    /// Send a message to many remote processes
    pub async fn send_many<
        'a,
        I: Iterator<Item = &'a PublicKey>,
        M: Message,
    >(
        &self,
        message: Arc<M>,
        keys: I,
    ) {
        for key in keys {
            if let Err(e) = self.send_to(key, message.clone()).await {
                error!("send failure for {}: {}", key, e);
            }
        }
    }

    /// Get an `Iterator` of all known keys in this `Sender`.
    pub async fn keys(&self) -> Vec<PublicKey> {
        self.connections.read().await.keys().map(|x| *x).collect()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::*;

    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test(threaded_scheduler)]
    async fn receive_from_manager() {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        const COUNT: usize = 50;

        #[derive(Default)]
        struct Dummy {
            sender: Option<mpsc::Sender<(PublicKey, usize)>>,
        }

        #[async_trait]
        impl Processor<usize, usize> for Dummy {
            type Handle = mpsc::Receiver<(PublicKey, usize)>;

            async fn process(
                self: Arc<Self>,
                message: &usize,
                key: PublicKey,
                _sender: Arc<Sender>,
            ) {
                self.sender
                    .as_ref()
                    .expect("not setup")
                    .clone()
                    .send((key, *message))
                    .await
                    .expect("channel failure");
            }

            async fn output(&mut self, _sender: Arc<Sender>) -> Self::Handle {
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

        let processor = Dummy::default();
        let manager = SystemManager::new(system);

        debug!("manager created");

        debug!("registering processor");

        let mut handle = manager.run(processor).await;
        let mut messages = Vec::with_capacity(COUNT);

        for _ in 0..COUNT {
            let (pkey, message) =
                handle.recv().await.expect("unexpected end of mesages");

            assert!(
                pkeys.iter().any(|(key, _)| *key == pkey),
                "bad message sender"
            );

            messages.push(message);
        }

        messages.sort();

        assert_eq!(
            messages,
            (0..COUNT).collect::<Vec<_>>(),
            "incorrect message sequence"
        );

        handles.await.expect("system failure");
    }
}
