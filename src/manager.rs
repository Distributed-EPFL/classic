use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;

use super::{Message, System};

use drop::async_trait;
use drop::crypto::key::exchange::PublicKey;
use drop::net::{Connection, ConnectionRead, ConnectionWrite, SendError};

use futures::future::{self, FutureExt};
use futures::Stream;

use snafu::{ResultExt, Snafu};

use tokio::sync::mpsc;
use tokio::task::{self, JoinHandle};

use tracing::{debug, debug_span, error, info, warn};
use tracing_futures::Instrument;

/// Status of a `MessageProcessor` after processing one message.
pub enum ProcessorStatus {
    /// Indicate to the `SystemManager` that this `Processor` can process
    /// more messages
    Continue,
    /// Indicate to the `SystemManager` that this `Processor` is done processing
    /// messages
    Stop,
}

#[async_trait]
pub trait Processor<M: Message, O: Message>: Send + Sync {
    type Handle;

    async fn process(
        &self,
        message: &M,
        from: PublicKey,
        sender: Arc<Sender<Arc<M>>>,
    );

    async fn output(&mut self, sender: Arc<Sender<Arc<M>>>) -> Self::Handle;
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
    incoming: Box<dyn Stream<Item = Connection> + Send>,
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
    /// Call this once you registered all required `MessageProcessor`s to begin
    /// processing and sending messages. This will run until all processors have
    /// failed/terminated
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

        let senders = self
            .writes
            .into_iter()
            .map(|x| Self::send(x))
            .collect::<Vec<_>>();

        let network_out = senders.into_iter().collect::<HashMap<_, _>>();

        debug!("send task setup finished");

        debug!("setting up dispatcher...");

        let sender = Arc::new(Sender {
            connections: network_out,
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

    fn send(mut write: ConnectionWrite) -> (PublicKey, mpsc::Sender<Arc<M>>) {
        let (outgoing_tx, mut outgoing_rx) = mpsc::channel(128);

        let pkey = *write.remote_pkey();

        task::spawn(
            async move {
                debug!("send task started");

                loop {
                    let message: Option<Arc<M>> = outgoing_rx.recv().await;

                    match message {
                        Some(msg) => {
                            if let Err(e) = write.send(msg.deref()).await {
                                error!("error sending to {}: {}", pkey, e);
                                return Err(e);
                            }
                        }
                        None => {
                            warn!("no more messages, exiting");
                            return Ok(());
                        }
                    }
                }
            }
            .instrument(debug_span!("send_task", remote = %pkey)),
        );

        (pkey, outgoing_tx)
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

pub struct Sender<M> {
    connections: HashMap<PublicKey, mpsc::Sender<M>>,
}

impl<M> Sender<M> {
    pub fn send_to(&self, pkey: &PublicKey, message: &M) {
        todo!()
    }

    pub fn send_many(&self, message: &M, keys: Vec<PublicKey>) {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::*;

    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test(threaded_scheduler)]
    async fn receive_from_manager() {
        static DELIVERED: AtomicUsize = AtomicUsize::new(0);
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
                &self,
                message: &usize,
                key: PublicKey,
                sender: Arc<Sender<Arc<usize>>>,
            ) {
                self.sender
                    .as_ref()
                    .expect("not setup")
                    .clone()
                    .send((key, *message))
                    .await
                    .expect("channel failure");
            }

            async fn output(
                &mut self,
                sender: Arc<Sender<Arc<usize>>>,
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

        let mut processor = Dummy::default();
        let manager = SystemManager::new(system);

        debug!("manager created");

        debug!("registering processor");

        let mut handle = manager.run(processor).await;

        for _ in 0..COUNT {
            let (pkey, message) =
                handle.recv().await.expect("unexpected end of mesages");

            assert!(
                pkeys.iter().any(|(key, _)| *key == pkey),
                "bad message sender"
            );
        }

        handles.await.expect("system failure");
    }
}
