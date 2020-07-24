use std::collections::HashMap;
use std::future::Future;
use std::ops::Deref;
use std::sync::Arc;

use super::{Message, System};
use crate::broadcast::BroadcastError;

use drop::async_trait;
use drop::crypto::key::exchange::PublicKey;
use drop::net::{ConnectionRead, ConnectionWrite, ReceiveError, SendError};

use futures::future::{self, FutureExt};

use snafu::{ResultExt, Snafu};

use tokio::sync::mpsc;
use tokio::task::{self, JoinHandle};

use tracing::{debug, debug_span, error, info, warn};
use tracing_futures::Instrument;

/// Status of a `MessageProcessor` after processing one message.
pub enum ProcessorStatus {
    Continue,
    Stop,
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
    processors: Vec<Box<dyn MessageProcessor<M> + Send + Sync>>,
    reads: Vec<ConnectionRead>,
    writes: Vec<ConnectionWrite>,
}

impl<M: Message + 'static> SystemManager<M> {
    pub fn new(mut system: System) -> Self {
        let (reads, writes): (Vec<_>, Vec<_>) = system
            .connections()
            .into_iter()
            .filter_map(|connection| connection.split())
            .unzip();

        Self {
            reads,
            writes,
            processors: Vec::new(),
        }
    }

    /// Register a new `MessageProcessor` to run on this `SystemManager`.
    pub fn register<P: MessageProcessor<M> + Send + Sync + 'static>(
        &mut self,
        mut processor: P,
    ) -> ProcessorHandle<M> {
        let handle = processor.register();
        self.processors.push(Box::new(processor));

        handle
    }

    /// Start the `SystemManager`.
    /// Call this once you registered all required `MessageProcessor`s to begin
    /// processing and sending messages. This will run until all processors have
    /// failed/terminated or the future is canceled
    pub async fn run(self) {
        let mut receiver = Self::receive(self.reads).await;

        info!("system starting up");

        let (sender_handles, channels): (Vec<_>, Vec<_>) = self
            .writes
            .into_iter()
            .map(|mut x| {
                let (outgoing_tx, mut outgoing_rx) = mpsc::channel(128);
                let pkey = *x.remote_pkey();

                let handle = task::spawn(
                    async move {
                        loop {
                            let message: Option<Arc<M>> =
                                outgoing_rx.recv().await;

                            match message {
                                Some(msg) => {
                                    if let Err(e) = x.send(msg.deref()).await {
                                        error!(
                                            "error sending to {}: {}",
                                            pkey, e
                                        );
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

                (handle, (pkey, outgoing_tx))
            })
            .unzip();

        let network_out =
            channels
                .into_iter()
                .collect::<HashMap<_, mpsc::Sender<Arc<M>>, _>>();

        let (handles, mut network_in): (Vec<_>, Vec<_>) = self
            .processors
            .into_iter()
            .map(|proc| {
                let (incoming_tx, incoming_rx) = mpsc::channel(128);
                let handle = task::spawn(Self::processor_future(
                    proc,
                    incoming_rx,
                    network_out.clone(),
                ));

                (handle, incoming_tx)
            })
            .unzip();

        task::spawn(async move {
            loop {
                match receiver.recv().await {
                    Some(message) => {
                        future::join_all(
                            network_in
                                .iter_mut()
                                .map(|sender| sender.send(message.clone())),
                        )
                        .await;
                    }
                    None => {
                        warn!("no more incoming messages");
                        break;
                    }
                }
            }
        });
    }

    fn processor_future(
        mut processor: Box<dyn MessageProcessor<M> + Send + Sync>,
        mut incoming: mpsc::Receiver<(PublicKey, Arc<M>)>,
        mut senders: HashMap<PublicKey, mpsc::Sender<Arc<M>>>,
    ) -> impl Future<Output = Result<(), SendError>> + Send {
        async move {
            loop {
                match incoming.recv().await {
                    Some((pkey, message)) => {
                        debug!("receive {:?} from {}", message, pkey);

                        if processor.wants_message(&message).await {
                            match processor
                                .process(pkey, message, &mut senders)
                                .await
                            {
                                ProcessorStatus::Continue => continue,
                                ProcessorStatus::Stop => return Ok(()),
                            }
                        }
                    }
                    None => {
                        warn!("no more incoming messages, quitting");
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn receive(
        read: Vec<ConnectionRead>,
    ) -> mpsc::Receiver<(PublicKey, Arc<M>)> {
        let (mut sender, receiver) = mpsc::channel(64);

        let task = async move {
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

                        break;
                    }
                };

                if sender.send(output).await.is_err() {
                    error!("system manager not running, exiting...");
                    break;
                }
            }
            None::<usize>
        };

        task::spawn(task);

        receiver
    }
}

#[async_trait]
pub trait MessageProcessor<M: Message + 'static> {
    fn register(&mut self) -> ProcessorHandle<M>;

    /// `Process a single message using this `MessageProcessor`
    async fn process(
        &mut self,
        pkey: PublicKey,
        message: Arc<M>,
        send_channels: &mut HashMap<PublicKey, mpsc::Sender<Arc<M>>>,
    ) -> ProcessorStatus;

    /// Check if this `MessageProcessor` is interested in having this `Message`
    async fn wants_message(&self, message: &M) -> bool;
}

/// A handle given by a `MessageProcessor`.
/// This allows sending and delivering messages using the `MessageProcessor`.
pub struct ProcessorHandle<M> {
    sender: mpsc::Sender<M>,
    receiver: mpsc::Receiver<(PublicKey, M)>,
}

impl<M> ProcessorHandle<M> {
    pub(crate) fn new(
        receiver: mpsc::Receiver<(PublicKey, M)>,
        sender: mpsc::Sender<M>,
    ) -> Self {
        Self { sender, receiver }
    }

    pub async fn send(&mut self, message: M) -> Result<(), Error> {
        self.sender
            .send(message)
            .await
            .map_err(|_| snafu::NoneError)
            .context(ChannelError)
    }

    /// Deliver a message (if any) from the `MessageProcessor` with which
    /// this handle was created
    pub async fn deliver(&mut self) -> Option<(PublicKey, M)> {
        self.receiver.recv().await
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::*;

    use std::sync::atomic::{AtomicUsize, Ordering};

    struct DummyProcessor<F>
    where
        F: Fn(
                PublicKey,
                Arc<usize>,
                &mut HashMap<PublicKey, mpsc::Sender<Arc<usize>>>,
            ) -> ProcessorStatus
            + Send
            + Sync,
    {
        closure: F,
    }

    #[async_trait]
    impl<F> MessageProcessor<usize> for DummyProcessor<F>
    where
        F: Fn(
                PublicKey,
                Arc<usize>,
                &mut HashMap<PublicKey, mpsc::Sender<Arc<usize>>>,
            ) -> ProcessorStatus
            + Send
            + Sync,
    {
        fn register(&mut self) -> ProcessorHandle<usize> {
            todo!()
        }

        async fn wants_message(&self, _: &usize) -> bool {
            true
        }

        async fn process(
            &mut self,
            pkey: PublicKey,
            message: Arc<usize>,
            channels: &mut HashMap<PublicKey, mpsc::Sender<Arc<usize>>>,
        ) -> ProcessorStatus {
            (self.closure)(pkey, message, channels)
        }
    }

    #[tokio::test]
    async fn receive_from_manager() {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);

        let (pkeys, handles, system) =
            create_system(50, |mut connection| async move {
                let value = COUNTER.fetch_add(1, Ordering::AcqRel);

                connection.send(&value).await.expect("recv failed");
            })
            .await;

        let mut manager = SystemManager::new(system);

        let processor = DummyProcessor {
            closure: |pkey, message, channels| {
                debug!("processing message {:?}", message);
                ProcessorStatus::Continue
            },
        };

        let mut handle = manager.register(processor);

        for i in 0..50 {
            let message =
                handle.deliver().await.expect("unexpected end of mesages");
        }
    }
}
