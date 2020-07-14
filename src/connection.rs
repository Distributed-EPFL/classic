use std::fmt;
use std::marker::Send as MSend;
use std::sync::Arc;

use crate::Message;

use drop::crypto::key::exchange::PublicKey;
use drop::net::{ConnectionRead, ConnectionWrite, Connector};

use futures::{Stream, StreamExt, TryStreamExt};

use snafu::{OptionExt, Snafu};

use tokio::sync::{broadcast, mpsc, Mutex};
use tokio::task::{self, JoinError, JoinHandle};

use tracing::{debug, error};

#[derive(Snafu, Debug)]
/// Error type for `Connection` related errors
pub enum ConnectionError {
    #[snafu(display("connection was closed"))]
    ConnectionClosed,
    #[snafu(display("handshake failed"))]
    HandshakeFailed,
    #[snafu(display("failed to establish connection"))]
    Connect,
    #[snafu(display("error sending"))]
    Send,
    #[snafu(display("error receiving"))]
    Receive,
    #[snafu(display("connection handler panicked: {}", source))]
    HandlerCrash { source: JoinError },
}

/// Receiving task for `ConnectionHandle`
pub struct ConnectionReceiver<M: Message> {
    sender: broadcast::Sender<(PublicKey, M)>,
    read: ConnectionRead,
    pkey: Arc<PublicKey>,
}

impl<M> ConnectionReceiver<M>
where
    M: Message + 'static,
{
    fn run(mut self) -> JoinHandle<Self> {
        task::spawn(async move {
            loop {
                match self.read.receive().await {
                    Err(e) => {
                        error!(
                            "connection receiver error for {}: {}",
                            self.pkey, e
                        );
                        return self;
                    }
                    Ok(msg) => {
                        if self.sender.send((*self.pkey, msg)).is_err() {
                            error!(
                                "no more connection handle for {}",
                                self.pkey
                            );
                            return self;
                        }
                    }
                }
            }
        })
    }
}

pub struct ConnectionSender<M: Message> {
    pkey: Arc<PublicKey>,
    receiver: mpsc::Receiver<M>,
    write: ConnectionWrite,
}

impl<M> ConnectionSender<M>
where
    M: Message + 'static,
{
    fn run(mut self) -> JoinHandle<Self> {
        task::spawn(async move {
            loop {
                match self.receiver.recv().await {
                    Some(ref message) => {
                        if let Err(e) = self.write.send(message).await {
                            error!("failed recv for {}: {}", self.pkey, e);
                            break;
                        }
                    }
                    None => {
                        debug!("no connection handle for {}", self.pkey);
                        break;
                    }
                }
            }
            return self;
        })
    }
}

/// Handle used to receive and send messages from a shared `Connection`
pub struct ConnectionHandle<M, C, A>
where
    M: Message + 'static,
    C: Connector<Candidate = A>,
    A: MSend + Sync + fmt::Display,
{
    bcast: broadcast::Sender<(PublicKey, M)>,
    receiver: broadcast::Receiver<(PublicKey, M)>,
    sender: mpsc::Sender<M>,
    connector: Arc<C>,
    candidate: Arc<A>,
    receive_handle: SharedJoinHandle<ConnectionReceiver<M>>,
    send_handle: SharedJoinHandle<ConnectionSender<M>>,
}

impl<M, C, A> ConnectionHandle<M, C, A>
where
    M: Message + 'static,
    C: Connector<Candidate = A>,
    A: MSend + Sync + fmt::Display,
{
    /// Create a new `ConnectionHandle` to the given peer.
    pub async fn connect(
        connector: C,
        candidate: A,
        pkey: PublicKey,
    ) -> Result<Self, ConnectionError> {
        let connection =
            connector.connect(&pkey, &candidate).await.map_err(|e| {
                error!("failed to connect to {}: {}", candidate, e);
                ConnectionClosed.build()
            })?;
        let connector = Arc::new(connector);
        let candidate = Arc::new(candidate);
        let pkey = Arc::new(pkey);

        let (recv_tx, recv_rx) = broadcast::channel(32);
        let (send_tx, send_rx) = mpsc::channel(32);

        let (read, write) = connection.split().context(HandshakeFailed)?;

        let sender = ConnectionSender {
            pkey: pkey.clone(),
            receiver: send_rx,
            write,
        };

        let receiver = ConnectionReceiver {
            pkey,
            sender: recv_tx.clone(),
            read,
        };

        let send_handle = SharedJoinHandle::new(sender.run());
        let receive_handle = SharedJoinHandle::new(receiver.run());

        Ok(Self {
            bcast: recv_tx,
            receiver: recv_rx,
            sender: send_tx,
            send_handle,
            receive_handle,
            connector,
            candidate,
        })
    }

    /// Reconnect the `Connection` associated with this handler.
    /// If the `Connection` is still established this will hang until the
    /// `Connection` is broken and needs a reconnection.
    /// If the reconnection fails all handles to this `Connection` will fail.
    pub async fn reconnect(&mut self) -> Result<(), ConnectionError> {
        let receiver = self.receive_handle.try_join().await;
        let sender = self.send_handle.try_join().await;

        let (mut receiver, mut sender) = match (receiver, sender) {
            (None, None) => {
                debug!("already reconnecting");
                return Ok(());
            }
            (None, Some(_)) | (Some(_), None) => panic!("application bug"),
            (Some(Err(_)), _) | (_, Some(Err(_))) => todo!("error handling"),
            (Some(Ok(receiver)), Some(Ok(sender))) => (receiver, sender),
        };

        let connection = self
            .connector
            .connect(&receiver.pkey, &self.candidate)
            .await
            .unwrap();

        let (read, write) = connection.split().context(HandshakeFailed)?;

        receiver.read = read;
        sender.write = write;

        self.send_handle.replace(sender.run()).await;
        self.receive_handle.replace(receiver.run()).await;

        Ok(())
    }

    /// Convert this `ConnectionHandle` into a `Stream` of `Message`s
    pub fn as_stream(
        &mut self,
    ) -> impl Stream<Item = Result<M, ConnectionError>> {
        self.bcast.subscribe().into_stream().map(|x| match x {
            Ok((_, m)) => Ok(m),
            Err(_) => ConnectionClosed.fail(),
        })
    }

    /// Receive a `Message` from from this `ConnectionHandle`
    pub async fn receive(&mut self) -> Result<M, ConnectionError> {
        match self.receiver.recv().await {
            Ok((_, m)) => Ok(m),
            Err(_) => ConnectionClosed.fail(),
        }
    }

    /// Send a `Message` on this `Connection`
    pub async fn send(&mut self, message: M) -> Result<(), ConnectionError> {
        self.sender
            .send(message)
            .await
            .map_err(|_| ConnectionError::ConnectionClosed)?;

        Ok(())
    }
}

impl<M, C, A> Clone for ConnectionHandle<M, C, A>
where
    M: Message + 'static,
    C: Connector<Candidate = A>,
    A: MSend + Sync + fmt::Display,
{
    fn clone(&self) -> Self {
        Self {
            bcast: self.bcast.clone(),
            receiver: self.bcast.subscribe(),
            sender: self.sender.clone(),
            candidate: self.candidate.clone(),
            connector: self.connector.clone(),
            send_handle: self.send_handle.clone(),
            receive_handle: self.receive_handle.clone(),
        }
    }
}

struct SharedJoinHandle<R> {
    handle: Arc<Mutex<Option<JoinHandle<R>>>>,
}

impl<R> SharedJoinHandle<R> {
    fn new(handle: JoinHandle<R>) -> Self {
        Self {
            handle: Arc::new(Mutex::new(Some(handle))),
        }
    }

    /// Attempt to join the shared task.
    /// Returns `None` if the task was already joined previously.
    /// Otherwise `Some` with the result of the join is returned.
    async fn try_join(&mut self) -> Option<Result<R, JoinError>> {
        if let Some(handle) = self.handle.lock().await.take() {
            match handle.await {
                Err(e) => Some(Err(e)),
                Ok(r) => Some(Ok(r)),
            }
        } else {
            None
        }
    }

    async fn replace(&mut self, handle: JoinHandle<R>) {
        self.handle.lock().await.replace(handle);
    }
}

impl<R> Clone for SharedJoinHandle<R> {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
        }
    }
}
