use std::fmt;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::Message;

use drop::crypto::key::exchange::PublicKey;
use drop::net::{Connection, ReceiveError, SendError};

use futures::future::{self, Either};
use futures::{self, FutureExt, Stream};

use tokio::sync::{broadcast, mpsc};
use tokio::task::{self, JoinHandle};

use tracing::error;

enum Action<M: Message + 'static> {
    Send(M),
    Receive(M),
    Error(HandlerError),
    Close,
}

/// A task responsible for handling incoming and outgoing messages on a given
/// `Connection`
pub struct ConnectionHandler<M: Message> {
    rx: mpsc::Receiver<ConnectionCommand<M>>,
    tx: broadcast::Sender<M>,
    connection: Connection,
    public: PublicKey,
}

impl<M: Message + 'static> ConnectionHandler<M> {
    fn new(
        connection: Connection,
        rx: mpsc::Receiver<ConnectionCommand<M>>,
        tx: broadcast::Sender<M>,
        public: PublicKey,
    ) -> Self {
        Self {
            connection,
            rx,
            tx,
            public,
        }
    }

    /// Spawn a `Task` that will service the `Connection` associated with this
    /// handler
    pub fn serve(mut self) -> JoinHandle<Result<(), HandlerError>> {
        task::spawn(async move {
            loop {
                let send_fut = self.rx.recv().boxed();
                let recv_fut = self.connection.receive::<M>().boxed();

                match Self::poll(recv_fut, send_fut).await {
                    Some(Action::Send(message)) => {
                        self.connection.send(&message).await?;
                    }
                    Some(Action::Receive(message)) => {
                        match self.tx.send(message) {
                            Err(e) => {
                                error!(
                                    "no one waiting for messages from {}",
                                    self.public
                                );
                                return Err(e.into());
                            }
                            Ok(_) => continue,
                        }
                    }
                    Some(Action::Close) => return Ok(()),
                    Some(Action::Error(e)) => return Err(e),
                    None => continue,
                }
            }
        })
    }

    async fn poll<RF, SF>(recv_fut: RF, send_fut: SF) -> Option<Action<M>>
    where
        RF: Future<Output = Result<M, ReceiveError>> + Unpin,
        SF: Future<Output = Option<ConnectionCommand<M>>> + Unpin,
    {
        match future::select(recv_fut, send_fut).await {
            Either::Left((Ok(message), _)) => {
                println!("received {:?}", message);
                Some(Action::Receive(message))
            }
            Either::Left((Err(e), _)) => Some(Action::Error(e.into())),
            Either::Right((Some(cmd), _)) => match cmd {
                ConnectionCommand::Send(to_send) => Some(Action::Send(to_send)),
                ConnectionCommand::Close => Some(Action::Close),
                ConnectionCommand::Ping => None,
            },
            Either::Right((None, _)) => Some(Action::Error(HandlerError)),
        }
    }
}

pub struct HandlerError;

impl From<ReceiveError> for HandlerError {
    fn from(_: ReceiveError) -> Self {
        todo!()
    }
}

impl From<SendError> for HandlerError {
    fn from(_: SendError) -> Self {
        todo!()
    }
}

impl<M> From<mpsc::error::SendError<M>> for HandlerError {
    fn from(_: mpsc::error::SendError<M>) -> Self {
        todo!()
    }
}

impl<M> From<broadcast::SendError<M>> for HandlerError {
    fn from(_: broadcast::SendError<M>) -> Self {
        todo!()
    }
}

enum ConnectionCommand<M: Message> {
    Ping,
    Send(M),
    Close,
}

/// A handle to an active `Connection` used to send messages
#[derive(Clone)]
pub struct ConnectionManager<M: Message + 'static> {
    tx: mpsc::Sender<ConnectionCommand<M>>,
    tx_clone: broadcast::Sender<M>,
    public: PublicKey,
}

impl<M: Message> ConnectionManager<M> {
    pub(crate) fn new(connection: Connection, public: PublicKey) -> Self {
        let (in_tx, _) = broadcast::channel(32);
        let (out_tx, out_rx) = mpsc::channel(32);

        ConnectionHandler::new(connection, out_rx, in_tx.clone(), public)
            .serve();

        Self {
            tx: out_tx,
            tx_clone: in_tx,
            public,
        }
    }

    /// Get the `PublicKey` for the remote end of this connection
    pub fn public(&self) -> &PublicKey {
        &self.public
    }

    /// Send a message on this `Connection`
    pub async fn send(&self, message: M) -> Result<(), ()> {
        self.tx
            .clone()
            .send(ConnectionCommand::Send(message))
            .await
            .map_err(|_| ())
    }

    /// Close the `Connection` associated with this `ConnectionManager`
    pub async fn close(&mut self) -> Result<(), HandlerError> {
        self.tx
            .send(ConnectionCommand::Close)
            .await
            .map_err(|e| e.into())
    }

    /// Get the state of the associated `Connection`
    pub async fn is_connected(&mut self) -> bool {
        self.tx.send(ConnectionCommand::Ping).await.is_ok()
    }

    /// Create a `Stream` that will produce the sequence of `Message`s
    /// received on this `Connection`
    pub fn stream(&mut self) -> ConnectionStream<M> {
        ConnectionStream::new(self.public, self.tx_clone.subscribe())
    }
}

impl<M: Message> PartialEq for ConnectionManager<M> {
    fn eq(&self, other: &Self) -> bool {
        self.public == other.public
    }
}

impl<M: Message> fmt::Display for ConnectionManager<M> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.public)
    }
}

impl<M: Message> Hash for ConnectionManager<M> {
    fn hash<H: Hasher>(&self, h: &mut H) {
        self.public.hash(h)
    }
}

impl<M: Message> Eq for ConnectionManager<M> {}

/// A `Stream` that contains all `Message`s received on the associated
/// `Connection`.
pub struct ConnectionStream<M: Message> {
    public: PublicKey,
    rx: broadcast::Receiver<M>,
}

impl<M: Message> ConnectionStream<M> {
    fn new(public: PublicKey, rx: broadcast::Receiver<M>) -> Self {
        Self { public, rx }
    }
}

impl<M: Message> Stream for ConnectionStream<M> {
    type Item = (PublicKey, M);

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Option<Self::Item>> {
        match self.rx.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(v)) => Poll::Ready(Some((self.public, v))),
            Poll::Ready(Err(broadcast::RecvError::Closed)) => {
                error!("connection to {} failed", self.public);
                Poll::Ready(None)
            }
            Poll::Ready(Err(_)) => self.poll_next(cx),
        }
    }
}

impl<M: Message> PartialEq for ConnectionStream<M> {
    fn eq(&self, other: &Self) -> bool {
        self.public == other.public
    }
}

impl<M: Message> Hash for ConnectionStream<M> {
    fn hash<H: Hasher>(&self, h: &mut H) {
        self.public.hash(h)
    }
}

impl<M: Message> Eq for ConnectionStream<M> {}
