use std::fmt;
use std::future::Future;
use std::marker::Send;
use std::sync::Arc;

use crate::Message;

use drop::crypto::key::exchange::PublicKey;
use drop::net::{
    Connection, ConnectionRead, ConnectionWrite, Connector, Listener,
};

use futures::{Stream, StreamExt, TryStreamExt};

use snafu::{OptionExt, ResultExt, Snafu};

use tokio::sync::{broadcast, mpsc, Mutex};
use tokio::task::{self, JoinError, JoinHandle};

use tracing::{debug, debug_span, error, info, warn};
use tracing_futures::Instrument;

#[derive(Snafu, Debug, Clone, Eq, PartialEq)]
/// Error type for `Connection` related errors
pub enum ConnectionError {
    /// An error type returned when trying to send/receive on a closed
    /// connection
    #[snafu(display("connection was closed"))]
    ConnectionClosed,
    #[snafu(display("handshake failed"))]
    /// An error returned when attempting to create a handle with an
    /// unauthenticated connection
    HandshakeFailed,
    #[snafu(display("lost {} messages", count))]
    /// An error returned when some number of messages were lost due to
    /// too slow processing
    Lagged {
        /// The number of messages that were lost
        count: u64,
    },
    #[snafu(display("failed to establish connection"))]
    /// Error returned when the connection failed to be established
    Connect,
    #[snafu(display("error sending"))]
    /// Error when sending a message fails
    SendError,
    #[snafu(display("error receiving"))]
    /// Error when receiving a message fails
    Receive,
    #[snafu(display("connection handler panicked"))]
    /// Error when the connection handler panicked
    HandlerCrash,
    #[snafu(display("read wasn't ready and should be retried"))]
    /// Error returned when a send or receive should be attempted again
    Again,
    #[snafu(display("connection was broken and needs a reconnect"))]
    /// Error returned when a connection has been broken and needs to be
    /// re-established
    Reconnect,
    #[snafu(display("fatal connection error"))]
    /// Error returned when a connection encounters a fatal error
    Fatal,
}

/// Receiving task for `ConnectionHandle`
struct ConnectionReceiver<M: Message> {
    sender: broadcast::Sender<Result<Arc<M>, ConnectionError>>,
    notify: mpsc::Sender<()>,
    read: ConnectionRead,
}

impl<M> ConnectionReceiver<M>
where
    M: Message + 'static,
{
    fn task(mut self) -> impl Future<Output = Self> {
        async move {
            loop {
                match self.read.receive().await {
                    Err(e) => {
                        error!("connection receiver error: {}", e);
                        let _ = self.notify.send(()).await;
                        if self.sender.send(Reconnect.fail()).is_err() {
                            warn!("no more connection handle");
                        }
                        return self;
                    }
                    Ok(msg) => {
                        let msg = Arc::new(msg);

                        if self.sender.send(Ok(msg)).is_err() {
                            error!("no more connection handle",);
                            return self;
                        }
                    }
                }
            }
        }
    }
}

struct ConnectionSender<M: Message> {
    receiver: mpsc::Receiver<M>,
    notify: mpsc::Receiver<()>,
    write: ConnectionWrite,
}

impl<M> ConnectionSender<M>
where
    M: Message + 'static,
{
    fn task(mut self) -> impl Future<Output = Self> {
        async move {
            loop {
                tokio::select! {
                    result = self.receiver.recv() =>  {
                        match result {
                            Some(ref message) => {
                                if let Err(e) = self.write.send(message).await {
                                    error!("writing message failed: {}", e);
                                    break;
                                }
                            }
                            None => {
                                break;
                            }
                        }
                    }

                    _ = self.notify.recv() => {
                        debug!("stopping send loop after receive error");
                        break;
                    }
                }
            }

            debug!("connection sender ending");

            return self;
        }
    }
}

/// Handle used to receive and send messages from a shared `Connection`
pub struct ConnectionHandle<M, C, A>
where
    M: Message + 'static,
    C: Connector<Candidate = A>,
    A: Send + Sync + fmt::Display,
{
    bcast: broadcast::Sender<Result<Arc<M>, ConnectionError>>,
    receiver: broadcast::Receiver<Result<Arc<M>, ConnectionError>>,
    sender: mpsc::Sender<M>,
    pkey: Arc<PublicKey>,
    connector: Option<Arc<C>>,
    candidate: Option<Arc<A>>,
    receive_handle: SharedJoinHandle<ConnectionReceiver<M>>,
    send_handle: SharedJoinHandle<ConnectionSender<M>>,
}

impl<M, C, A> ConnectionHandle<M, C, A>
where
    M: Message + 'static,
    C: Connector<Candidate = A>,
    A: Send + Sync + fmt::Display,
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

        Self::setup(connection, pkey, Some(connector), Some(candidate))
    }

    /// Create a new `ConnectionHandle` for an incoming `Connection` using the
    /// provided `Listener`
    pub async fn accept<L: Listener<Candidate = A>>(
        listener: &mut L,
    ) -> Result<Self, ConnectionError> {
        let connection = listener.accept().await.map_err(|e| {
            error!("failed to accept connection: {}", e);
            Connect.build()
        })?;

        let pkey = connection.remote_key().context(HandshakeFailed)?;

        Self::setup(connection, pkey, None, None)
    }

    fn setup(
        connection: Connection,
        pkey: PublicKey,
        connector: Option<Arc<C>>,
        candidate: Option<Arc<A>>,
    ) -> Result<Self, ConnectionError> {
        let pkey = Arc::new(pkey);

        let (recv_tx, recv_rx) = broadcast::channel(32);
        let (send_tx, send_rx) = mpsc::channel(32);

        let (read, write) = connection.split().context(HandshakeFailed)?;

        let (stop_tx, stop_rx) = mpsc::channel(1);

        let sender = ConnectionSender {
            receiver: send_rx,
            notify: stop_rx,
            write,
        };

        let receiver = ConnectionReceiver {
            sender: recv_tx.clone(),
            notify: stop_tx,
            read,
        };

        let send_handle = SharedJoinHandle::new(task::spawn(
            sender
                .task()
                .instrument(debug_span!("sender", remote = %pkey)),
        ));
        let receive_handle = SharedJoinHandle::new(task::spawn(
            receiver
                .task()
                .instrument(debug_span!("receiver", remote = %pkey)),
        ));

        Ok(Self {
            bcast: recv_tx,
            receiver: recv_rx,
            sender: send_tx,
            pkey,
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
        match (self.connector.as_ref(), self.candidate.as_ref()) {
            (Some(_), Some(_)) => self.reconnect_internal().await,
            (_, _) => {
                error!("can't reconnect an incoming connection");
                ConnectionClosed.fail()
            }
        }
    }

    async fn reconnect_internal(&mut self) -> Result<(), ConnectionError> {
        let connector = self.connector.as_ref().unwrap();
        let candidate = self.candidate.as_ref().unwrap();

        info!("attempting reconnection to {}", candidate);

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

        debug!("handler are finished, reconnecting to {}", candidate);

        let connection = connector
            .connect(&self.pkey, &candidate)
            .await
            .map_err(|_| {
                error!("error reconnecting to {}", candidate);
                snafu::NoneError
            })
            .context(Connect)?;

        let (read, write) = connection.split().context(HandshakeFailed)?;

        receiver.read = read;
        sender.write = write;

        let send_handle = task::spawn(
            sender
                .task()
                .instrument(debug_span!("sender", remote = %self.pkey)),
        );

        let receive_handle = task::spawn(
            receiver
                .task()
                .instrument(debug_span!("receiver", remote = %self.pkey)),
        );

        self.send_handle.replace(send_handle).await;
        self.receive_handle.replace(receive_handle).await;

        Ok(())
    }

    /// Convert this `ConnectionHandle` into a `Stream` of `Message`s
    pub fn into_stream(
        self,
    ) -> impl Stream<Item = Result<Arc<M>, ConnectionError>> {
        self.bcast.subscribe().into_stream().map(|x| match x {
            Ok(m) => m,
            Err(broadcast::RecvError::Lagged(count)) => {
                warn!("running too slow, {} messages were lost", count);
                Lagged { count }.fail()
            }
            Err(_) => ConnectionClosed.fail(),
        })
    }

    /// Get the `PublicKey` associated with this `ConnectionHandle`
    pub fn public_key(&self) -> &PublicKey {
        &self.pkey
    }

    /// Receive a `Message` from from this `ConnectionHandle`
    pub async fn receive(&mut self) -> Result<Arc<M>, ConnectionError> {
        match self.receiver.recv().await {
            Ok(msg) => msg,
            Err(e) => {
                error!("handler error: {}", e);
                Fatal.fail()
            }
        }
    }

    /// Send a `Message` on this `Connection`
    pub async fn send(&mut self, message: M) -> Result<(), ConnectionError> {
        debug!("sending {:?}", message);

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
    A: Send + Sync + fmt::Display,
{
    fn clone(&self) -> Self {
        debug!("created new handle");

        Self {
            pkey: self.pkey.clone(),
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

#[cfg(test)]
mod test {
    use super::*;

    use crate::test::*;

    use std::net::{Ipv4Addr, SocketAddr};

    use drop::crypto::key::exchange::Exchanger;
    use drop::net::{Listener, TcpConnector, TcpListener};

    use futures::future;

    use tokio::sync::oneshot;

    async fn test_setup() -> (SocketAddr, PublicKey, Exchanger, TcpListener) {
        init_logger();

        let addr: SocketAddr = (Ipv4Addr::LOCALHOST, 0).into();
        let server_keypair = Exchanger::random();
        let server_key = *server_keypair.keypair().public();
        let client_keypair = Exchanger::random();

        let listener = TcpListener::new(addr, server_keypair)
            .await
            .expect("listen failed");
        let addr = listener.local_addr().expect("no address");

        (addr, server_key, client_keypair, listener)
    }

    #[tokio::test]
    async fn simple_message() {
        let (addr, server_key, client_keypair, mut listener) =
            test_setup().await;

        let t = task::spawn(async move {
            let mut connection =
                listener.accept().await.expect("failed to accept");

            let v = connection.receive::<usize>().await.expect("recv failed");

            assert_eq!(v, 0, "wrong value received");
        });

        let mut handle = ConnectionHandle::connect(
            TcpConnector::new(client_keypair),
            addr,
            server_key,
        )
        .await
        .expect("failed to connect");

        handle.send(0usize).await.expect("send failed");

        t.await.expect("panic error");
    }

    #[tokio::test]
    async fn cloned_handle() {
        let (addr, server_key, client_keypair, mut listener) =
            test_setup().await;

        let (start_tx, start_rx) = oneshot::channel();

        let handle = task::spawn(async move {
            let mut connection =
                listener.accept().await.expect("accept failed");

            start_rx.await.expect("start notification failed");

            connection.send(&0usize).await.expect("failed to send");
        });

        let connection = ConnectionHandle::<usize, _, _>::connect(
            TcpConnector::new(client_keypair),
            addr,
            server_key,
        )
        .await
        .expect("failed to connect");

        let connections =
            (0usize..10).map(|_| connection.clone()).collect::<Vec<_>>();

        start_tx.send(()).expect("start failed");

        future::join_all(connections.into_iter().map(|mut c| async move {
            c.receive().await.expect("recv failed")
        }))
        .await
        .into_iter()
        .for_each(|m| assert_eq!(*m, 0usize, "wrong value received"));

        handle.await.expect("task panicked");
    }

    #[tokio::test]
    async fn closed_connection_receive() {
        let (addr, server_key, client_keypair, mut listener) =
            test_setup().await;

        let handle = task::spawn(async move {
            listener.accept().await.expect("accept failed");
        });

        let mut connection = ConnectionHandle::<usize, _, _>::connect(
            TcpConnector::new(client_keypair),
            addr,
            server_key,
        )
        .await
        .expect("failed to connect");

        connection
            .receive()
            .await
            .expect_err("no error occurred on closed connection");

        handle.await.expect("listener panicked");
    }

    #[tokio::test]
    async fn reconnect() {
        let (addr, server_key, client_keypair, mut listener) =
            test_setup().await;

        use tracing::info;

        let handle = task::spawn(async move {
            let mut connection =
                listener.accept().await.expect("accept failed");

            info!("accepted first connection");

            connection.close().await.expect("close failed");

            info!("closed first connection");

            let mut connection =
                listener.accept().await.expect("accept failed");

            info!("accepted second connection");

            connection.send(&0usize).await.expect("send failed");
        });

        let mut connection = ConnectionHandle::<usize, _, _>::connect(
            TcpConnector::new(client_keypair),
            addr,
            server_key,
        )
        .await
        .expect("connect failed");

        let result = connection.receive().await;

        assert!(result.is_err(), "success despite disconnect");
        assert_eq!(
            result.unwrap_err(),
            ConnectionError::Reconnect,
            "wrong error type returned"
        );

        connection.reconnect().await.expect("reconnect failed");

        assert_eq!(
            *connection.receive().await.expect("recv failed"),
            0usize,
            "wrong value received"
        );

        handle.await.expect("panic error");
    }
}
