use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::{Broadcaster, Deliverer};
use crate::{Message, System};

use drop::async_trait;
use drop::crypto::key::exchange::PublicKey;
use drop::net::{
    Connection, ConnectionRead, ConnectionWrite, ReceiveError, SendError,
};

use futures::future::{self, FutureExt};
use futures::stream::{self, SelectAll, Stream, StreamExt};

use tokio::sync::mpsc;
use tokio::task;

use tracing::{debug_span, error, info, warn};
use tracing_futures::Instrument;

fn poll_peers<T>(receiver: &mut mpsc::Receiver<T>, dest: &mut Vec<T>) {
    while let Ok(reader) = receiver.try_recv() {
        dest.push(reader);
    }
}

/// A `Broadcast` implementation that does not provide any guarantee of
/// reliability.
pub struct BestEffort {}

impl BestEffort {
    /// Create a new `BestEffort` broadcast that will use the given `System`
    pub fn with<M: Message + 'static>(
        mut system: System,
    ) -> (BestEffortBroadcaster, BestEffortReceiver<M>) {
        let (readers, writers): (Vec<_>, Vec<_>) = system
            .connections()
            .drain(..)
            .filter_map(|x: Connection| x.split())
            .unzip();

        info!("creating best effort broadcast primitive");

        let mut peer_source = system.peer_source();
        let (mut read_tx, read_rx) = mpsc::channel(32);
        let (mut write_tx, write_rx) = mpsc::channel(32);

        task::spawn(async move {
            loop {
                match peer_source.next().await {
                    Some(connection) => {
                        if let Some((read, write)) = connection.split() {
                            let input = read_tx.send(read).await;
                            let output = write_tx.send(write).await;

                            match (input, output) {
                                (Ok(_), Ok(_)) => continue,
                                (Err(_), Ok(_)) => warn!("best effort broadcast receiver dropped early"),
                                (Ok(_), Err(_)) => warn!("best effort broadcast sender dropped  early"),
                                (Err(_), Err(_)) => {
                                    info!("best effort broadcast stopped");
                                    return;
                                },
                            }
                        }
                    }
                    None => {
                        warn!("no new peers can be added");
                        return;
                    }
                }
            }
        });

        let receiver = BestEffortReceiver::new(readers, read_rx);
        let broadcaster = BestEffortBroadcaster::new(writers, write_rx);

        (broadcaster, receiver)
    }
}

/// The sending end of the best effort broadcast primitive
pub struct BestEffortBroadcaster {
    connections: Vec<ConnectionWrite>,
    peer_source: mpsc::Receiver<ConnectionWrite>,
}

impl BestEffortBroadcaster {
    fn new(
        connections: Vec<ConnectionWrite>,
        peer_source: mpsc::Receiver<ConnectionWrite>,
    ) -> Self {
        Self {
            connections,
            peer_source,
        }
    }

    /// Remove all broken `Connections` from this `Broadcaster`
    pub fn purge(&mut self) {
        self.connections.retain(|_| todo!());
    }
}

#[async_trait]
impl<M: Message> Broadcaster<M> for BestEffortBroadcaster {
    /// Broadcast a `Message` using the best effort strategy.
    async fn broadcast(
        &mut self,
        message: &M,
    ) -> Option<Vec<(PublicKey, SendError)>> {
        poll_peers(&mut self.peer_source, &mut self.connections);

        if self.connections.is_empty() {
            return None;
        }

        let errors = future::join_all(self.connections.iter_mut().map(|c| {
            let pkey = *c.remote_pkey();
            let res = c
                .send(message)
                .instrument(debug_span!("peer", dest = %pkey));

            async move { (pkey, res.await) }
        }))
        .instrument(debug_span!("broadcast", message = ?message))
        .await
        .drain(..)
        .filter_map(|(pkey, res)| {
            if let Err(e) = res {
                Some((pkey, e))
            } else {
                None
            }
        })
        .collect();

        Some(errors)
    }
}

type ReceiverResult<M> = Result<(M, ConnectionRead), (PublicKey, ReceiveError)>;

/// The receiving end of the `BestEffort` broadcast primitive
pub struct BestEffortReceiver<M: Message + 'static> {
    connections: SelectAll<MessageStream<M>>,
    peer_source: mpsc::Receiver<ConnectionRead>,
}

impl<M: Message + 'static> BestEffortReceiver<M> {
    fn new<I: IntoIterator<Item = ConnectionRead>>(
        readers: I,
        peer_source: mpsc::Receiver<ConnectionRead>,
    ) -> Self {
        let connections = readers.into_iter().map(MessageStream::new);

        let connections = stream::select_all(connections);

        Self {
            connections,
            peer_source,
        }
    }
}

#[async_trait]
impl<M: Message + 'static> Deliverer<M> for BestEffortReceiver<M> {
    async fn deliver(&mut self) -> Option<(PublicKey, M)> {
        while let Ok(connection) = self.peer_source.try_recv() {
            self.connections.push(MessageStream::new(connection));
        }

        self.connections.next().await
    }
}

struct MessageStream<M: Message + 'static> {
    future: Pin<Box<dyn Future<Output = ReceiverResult<M>> + Send>>,
}

impl<M: Message + 'static> MessageStream<M> {
    fn new(connection: ConnectionRead) -> Self {
        Self {
            future: Self::future_from_read(connection),
        }
    }

    fn future_from_read(
        mut connection: ConnectionRead,
    ) -> Pin<Box<dyn Future<Output = ReceiverResult<M>> + Send>> {
        async move {
            match connection.receive::<M>().await {
                Ok(message) => Ok((message, connection)),
                Err(e) => Err((*connection.remote_pkey(), e)),
            }
        }
        .boxed()
    }
}

impl<M: Message + 'static> Stream for MessageStream<M> {
    type Item = (PublicKey, M);

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Option<(PublicKey, M)>> {
        match self.future.poll_unpin(cx) {
            Poll::Ready(Ok((message, connection))) => {
                let pkey = *connection.remote_pkey();
                self.future = Self::future_from_read(connection);
                Poll::Ready(Some((pkey, message)))
            }
            Poll::Ready(Err((pkey, err))) => {
                error!("error receiving message from {}: {}", pkey, err);
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::broadcast::Broadcaster;
    use crate::test::*;
    use crate::System;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use drop::crypto::key::exchange::Exchanger;
    use drop::net::TcpConnector;

    use tracing::debug_span;
    use tracing_futures::Instrument;

    impl Message for usize {}

    #[tokio::test]
    async fn single_shot() {
        init_logger();
        let tcp = TcpConnector::new(Exchanger::random());
        let mut addrs = test_addrs(10);
        let mut public =
            create_receivers(addrs.clone().into_iter(), |mut connection| {
                let addr = connection.peer_addr().unwrap();
                async move {
                    let data: usize =
                        connection.receive().await.expect("recv failed");
                    assert_eq!(0, data, "wrong data received");
                }
                .instrument(debug_span!("client", dest=%addr))
            })
            .await;
        let pkeys = public.iter().map(|x| x.0).collect::<Vec<_>>();
        let handles = public.drain(..).map(|x| x.1);
        let candidates = pkeys.into_iter().zip(addrs.drain(..).map(|x| x.1));

        let system: System =
            System::new_with_connector_zipped(&tcp, candidates).await;
        let (mut sender, _) = BestEffort::with::<usize>(system);

        let errors = sender.broadcast(&0usize).await;

        assert!(
            errors.expect("bcast failure").is_empty(),
            "broadcast failed"
        );

        future::join_all(handles)
            .await
            .drain(..)
            .collect::<Result<Vec<_>, _>>()
            .expect("receiver failure");
    }

    #[tokio::test]
    async fn incoming_message() {
        init_logger();
        static DATA: AtomicUsize = AtomicUsize::new(0);
        let tcp = TcpConnector::new(Exchanger::random());
        let mut addrs = test_addrs(10);
        let mut public = create_receivers(
            addrs.clone().into_iter(),
            |mut connection| async move {
                let data = DATA.fetch_add(1, Ordering::AcqRel);

                connection.send(&data).await.expect("recv failed");

                connection.close().await.expect("close failed");
            },
        )
        .await;
        let pkeys = public.iter().map(|x| x.0).collect::<Vec<_>>();
        let handles = public.drain(..).map(|x| x.1);
        let candidates = pkeys.into_iter().zip(addrs.drain(..).map(|x| x.1));

        let system: System =
            System::new_with_connector_zipped(&tcp, candidates).await;

        let (_, mut receiver) = BestEffort::with::<usize>(system);

        for _ in 0..10usize {
            let (_, data) = receiver.deliver().await.expect("early eof");
            assert!((0..10usize).contains(&data), "invalid data broadcasted");
        }

        future::join_all(handles)
            .await
            .iter()
            .for_each(|x| assert!(x.is_ok()));
    }
}
