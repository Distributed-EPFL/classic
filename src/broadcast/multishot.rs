use std::collections::HashSet;
use std::future::Future;

use super::besteffort::BestEffort;
use super::*;
use crate::{Connection as ConnectionManager, System};

use drop::async_trait;

use futures::future::{self, Either, FutureExt};
use futures::stream::{Stream, StreamExt};

use tokio::task::{self, JoinHandle};

use tracing::{debug, info};

/// An implementation of `Broadcast` that provides the following guarantees:
/// * a `Message` is delivered exactly once
/// * a `Message` is delivered by every correct process or no process at all
pub struct ReliableMultiShot<M: Message + 'static> {
    beb: BestEffort<M>,
    _handle: JoinHandle<()>,
}

impl<M: Message> ReliableMultiShot<M> {
    /// Create a new `ReliableMultiShot` that will use the given
    /// `System` to broadcast `Message`s
    pub async fn new(system: System<M>) -> Self {
        let mut beb = BestEffort::from(system);
        let incoming = beb.incoming().await;

        let retransmitter = Rebroadcaster::new(beb.system(), incoming).await;
        let handle = task::spawn(async move { retransmitter.start().await });

        Self {
            beb,
            _handle: handle,
        }
    }
}

#[async_trait]
impl<M: Message + Unpin + 'static> Broadcast<M> for ReliableMultiShot<M> {
    async fn broadcast(&mut self, message: &M) -> Result<(), ()> {
        self.beb.broadcast(message).await
    }

    async fn incoming(
        &mut self,
    ) -> Box<dyn Stream<Item = (PublicKey, M)> + Send + Unpin> {
        // FIXME: fix multiple delivery of messages
        self.beb.incoming().await
    }
}

/// A `Stream` that produces values received from the given `System` and
/// retransmits them to ensure reliable broadcast.
pub struct Rebroadcaster<M, S>
where
    M: Message + 'static,
    S: Stream<Item = (PublicKey, M)> + Send + 'static,
{
    incoming: S,
    peer_source: Pin<Box<dyn Stream<Item = ConnectionManager<M>> + Send>>,
    delivered: HashSet<M>,
    outgoing: HashSet<ConnectionManager<M>>,
}

impl<M, S> Rebroadcaster<M, S>
where
    M: Message + 'static,
    S: Stream<Item = (PublicKey, M)> + Send + Unpin + 'static,
{
    async fn new(system: &mut System<M>, incoming: S) -> Self {
        let peer_source =
            Box::pin(system.peer_source().filter_map(|x| async { x.ok() }));
        let outgoing = system.connections().await.drain(..).collect();

        Self {
            peer_source,
            delivered: HashSet::new(),
            incoming,
            outgoing,
        }
    }

    async fn broadcast(&mut self, source: &PublicKey, message: &M) {
        if self.delivered.insert(*message) {
            debug!("retransmitting {:?}", message);
            future::join_all(self.outgoing.iter().filter_map(|x| {
                if x.public() == source {
                    None
                } else {
                    Some(x.send(*message))
                }
            }))
            .await;
        } else {
            debug!("delivery of old message {:?}", message);
        }
    }

    async fn start(mut self) {
        loop {
            match Self::poll(
                self.incoming.next(),
                self.peer_source.next().boxed(),
            )
            .await
            {
                Action::Insert(connection) => {
                    info!("new connection {}", connection);
                    self.outgoing.insert(connection);
                }
                Action::Broadcast(ref pkey, ref message) => {
                    self.broadcast(pkey, message).await;
                }
                Action::Stop => return,
            }
        }
    }

    async fn poll<PF, RF>(receive: RF, peer: PF) -> Action<M>
    where
        PF: Future<Output = Option<ConnectionManager<M>>> + Unpin,
        RF: Future<Output = Option<(PublicKey, M)>> + Unpin,
    {
        match future::select(receive, peer).await {
            Either::Right((Some(connection), _)) => Action::Insert(connection),
            Either::Right((None, _)) => todo!(),
            Either::Left((Some((pkey, message)), _)) => {
                Action::Broadcast(pkey, message)
            }
            Either::Left((None, _)) => Action::Stop,
        }
    }
}

enum Action<M: Message + 'static> {
    Insert(ConnectionManager<M>),
    Broadcast(PublicKey, M),
    Stop,
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::broadcast::Broadcast;
    use crate::test::*;
    use crate::System;

    use drop::crypto::key::exchange::Exchanger;
    use drop::net::TcpConnector;

    static DATA: AtomicUsize = AtomicUsize::new(0);

    #[tokio::test]
    async fn single_message() {
        init_logger();
        const SIZE: usize = 10;
        let mut addrs = test_addrs(SIZE);
        let public = create_receivers(
            addrs.clone().into_iter(),
            |mut connection| async move {
                let data = DATA.fetch_add(1, Ordering::AcqRel);
                let mut received = Vec::new();
                connection.send(&data).await.expect("failed to send");

                for _ in 0..(SIZE - 1) {
                    let integer =
                        connection.receive().await.expect("recv failed");

                    received.push(integer);
                }
                assert_eq!(
                    received.len(),
                    SIZE - 1,
                    "wrong number of messages"
                );
                (0..SIZE).filter(|x| *x != data).for_each(|x| {
                    assert!(received.contains(&x));
                })
            },
        )
        .await;
        let pkeys = public.iter().map(|x| x.0);
        let connector = TcpConnector::new(Exchanger::random());
        let system = System::new_with_connector(
            &connector,
            pkeys,
            addrs.drain(..).map(|x| x.1),
        )
        .await;
        let mut bcast = ReliableMultiShot::new(system).await;

        let mut incoming = bcast.incoming().await;

        for _ in 0..10usize {
            let (_, data) = incoming.next().await.expect("missing message");

            assert!((0..10usize).contains(&data), "wrong message received");
        }
    }

    #[tokio::test]
    async fn many_messages() {
        let (_, handle, system): (_, _, System<usize>) =
            create_system(10, |mut connection| async move {
                for i in 0..10usize {
                    connection.send(&i).await.expect("send failed");
                }

                connection
                    .close()
                    .await
                    .expect("connection failed to close");
            })
            .await;
        let mut broadcast = ReliableMultiShot::new(system).await;
        let mut incoming = broadcast.incoming().await;

        let mut received = Vec::new();

        while received.len() < 10 {
            received.push(incoming.next().await.unwrap());
        }

        received.sort();

        let received: Vec<usize> = received.into_iter().map(|x| x.1).collect();

        assert_eq!(
            received,
            (0..10).collect::<Vec<_>>(),
            "bad sequence of messages"
        );

        handle.await.expect("system failure");

        assert!(incoming.next().await.is_none());
    }
}
