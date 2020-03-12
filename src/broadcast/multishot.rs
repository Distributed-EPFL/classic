use std::collections::HashSet;
use std::future::Future;

use super::besteffort::BestEffort;
use super::*;
use crate::{ConnectionManager, System};

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

    async fn broadcast(&mut self, message: &M) {
        if self.delivered.insert(*message) {
            debug!("retransmitting {:?}", message);
            future::join_all(self.outgoing.iter().map(|x| x.send(*message)))
                .await;
        }
    }

    async fn start(mut self) {
        loop {
            match Self::poll(
                self.incoming.next().boxed(),
                self.peer_source.next().boxed(),
            )
            .await
            {
                Action::Insert(connection) => {
                    info!("new connection {}", connection);
                    self.outgoing.insert(connection);
                }
                Action::Broadcast(ref message) => {
                    self.broadcast(message).await;
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
            Either::Left((Some((_, message)), _)) => Action::Broadcast(message),
            Either::Left((None, _)) => Action::Stop,
        }
    }
}

enum Action<M: Message + 'static> {
    Insert(ConnectionManager<M>),
    Broadcast(M),
    Stop,
}
