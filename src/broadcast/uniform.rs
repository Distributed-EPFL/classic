use std::pin::Pin;
use std::task::{Context, Poll};

use super::{BestEffort, Broadcast};
use crate::{Connection as ConnectionManager, Message, System};

use drop::async_trait;
use drop::crypto::key::exchange::PublicKey;

use futures::stream::{Stream, StreamExt};

use hashbrown::hash_map::Entry;
use hashbrown::{HashMap, HashSet};

/// A `Broadcast` implementation that provides the following guarantees:
/// * all correct process will eventually deliver a `Message` broadcasted by a
/// correct process
pub struct UniformReliable<M: Message + 'static> {
    beb: BestEffort<M>,
}

impl<M: Message + 'static> UniformReliable<M> {
    /// Create a new `UniformReliableBroadcast` with the given besteffort
    /// broadcast.
    pub fn new(system: System<M>) -> Self {
        let beb = BestEffort::from(system);

        Self { beb }
    }
}

#[async_trait]
impl<M: Message + Unpin> Broadcast<M> for UniformReliable<M> {
    async fn broadcast(&mut self, message: &M) -> Result<(), ()> {
        todo!()
    }

    async fn incoming(
        &mut self,
    ) -> Box<dyn Stream<Item = (PublicKey, M)> + Send + Unpin> {
        let peer_source = self
            .beb
            .system()
            .peer_source()
            .filter_map(|x| async { x.ok() })
            .boxed();
        let messages = self.beb.incoming().await;
        let initial = self
            .beb
            .system()
            .connections()
            .await
            .into_iter()
            .map(|x| *x.public());

        Box::new(Acker::new(peer_source, messages, initial))
    }
}

struct Acker<M, S, P>
where
    Self: Unpin,
    M: Message + 'static,
    S: Stream<Item = (PublicKey, M)>,
    P: Stream<Item = ConnectionManager<M>> + Send,
{
    messages: HashMap<M, HashSet<PublicKey>>,
    peer_source: P,
    peers: HashSet<PublicKey>,
    incoming: S,
}

impl<M, S, P> Acker<M, S, P>
where
    M: Message + Unpin,
    S: Stream<Item = (PublicKey, M)> + Send + Unpin,
    P: Stream<Item = ConnectionManager<M>> + Send + Unpin,
{
    fn new<I: Iterator<Item = PublicKey>>(
        peer_source: P,
        incoming: S,
        initial: I,
    ) -> Self {
        Self {
            peer_source,
            incoming,
            peers: initial.collect(),
            messages: Default::default(),
        }
    }

    fn can_deliver(&mut self, pkey: &PublicKey, message: &M) -> bool {
        match self.messages.entry(*message) {
            Entry::Occupied(mut e) => {
                let acks = e.get_mut();
                acks.insert(*pkey);

                self.peers.difference(acks).count() == 0
            }
            Entry::Vacant(e) => {
                let mut new_acks = HashSet::default();
                new_acks.insert(*pkey);
                e.insert(new_acks);

                false
            }
        }
    }

    fn update_peer_list(&mut self, conn: &ConnectionManager<M>) {
        self.peers.insert(*conn.public());
    }

    fn process_message(
        mut self: Pin<&mut Self>,
        data: Option<(PublicKey, M)>,
        cx: &mut Context,
    ) -> Poll<Option<<Self as Stream>::Item>> {
        match data {
            Some((ref pkey, ref message)) => {
                if self.as_mut().can_deliver(pkey, message) {
                    Poll::Ready(Some((*pkey, *message)))
                } else {
                    self.poll_next(cx)
                }
            }
            _ => Poll::Ready(None),
        }
    }

    fn process_peer(&mut self, data: Option<ConnectionManager<M>>) {
        if let Some(ref connection) = data {
            self.update_peer_list(connection);
        }
    }
}

impl<M, S, P> Stream for Acker<M, S, P>
where
    M: Message + Unpin,
    S: Stream<Item = (PublicKey, M)> + Send + Unpin,
    P: Stream<Item = ConnectionManager<M>> + Send + Unpin,
{
    type Item = (PublicKey, M);

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Option<Self::Item>> {
        while let Poll::Ready(peer) = self.peer_source.poll_next_unpin(cx) {
            self.as_mut().process_peer(peer);
        }

        match self.incoming.poll_next_unpin(cx) {
            Poll::Ready(message) => self.process_message(message, cx),
            e => e,
        }
    }
}
