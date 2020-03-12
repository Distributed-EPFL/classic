use std::pin::Pin;
use std::task::{Context, Poll};

use super::Message;

use drop::async_trait;
use drop::crypto::key::exchange::PublicKey;

use futures::stream::{self, SelectAll, Stream, StreamExt};

mod besteffort;
pub use besteffort::BestEffort;

mod multishot;
pub use multishot::ReliableMultiShot;

mod uniform;
pub use uniform::UniformReliable;

#[async_trait]
pub trait Broadcast<M: Message> {
    /// Broadcast a `Message` providing guarantees according to the actual
    /// implementation of this trait.
    async fn broadcast(&mut self, message: &M) -> Result<(), ()>;

    /// Get a `Stream` that contains messages broadcasted by other peers in the
    /// `System`
    async fn incoming(
        &mut self,
    ) -> Box<dyn Stream<Item = (PublicKey, M)> + Send + Unpin>;
}

/// A stream that produces `Message`s received from other peers in the system.
pub struct BroadcastStream<M, S>
where
    M: Message + 'static,
    S: Stream<Item = (PublicKey, M)> + Send,
{
    future: SelectAll<S>,
    peer_source: Pin<Box<dyn Stream<Item = S> + Send>>,
}

impl<M, S> BroadcastStream<M, S>
where
    M: Message + 'static,
    S: Stream<Item = (PublicKey, M)> + Send + Unpin + 'static,
{
    pub fn new<I, P>(streams: I, peer_source: P) -> Self
    where
        I: Iterator<Item = S>,
        P: Stream<Item = S> + Send + 'static,
    {
        Self {
            future: stream::select_all(streams),
            peer_source: Box::pin(peer_source),
        }
    }

    pub fn add_peer(&mut self, stream: S) {
        self.future.push(stream);
    }
}

impl<M, S> Stream for BroadcastStream<M, S>
where
    M: Message + 'static,
    S: Stream<Item = (PublicKey, M)> + Unpin + Send + 'static,
{
    type Item = (PublicKey, M);

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Option<Self::Item>> {
        while let Poll::Ready(Some(connection)) =
            self.peer_source.poll_next_unpin(cx)
        {
            self.add_peer(connection);
        }

        self.future.poll_next_unpin(cx)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use drop::crypto::key::exchange::Exchanger;

    use futures::stream;

    const SIZE: usize = 10;

    impl Message for usize {}

    fn make_stream<
        M: Message + 'static,
        I: Iterator<Item = S>,
        S: Stream<Item = (PublicKey, M)> + Unpin + Send + 'static,
    >(
        stream: I,
    ) -> BroadcastStream<M, S> {
        BroadcastStream::new(stream, stream::empty())
    }

    #[tokio::test]
    async fn bcast_stream() {
        let bstream = make_stream((0..SIZE).map(|_| {
            stream::iter(
                (0..SIZE).map(|_| (*Exchanger::random().keypair().public(), 1)),
            )
        }));
        let value = bstream.fold(0, |x, y| async move { x + y.1 }).await;

        assert_eq!(value, SIZE * SIZE, "streaming error");
    }

    #[tokio::test]
    async fn bcast_for_each() {
        let bstream = make_stream((0..SIZE).map(|_| {
            stream::iter(
                (0..SIZE).map(|_| (*Exchanger::random().keypair().public(), 1)),
            )
        }));

        bstream
            .for_each(
                |x| async move { assert_eq!(1, x.1, "wrong value produced") },
            )
            .await;
    }
}
