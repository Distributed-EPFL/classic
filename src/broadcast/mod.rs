use std::pin::Pin;
use std::task::{Context, Poll};

use super::Message;

use drop::async_trait;
use drop::crypto::key::exchange::PublicKey;
use drop::net::SendError;

use futures::stream::{self, SelectAll, Stream, StreamExt};

use tracing::{debug, info};

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
    async fn broadcast(&mut self, message: &M) -> Vec<(PublicKey, SendError)>;

    /// Get a the next delivered `Message` from this `Broadcast` instance
    async fn delivered(&mut self) -> Option<M>;
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
