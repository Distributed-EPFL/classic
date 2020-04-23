use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::Message;

use drop::async_trait;
use drop::crypto::key::exchange::PublicKey;
use drop::net::SendError;

use futures::future::{Fuse, FusedFuture, FutureExt};
use futures::sink::Sink;
use futures::stream::{self, Stream, StreamExt};

mod besteffort;
pub use besteffort::BestEffort;

mod multishot;
pub use multishot::ReliableMultiShot;

// mod uniform;
// pub use uniform::UniformReliable;

/// This trait is used to broadcast `Message`s providing guarantees depending on
/// the actual implementation of the trait.
#[async_trait]
pub trait Broadcaster<M: Message>: Send {
    /// Broadcast a `Message` using the given `System`.
    /// Guarantees provided vary according to the actual implementation of
    /// this trait.
    /// If broadcasting succeeded for at least one peer a `Some` is returned
    /// containing a `Vec` of all encountered errors. An empty `Vec` means
    /// every peer successfully received the broadcasted `Message`.
    /// If `None` is returned, this `Broadcaster` instance is not usable
    /// anymore.
    async fn broadcast(
        &mut self,
        message: &M,
    ) -> Option<Vec<(PublicKey, SendError)>>;

    fn to_sink(self) -> BroadcastSink<Self, M>
    where
        Self: Sized + 'static,
        M: 'static,
    {
        BroadcastSink::new(self)
    }
}

type SinkFuture<B> =
    dyn Future<Output = (Option<Vec<(PublicKey, SendError)>>, B)> + Send;

/// A `Sink` that will consume all messages that are given to it and broadcast
/// them using a method depending on which `Broadcaster` it was created with.
pub struct BroadcastSink<B: Broadcaster<M>, M: Message> {
    future: Fuse<Pin<Box<SinkFuture<B>>>>,
    broadcaster: Option<B>,
    failed: bool,
    _m: PhantomData<M>,
}

impl<M: Message + 'static, B: Broadcaster<M> + 'static> BroadcastSink<B, M> {
    fn new(broadcaster: B) -> Self {
        Self {
            future: Fuse::terminated(),
            broadcaster: Some(broadcaster),
            failed: false,
            _m: PhantomData,
        }
    }

    fn check(&mut self) -> bool {
        self.future.is_terminated() && !self.failed
    }

    fn ready_to_send(&mut self) -> Option<B> {
        if self.future.is_terminated() || !self.failed {
            self.broadcaster.take()
        } else {
            None
        }
    }

    fn make_future(
        mut broadcaster: B,
        item: M,
    ) -> Fuse<Pin<Box<SinkFuture<B>>>> {
        async move {
            let errs = broadcaster.broadcast(&item).await;

            (errs, broadcaster)
        }
        .boxed()
        .fuse()
    }
}

impl<B, M> Sink<M> for BroadcastSink<B, M>
where
    B: Broadcaster<M> + Unpin + 'static,
    M: Message + Unpin + 'static,
{
    type Error = Option<Vec<(PublicKey, SendError)>>;

    fn poll_ready(
        self: Pin<&mut Self>,
        _: &mut Context,
    ) -> Poll<Result<(), Self::Error>> {
        if self.get_mut().check() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn start_send(self: Pin<&mut Self>, item: M) -> Result<(), Self::Error> {
        let mut_self = self.get_mut();

        if let Some(broadcaster) = mut_self.ready_to_send() {
            mut_self.future = Self::make_future(broadcaster, item);
            Ok(())
        } else {
            Err(None)
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Result<(), Self::Error>> {
        let mut_self = self.get_mut();

        match mut_self.future.poll_unpin(cx) {
            Poll::Ready((Some(errs), bcast)) => {
                mut_self.broadcaster = Some(bcast);
                Poll::Ready(Err(Some(errs)))
            }
            Poll::Ready((None, _)) => {
                mut_self.failed = true;
                Poll::Ready(Err(None))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}

/// A `Deliverer` is the receiving end of the broadcast primitive.
#[async_trait]
pub trait Deliverer<M: Message + 'static> {
    /// Get the next delivered `Message` on the provided `System`. Returns
    /// `None` if no further  `Message`s can be received using this `Deliverer`.
    async fn deliver(&mut self) -> Option<(PublicKey, M)>;

    /// Turns this `Deliverer` into a `Stream` that will produce pairs of
    /// `PublicKey`s and `Message`s.
    fn to_stream(self) -> Pin<Box<dyn Stream<Item = (PublicKey, M)>>>
    where
        Self: Send + Sized + 'static,
    {
        stream::unfold(self, |mut deliverer: Self| async move {
            if let Some(msg) = deliverer.deliver().await {
                Some((msg, deliverer))
            } else {
                None
            }
        })
        .boxed()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use futures::SinkExt;

    struct TestBroadcaster<F, M>
    where
        F: FnMut(&M) -> Option<Vec<(PublicKey, SendError)>>,
        M: Message + 'static,
    {
        closure: F,
        _m: PhantomData<M>,
    }

    impl<F, M> TestBroadcaster<F, M>
    where
        F: FnMut(&M) -> Option<Vec<(PublicKey, SendError)>>,
        M: Message,
    {
        fn with(closure: F) -> Self {
            Self {
                closure,
                _m: PhantomData,
            }
        }
    }

    #[async_trait]
    impl<F, M> Broadcaster<M> for TestBroadcaster<F, M>
    where
        F: FnMut(&M) -> Option<Vec<(PublicKey, SendError)>> + Send,
        M: Message,
    {
        async fn broadcast(
            &mut self,
            message: &M,
        ) -> Option<Vec<(PublicKey, SendError)>> {
            (self.closure)(message)
        }
    }

    #[tokio::test]
    async fn failed_sink_returns_none() {
        let bcast = TestBroadcaster::with(|_| None);
        let mut sink = bcast.to_sink();

        assert!(
            sink.send(0usize).await.expect_err("oops").is_none(),
            "fake error reported"
        );
    }

    #[tokio::test]
    async fn sink_reports_errors() {
        let bcast = TestBroadcaster::with(|_| Some(Vec::new()));
        let mut sink = bcast.to_sink();

        let errors = sink
            .send(0usize)
            .await
            .expect_err("failed silently")
            .expect("unexpected sink failure");

        assert_eq!(errors.len(), 0, "incorrect number of errors");
    }
}
