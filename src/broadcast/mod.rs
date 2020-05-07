use std::collections::HashSet;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::Message;

use drop::async_trait;
use drop::crypto::key::exchange::PublicKey;
use drop::net::{ConnectionRead, ReceiveError, SendError};

use futures::future::{Fuse, FusedFuture, FutureExt};
use futures::sink::Sink;
use futures::stream::{self, Stream, StreamExt};

use tokio::sync::broadcast;
use tokio::task;

use tracing::{debug_span, error, warn};
use tracing_futures::Instrument;

mod besteffort;
pub use besteffort::{BestEffort, BestEffortBroadcaster, BestEffortReceiver};

mod multishot;
pub use multishot::ReliableMultiShot;

mod uniform;
pub use uniform::UniformReliable;

/// This is the error type returned by `Broadcaster::broadcast`.
/// If this value is `None` the `Broadcaster` is not in an usable state anymore
/// and should be dropped.
/// In case of a `Some` value it indicates which `Connection` failed by
/// providing pairs of `PublicKey` and a detailed error value. An empty `Vec`
/// indicates that no send failures were encountered.
pub type BroadcastError = Option<Vec<(PublicKey, SendError)>>;

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

    /// Returns a `HashSet` of all `PublicKey` currently known by this
    /// `Broadcaster`;
    fn known_peers(&self) -> HashSet<PublicKey>;

    /// Transform this `Broadcaster` into a `Sink` that can be used with the
    /// adpaters from  the `futures` crate. `Message`s consumed by the `Sink`
    /// will be broadcasted using the `Broadcaster`.
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
pub trait Deliverer<M: Message + 'static>: Send {
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

/// A wrapper for `Deliverer`s that creates two new `Deliverer` that will each
/// deliver the same set of `Message`s. `Message`s are `Clone`d and sent to each
/// instance of the `Duplicator`. This may cause performance and memory issues
/// if a `Deliverer` is duplicated  too many times, especially if the `Message`
/// type is big.
pub struct Duplicator<D: Deliverer<M>, M: Message + 'static> {
    _d: PhantomData<D>,
    msg_rx: broadcast::Receiver<(PublicKey, M)>,
}

impl<D: Deliverer<M> + 'static, M: Message + 'static> Duplicator<D, M> {
    /// Create a new `Duplicator` from another `Deliverer`
    pub fn duplicate(mut deliverer: D) -> (Self, Self) {
        let (msg_tx, msg_rx) = broadcast::channel(32);
        let msg_rx2 = msg_tx.subscribe();

        let first = Self {
            msg_rx,
            _d: PhantomData,
        };
        let second = Self {
            msg_rx: msg_rx2,
            _d: PhantomData,
        };

        task::spawn(async move {
            while let Some(v) = deliverer.deliver().await {
                match msg_tx.send(v) {
                    Ok(v) => {
                        debug_assert_eq!(
                            v, 2,
                            "incorrect number of subscribers"
                        );
                    }
                    Err(broadcast::SendError(msg)) => {
                        warn!("all receivers dropped, lost message {:?}", msg);
                        return;
                    }
                }
            }
        });

        (first, second)
    }
}

#[async_trait]
impl<D: Deliverer<M>, M: Message + 'static> Deliverer<M> for Duplicator<D, M> {
    async fn deliver(&mut self) -> Option<(PublicKey, M)> {
        match self
            .msg_rx
            .recv()
            .instrument(debug_span!("duplicator"))
            .await
        {
            Ok(msg) => Some(msg),
            Err(broadcast::RecvError::Closed) => None,
            Err(broadcast::RecvError::Lagged(count)) => {
                warn!("delivery lagging  behind by {} messages", count);
                None
            }
        }
    }
}

type ReceiverResult<M> = Result<(M, ConnectionRead), (PublicKey, ReceiveError)>;

/// A wrapper that allows a `ConnectionRead` to be turned into a `Stream` of
/// `Message`s
pub struct MessageStream<M: Message + 'static> {
    future: Pin<Box<dyn Future<Output = ReceiverResult<M>> + Send>>,
}

impl<M: Message + 'static> MessageStream<M> {
    /// Create a new `Stream` of `Message` from the given `ConnectionRead`
    pub fn new(connection: ConnectionRead) -> Self {
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

        fn known_peers(&self) -> HashSet<PublicKey> {
            panic!("unexpected call")
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
