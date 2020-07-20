use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use super::{BestEffort, BroadcastError, Broadcaster, Deliverer};
use crate::{Message, System};

use drop::async_trait;
use drop::crypto::key::exchange::PublicKey;
use drop::net::SendError;

use futures::future::Either;
use futures::FutureExt;

use tokio::sync::mpsc;
use tokio::task::{self, JoinHandle};

use tracing::{debug_span, info, warn};
use tracing_futures::Instrument;

/// A `Broadcast` implementation that provides the following guarantees:
/// * all correct process will eventually deliver a `Message` broadcasted by a
/// correct process
pub struct UniformReliable {}

impl UniformReliable {
    /// Create both ends of a `UniformReliable` broadcast primitive using the
    /// given `System`.
    pub fn with<M: Message + 'static>(
        system: System,
    ) -> (UniformReliableBroadcaster<M>, UniformReliableDeliverer<M>) {
        let (beb_bcast, beb_deliver) = BestEffort::with::<M>(system);
        let (msg_tx, msg_rx) = mpsc::channel(32);
        let (error_tx, error_rx) = mpsc::channel(1);
        let (deliver_tx, deliver_rx) = mpsc::channel(32);

        AckTask::spawn(beb_deliver, beb_bcast, deliver_tx, msg_rx, error_tx);

        (
            UniformReliableBroadcaster::new(msg_tx, error_rx),
            UniformReliableDeliverer::new(deliver_rx),
        )
    }
}

/// The sending end of the `UniformReliable` broadcast primitive'
pub struct UniformReliableBroadcaster<M: Message + 'static> {
    msg_tx: mpsc::Sender<M>,
    error_rx: mpsc::Receiver<BroadcastError>,
}

impl<M: Message + 'static> UniformReliableBroadcaster<M> {
    fn new(
        msg_tx: mpsc::Sender<M>,
        error_rx: mpsc::Receiver<BroadcastError>,
    ) -> Self {
        Self { msg_tx, error_rx }
    }
}

#[async_trait]
impl<M: Message + 'static> Broadcaster<M> for UniformReliableBroadcaster<M> {
    async fn broadcast(
        &mut self,
        message: &M,
    ) -> Option<Vec<(PublicKey, SendError)>> {
        if self.msg_tx.send(message.clone()).await.is_err() {
            None
        } else {
            self.error_rx.recv().await.unwrap_or(None)
        }
    }

    fn known_peers(&self) -> HashSet<PublicKey> {
        todo!()
    }
}

/// The delivery end of the `UniformReliable` broadcast primitive.
pub struct UniformReliableDeliverer<M: Message + 'static> {
    msg_rx: mpsc::Receiver<(PublicKey, M)>,
}

impl<M: Message + 'static> UniformReliableDeliverer<M> {
    fn new(msg_rx: mpsc::Receiver<(PublicKey, M)>) -> Self {
        Self { msg_rx }
    }
}

#[async_trait]
impl<M: Message + 'static> Deliverer<M> for UniformReliableDeliverer<M> {
    async fn deliver(&mut self) -> Option<(PublicKey, M)> {
        self.msg_rx.recv().await
    }
}

struct AckTask<B, D, M>
where
    B: Broadcaster<M> + 'static,
    D: Deliverer<M> + 'static,
    M: Message + 'static,
{
    incoming: D,
    outgoing: B,
    msg_tx: mpsc::Sender<(PublicKey, M)>,
    msg_rx: mpsc::Receiver<M>,
    error_tx: mpsc::Sender<BroadcastError>,
    pending: HashMap<M, HashSet<PublicKey>>,
}

impl<B, D, M> AckTask<B, D, M>
where
    B: Broadcaster<M> + 'static,
    D: Deliverer<M> + 'static,
    M: Message + 'static,
{
    fn spawn(
        incoming: D,
        outgoing: B,
        msg_tx: mpsc::Sender<(PublicKey, M)>,
        msg_rx: mpsc::Receiver<M>,
        error_tx: mpsc::Sender<BroadcastError>,
    ) -> JoinHandle<(B, D)> {
        let task = Self {
            incoming,
            outgoing,
            msg_tx,
            error_tx,
            msg_rx,
            pending: HashMap::default(),
        };

        task.task()
    }

    fn can_deliver(&mut self, pkey: &PublicKey, message: &M) -> bool {
        match self.pending.entry(message.clone()) {
            Entry::Occupied(mut e) => {
                if e.get_mut().insert(*pkey) {
                    info!("new ack from {} for {:?}", pkey, message);
                    let peers = self.outgoing.known_peers();

                    peers.difference(e.get()).count() == 0
                } else {
                    false
                }
            }
            Entry::Vacant(e) => {
                info!("unseen message {:?}", message);

                let mut new_acks = HashSet::default();
                new_acks.insert(*pkey);
                e.insert(new_acks);

                false
            }
        }
    }

    async fn poll(&mut self) -> Either<Option<(PublicKey, M)>, Option<M>> {
        futures::select! {
            out = self.msg_rx.recv().fuse() => Either::Right(out),
            r#in = self.incoming.deliver().fuse() => Either::Left(r#in),
        }
    }

    fn task(mut self) -> JoinHandle<(B, D)> {
        task::spawn(
            async move {
                let mut peer_warn = true;

                loop {
                    match self.poll().await {
                        Either::Right(Some(ref msg)) => {
                            let result = self.outgoing.broadcast(msg).await;
                            if self.error_tx.send(result).await.is_err() {
                                warn!("broadcaster has been dropped");
                            }
                        }
                        Either::Right(None) => {
                            if peer_warn {
                                warn!("no more peers available in system");
                                peer_warn = false;
                            }

                            continue;
                        }
                        Either::Left(Some((pkey, msg))) => {
                            if self.can_deliver(&pkey, &msg) {
                                info!("all acks for {:?} delivering", msg);
                                if self.msg_tx.send((pkey, msg)).await.is_err()
                                {
                                    warn!("no more incoming messages");
                                    break;
                                }
                            }
                        }
                        Either::Left(None) => {
                            warn!("end of broadcast stream");
                            break;
                        }
                    }
                }

                (self.outgoing, self.incoming)
            }
            .instrument(debug_span!("ack_task")),
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::*;

    const VALUE: usize = 10;

    #[tokio::test]
    async fn single_message_broadcast() {
        init_logger();

        let (_, handle, system) =
            create_system(10, |mut connection| async move {
                let value =
                    connection.receive::<usize>().await.expect("recv failed");

                assert_eq!(value, VALUE, "wrong value broadcasted");
            })
            .await;

        let (mut bcast, _) = UniformReliable::with::<usize>(system);

        let errors = bcast.broadcast(&VALUE).await.expect("broadcast failed");

        assert!(errors.is_empty(), "some peers failed to receive");

        handle.await.expect("failure");
    }

    #[tokio::test]
    async fn single_message_delivery() {
        init_logger();

        let (_, handle, system) =
            create_system(50, |mut connection| async move {
                connection.send(&VALUE).await.expect("send failed");
            })
            .await;

        let (_, mut delivery) = UniformReliable::with::<usize>(system);

        let (_, value) = delivery.deliver().await.expect("no messages");

        assert_eq!(value, VALUE, "wrong value delivered");

        handle.await.expect("panic");
    }

    #[tokio::test]
    async fn broadcast_then_deliver() {
        const VALUE: usize = 200;

        init_logger();

        let (_, handle, system) =
            create_system(50, |mut connection| async move {
                let value: usize =
                    connection.receive().await.expect("failed to receive");
                assert_eq!(value, VALUE, "wrong value received");
                connection.send(&value).await.expect("failed to send");
            })
            .await;

        let (mut bcast, mut delivery) = UniformReliable::with::<usize>(system);

        bcast.broadcast(&VALUE).await.expect("failed to broadcast");

        let (_, delivered) = delivery.deliver().await.expect("deliver failed");

        assert_eq!(delivered, VALUE, "wrong value delivered");

        handle.await.expect("system failure");
    }

    #[tokio::test]
    async fn failed_peer_does_not_deliver() {
        use std::sync::atomic::{AtomicBool, Ordering};

        const VALUE: &str = "Test message no1";
        static SHOULD_FAIL: AtomicBool = AtomicBool::new(true);

        init_logger();

        let (_, handle, system) =
            create_system(50, |mut connection| async move {
                if !SHOULD_FAIL.compare_and_swap(true, false, Ordering::AcqRel)
                {
                    connection
                        .send(&VALUE.to_string())
                        .await
                        .expect("send failed");
                }

                connection.close().await.expect("close failed");
            })
            .await;

        let (_, mut delivery) = UniformReliable::with::<String>(system);

        let message = delivery.deliver().await;

        assert!(message.is_none(), "delivered unacked message");

        handle.await.expect("system failure");
    }
}
