use std::collections::HashSet;

use super::besteffort::*;
use super::{BroadcastTask, Broadcaster, Deliverer};
use crate::{Message, System};

use drop::async_trait;
use drop::crypto::key::exchange::PublicKey;
use drop::net::SendError;

use tokio::sync::mpsc;

use tracing::error;

/// An implementation of `Broadcast` that provides the following guarantees:
/// * a `Message` is delivered exactly once
/// * a `Message` is delivered by every correct process or no process at all
pub struct ReliableMultiShot {}

impl ReliableMultiShot {
    /// Create a new `ReliableMultiShot` that will use the given
    /// `System` to broadcast `Message`s
    pub fn with<M: Message + 'static>(
        system: System,
    ) -> (
        ReliableMultiShotBroadcaster<M>,
        ReliableMultiShotDeliverer<M>,
    ) {
        let (bcast_tx, bcast_rx) = mpsc::channel(32);
        let (rb_tx, rb_rx) = mpsc::channel(32);
        let (broadcaster, deliverer) = BestEffort::with::<M>(system);
        let (err_rx, _) = BroadcastTask::spawn(broadcaster, bcast_rx, rb_rx);
        let broadcaster = ReliableMultiShotBroadcaster::new(bcast_tx, err_rx);
        let receiver = ReliableMultiShotDeliverer::new(deliverer, rb_tx);

        (broadcaster, receiver)
    }
}

/// The sending end of the `ReliableMultiShot` broadcast primitive.
/// See `ReliableMultiShot` for more.
pub struct ReliableMultiShotBroadcaster<M: Message + 'static> {
    broadcast_out: mpsc::Sender<M>,
    errors_in: mpsc::Receiver<Option<Vec<(PublicKey, SendError)>>>,
}

impl<M: Message + 'static> ReliableMultiShotBroadcaster<M> {
    fn new(
        broadcast_out: mpsc::Sender<M>,
        errors_in: mpsc::Receiver<Option<Vec<(PublicKey, SendError)>>>,
    ) -> Self {
        Self {
            broadcast_out,
            errors_in,
        }
    }
}

#[async_trait]
impl<M: Message> Broadcaster<M> for ReliableMultiShotBroadcaster<M> {
    async fn broadcast(
        &mut self,
        message: &M,
    ) -> Option<Vec<(PublicKey, SendError)>> {
        if let Err(e) = self.broadcast_out.send(message.clone()).await {
            error!("no broadcast task running: {}", e);
            None
        } else {
            self.errors_in.recv().await.unwrap_or(None)
        }
    }

    fn known_peers(&self) -> HashSet<PublicKey> {
        todo!()
    }
}

/// The delivery end of the reliable multishot broadcast primitive.
/// See `ReliableMultiShot` for more information.
pub struct ReliableMultiShotDeliverer<M: Message + 'static> {
    beb: BestEffortReceiver<M>,
    rebroadcast: mpsc::Sender<M>,
    delivered: HashSet<M>,
}

impl<M: Message + 'static> ReliableMultiShotDeliverer<M> {
    fn new(beb: BestEffortReceiver<M>, rebroadcast: mpsc::Sender<M>) -> Self {
        Self {
            beb,
            rebroadcast,
            delivered: HashSet::default(),
        }
    }
}

#[async_trait]
impl<M: Message + 'static> Deliverer<M> for ReliableMultiShotDeliverer<M> {
    async fn deliver(&mut self) -> Option<(PublicKey, M)> {
        loop {
            let (pkey, message) = self.beb.deliver().await?;

            if self.delivered.insert(message.clone()) {
                if self.rebroadcast.send(message.clone()).await.is_err() {
                    return None;
                }

                return Some((pkey, message));
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::*;
    use crate::test::*;
    use crate::System;

    use drop::crypto::key::exchange::Exchanger;
    use drop::net::TcpConnector;

    static DATA: AtomicUsize = AtomicUsize::new(0);
    const VALUE: usize = 300;

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
        let (_, mut deliverer) = ReliableMultiShot::with(system);

        for _ in 0..10usize {
            let (_, data) = deliverer.deliver().await.expect("early eof");

            assert!((0..10usize).contains(&data), "wrong message received");
        }
    }

    #[tokio::test]
    async fn many_messages() {
        let (_, handle, system) =
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
        let (_, mut deliverer) = ReliableMultiShot::with(system);

        let mut received: Vec<usize> = Vec::new();

        while received.len() < 10 {
            let (_, data) = deliverer.deliver().await.expect("early  eof");
            received.push(data);
        }

        received.sort();

        assert_eq!(
            received,
            (0..10).collect::<Vec<_>>(),
            "bad sequence of messages"
        );

        handle.await.expect("system failure");
    }

    #[tokio::test]
    async fn rebroadcast() {
        static SHOULD_SEND: AtomicBool = AtomicBool::new(false);

        let (_, handle, system) =
            create_system(10, |mut connection| async move {
                if SHOULD_SEND.compare_and_swap(false, true, Ordering::AcqRel) {
                    connection.send(&VALUE).await.expect("send failed");
                }

                let value =
                    connection.receive::<usize>().await.expect("recv failed");

                assert_eq!(VALUE, value, "wrong value received");
            })
            .await;

        let (_, mut deliverer) = ReliableMultiShot::with::<usize>(system);

        let (_, value) = deliverer.deliver().await.expect("no messages");

        assert_eq!(value, VALUE, "wrong value delivered");

        handle.await.expect("panic");
    }
}
