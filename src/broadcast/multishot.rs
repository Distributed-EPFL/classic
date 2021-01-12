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
