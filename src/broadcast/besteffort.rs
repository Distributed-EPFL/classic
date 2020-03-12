use super::{Broadcast, BroadcastStream};
use crate::{Message, System};

use drop::async_trait;
use drop::crypto::key::exchange::PublicKey;

use futures::future;
use futures::stream::{Stream, StreamExt};

use tracing::error;

/// A `Broadcast` implementation that does not provide any guarantee of
/// reliability.
pub struct BestEffort<M: Message + 'static> {
    system: System<M>,
}

impl<M: Message + 'static> BestEffort<M> {
    pub(super) fn new(system: System<M>) -> Self {
        Self { system }
    }

    /// Get a mutable reference to the `System` used by this
    /// `BestEffortBroadcast`
    pub fn system(&mut self) -> &mut System<M> {
        &mut self.system
    }
}

#[async_trait]
impl<M: Message> Broadcast<M> for BestEffort<M> {
    async fn broadcast(&mut self, message: &M) -> Result<(), ()> {
        let mut values = self.system.connections().await;

        if values.is_empty() {
            error!("no peers to broadcast to");
            return Err(());
        }

        if future::join_all(values.iter_mut().map(|c| c.send(*message)))
            .await
            .iter()
            .any(|x| x.is_err())
        {
            Err(())
        } else {
            Ok(())
        }
    }

    async fn incoming(
        &mut self,
    ) -> Box<dyn Stream<Item = (PublicKey, M)> + Send + Unpin> {
        let current = self
            .system
            .connections()
            .await
            .drain(..)
            .map(|mut x| x.stream())
            .collect::<Vec<_>>();

        Box::new(BroadcastStream::new(
            current.into_iter(),
            self.system
                .peer_source()
                .filter_map(|x| async { x.map(|mut x| x.stream()).ok() }),
        ))
    }
}

impl<M: Message> From<System<M>> for BestEffort<M> {
    fn from(system: System<M>) -> Self {
        Self::new(system)
    }
}

#[cfg(test)]
mod test {
    use std::net::{Ipv4Addr, SocketAddr};
    use std::sync::atomic::{AtomicU16, Ordering};

    use super::*;
    use crate::broadcast::Broadcast;
    use crate::System;

    use drop::crypto::key::exchange::Exchanger;
    use drop::net::TcpConnector;

    static PORT_OFFSET: AtomicU16 = AtomicU16::new(0);

    fn next_test_ip4() -> SocketAddr {
        (
            Ipv4Addr::LOCALHOST,
            9000 + PORT_OFFSET.fetch_add(1, Ordering::AcqRel),
        )
            .into()
    }

    #[tokio::test]
    async fn single_shot() {
        let exchanger = Exchanger::random();
        let tcp = TcpConnector::new(Exchanger::random());
        let candidates = (0..2usize)
            .map(|_| (*exchanger.keypair().public(), next_test_ip4()));
        let system: System<usize> =
            System::new_with_connector(&tcp, candidates).await;

        let mut broadcast = BestEffort::new(system);

        broadcast
            .broadcast(&0usize)
            .await
            .expect("failed to broadcast");
    }
}
