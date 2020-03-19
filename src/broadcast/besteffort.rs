use super::{Broadcast, BroadcastStream};
use crate::{Message, System};

use drop::async_trait;
use drop::crypto::key::exchange::PublicKey;

use futures::future;
use futures::stream::{Stream, StreamExt};

use tracing::{debug, debug_span, error, info};
use tracing_futures::Instrument;

/// A `Broadcast` implementation that does not provide any guarantee of
/// reliability.
pub struct BestEffort<M: Message + 'static> {
    system: System<M>,
}

impl<M: Message + 'static> BestEffort<M> {
    pub(super) fn new(system: System<M>) -> Self {
        info!("creating best effort broadcast");
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

        if future::join_all(
            values.iter_mut().map(|c| {
                c.send(*message).instrument(debug_span!("sending {:?}"))
            }),
        )
        .await
        .iter()
        .any(|x| x.is_err())
        {
            error!("failed to send message to at least one peer");
            Err(())
        } else {
            debug!("broadcasted {:?}", message);
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

        info!(
            "creating broadcast stream with {} incoming connections",
            current.len()
        );

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
    use super::*;
    use crate::broadcast::Broadcast;
    use crate::test::*;
    use crate::System;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use drop::crypto::key::exchange::Exchanger;
    use drop::net::TcpConnector;

    use tracing::debug_span;
    use tracing_futures::Instrument;

    #[tokio::test]
    async fn single_shot() {
        init_logger();
        let tcp = TcpConnector::new(Exchanger::random());
        let mut addrs = test_addrs(10);
        let mut public =
            create_receivers(addrs.clone().into_iter(), |mut connection| {
                let addr = connection.peer_addr().unwrap();
                async move {
                    let data: usize =
                        connection.receive().await.expect("recv failed");
                    assert_eq!(0, data, "wrong data received");
                }
                .instrument(debug_span!("client", dest=%addr))
            })
            .await;
        let pkeys = public.iter().map(|x| x.0).collect::<Vec<_>>();
        let handles = public.drain(..).map(|x| x.1);
        let candidates = pkeys.into_iter().zip(addrs.drain(..).map(|x| x.1));

        let system: System<usize> =
            System::new_with_connector_zipped(&tcp, candidates).await;
        let mut broadcast = BestEffort::new(system);

        broadcast
            .broadcast(&0usize)
            .await
            .expect("failed to broadcast");

        future::join_all(handles.into_iter())
            .await
            .drain(..)
            .collect::<Result<Vec<_>, _>>()
            .expect("receiver failure");
    }

    #[tokio::test]
    async fn incoming_message() {
        init_logger();
        static DATA: AtomicUsize = AtomicUsize::new(0);
        let tcp = TcpConnector::new(Exchanger::random());
        let mut addrs = test_addrs(10);
        let mut public = create_receivers(
            addrs.clone().into_iter(),
            |mut connection| async move {
                let data = DATA.fetch_add(1, Ordering::AcqRel);

                connection.send(&data).await.expect("recv failed");

                connection.close().await.expect("close failed");
            },
        )
        .await;
        let pkeys = public.iter().map(|x| x.0).collect::<Vec<_>>();
        let handles = public.drain(..).map(|x| x.1);
        let candidates = pkeys.into_iter().zip(addrs.drain(..).map(|x| x.1));

        let system: System<usize> =
            System::new_with_connector_zipped(&tcp, candidates).await;

        let mut broadcast = BestEffort::new(system);

        let mut incoming = broadcast.incoming().await;

        for _ in 0..10usize {
            let (_, data) = incoming.next().await.expect("early eof");
            assert!((0..10usize).contains(&data), "invalid data broadcasted");
        }

        future::join_all(handles)
            .await
            .iter()
            .for_each(|x| assert!(x.is_ok()));
    }
}
