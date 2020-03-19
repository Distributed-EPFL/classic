use std::collections::HashMap;
use std::fmt;
use std::future::Future;

use super::connection::ConnectionManager;
use super::Message;

use drop::crypto::key::exchange::PublicKey;
use drop::net::{ConnectError, Connection, Connector, Listener, ListenerError};

use futures::future;

use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tokio::task::{self, JoinHandle};

use tracing::{debug_span, error, info};
use tracing_futures::Instrument;

/// A representation of a distributed `System` that manages connections to and
/// from other peers.
pub struct System<M: Message + 'static> {
    connections: RwLock<HashMap<PublicKey, ConnectionManager<M>>>,
    peer_tx: broadcast::Sender<ConnectionManager<M>>,
    listeners: Vec<JoinHandle<Result<(), ListenerError>>>,
}

impl<M: Message + 'static> System<M> {
    /// Create a new `System` using an `Iterator` over pairs of `PublicKey`s and
    /// `Connection` `Future`s
    pub async fn new<
        I: IntoIterator<Item = (PublicKey, F)>,
        F: Future<Output = Result<Connection, ConnectError>>,
    >(
        initial: I,
    ) -> Self {
        let iter = initial.into_iter();

        let connections = future::join_all(iter.map(|x| async {
            (
                x.1.instrument(debug_span!("system_connect", dest = %x.0))
                    .await,
                x.0,
            )
        }))
        .await
        .drain(..)
        .filter_map(|(result, pkey)| match result {
            Ok(connection) => {
                info!("connected to {}", pkey);
                Some((pkey, connection))
            }
            Err(e) => {
                error!("failed to connect to {}: {}", pkey, e);
                None
            }
        })
        .map(|(pkey, connection)| {
            (pkey, ConnectionManager::new(connection, pkey))
        })
        .collect::<HashMap<_, _>>();

        let (peer_tx, _) = broadcast::channel(32);

        Self {
            connections: RwLock::new(connections),
            listeners: Vec::new(),
            peer_tx,
        }
    }

    /// Create a new `System` using a list of peers and some `Connector`
    pub async fn new_with_connector_zipped<
        C: Connector<Candidate = CD>,
        CD: fmt::Display + Send + Sync,
        I: IntoIterator<Item = (PublicKey, CD)>,
    >(
        connector: &C,
        peers: I,
    ) -> Self {
        Self::new(peers.into_iter().map(|(pkey, candidate)| {
            (
                pkey,
                async move { connector.connect(&pkey, &candidate).await },
            )
        }))
        .await
    }

    /// Create a new `System` from an iterator of `Candidate`s and another of
    /// `PublicKey`s
    pub async fn new_with_connector<
        C: Connector<Candidate = CD>,
        CD: fmt::Display + Send + Sync,
        I1: IntoIterator<Item = PublicKey>,
        I2: IntoIterator<Item = CD>,
    >(
        connector: &C,
        pkeys: I1,
        candidates: I2,
    ) -> Self {
        Self::new_with_connector_zipped(
            connector,
            pkeys.into_iter().zip(candidates),
        )
        .await
    }

    /// Add a new peer into the `System` using the provided `Candidate` and
    /// `Connector`
    pub async fn add_peer<CD, C>(
        &mut self,
        connector: &C,
        candidates: &[CD],
        public: &PublicKey,
    ) -> Result<(), ConnectError>
    where
        CD: fmt::Display + Send + Sync,
        C: Connector<Candidate = CD>,
    {
        let connection = connector.connect_any(public, candidates).await?;
        let manager = ConnectionManager::new(connection, *public);

        let _ = self.peer_tx.send(manager.clone());
        self.connections.write().await.insert(*public, manager);

        Ok(())
    }

    /// Add many peers to this `System` using the provided `Connector`
    pub async fn add_peers<CD, C>(
        &mut self,
        connector: &C,
        candidates: &[(CD, PublicKey)],
    ) -> Vec<ConnectionManager<M>>
    where
        CD: fmt::Display + Send + Sync,
        C: Connector<Candidate = CD>,
    {
        let iter = connector
            .connect_many(candidates)
            .await
            .drain(..)
            .zip(candidates.iter().map(|x| x.1))
            .filter_map(|(result, pkey)| match result {
                Ok(connection) => {
                    info!("connected to {}", pkey);
                    Some((pkey, ConnectionManager::new(connection, pkey)))
                }
                Err(_) => None,
            })
            .collect::<Vec<_>>();

        self.connections.write().await.extend(iter.iter().cloned());

        iter.into_iter().map(|x| x.1).collect()
    }

    /// Get a list of all `Connection`s currently established
    pub async fn connections(&self) -> Vec<ConnectionManager<M>> {
        self.connections.write().await.values().cloned().collect()
    }

    /// Add a `Listener` to this `System` that will accept incoming peer
    /// `Connection`s
    pub async fn add_listener<C, L>(
        &mut self,
        mut listener: L,
    ) -> mpsc::Receiver<ListenerError>
    where
        C: fmt::Display + Sync + Send,
        L: Listener<Candidate = C> + 'static,
    {
        let tx = self.peer_tx.clone();
        let (mut err_tx, err_rx) = mpsc::channel(1);

        let handle = task::spawn(async move {
            let tx = tx;

            loop {
                match listener.accept().await {
                    Err(e) => {
                        let _ = err_tx.send(e).await;
                    }
                    Ok(_) => todo!("insert incoming connections in the system"),
                }
            }
        });

        self.listeners.push(handle);

        err_rx
    }

    /// Get a `Stream` that will produce new peers as they join the `System`
    pub fn peer_source(&mut self) -> broadcast::Receiver<ConnectionManager<M>> {
        self.peer_tx.subscribe()
    }
}

impl<M: Message> Default for System<M> {
    fn default() -> Self {
        Self {
            peer_tx: broadcast::channel(32).0,
            connections: Default::default(),
            listeners: Default::default(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::*;

    use drop::crypto::key::exchange::Exchanger;
    use drop::net::TcpConnector;

    #[tokio::test]
    async fn connect() {
        todo!()
    }

    #[tokio::test]
    async fn add_peers() {
        init_logger();
        let addrs = test_addrs(11);
        let candidates = addrs
            .clone()
            .into_iter()
            .map(|(exchanger, addr)| (addr, *exchanger.keypair().public()))
            .collect::<Vec<_>>();
        let receivers =
            create_receivers(addrs.into_iter(), |mut connection| async move {
                let data = connection
                    .receive::<usize>()
                    .await
                    .expect("receive failed");

                assert_eq!(data, 0, "wrong data received");
            })
            .await;
        let mut system: System<usize> = Default::default();
        let connector = TcpConnector::new(Exchanger::random());
        let mut connections =
            system.add_peers(&connector, candidates.as_slice()).await;

        future::join_all(connections.iter_mut().map(|x| async move {
            x.send(0usize).await.expect("send failed");
        }))
        .await;

        future::join_all(receivers.into_iter().map(|(_, handle)| handle)).await;

        assert_eq!(connections.len(), 11, "not all connections opened");
    }

    #[tokio::test]
    async fn add_listener() {
        todo!()
    }
}
