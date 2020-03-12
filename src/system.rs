use std::collections::HashMap;
use std::fmt;
use std::future::Future;

use super::connection::ConnectionManager;

use super::Message;

use drop::crypto::key::exchange::PublicKey;
use drop::net::{ConnectError, Connection, Connector, Listener, ListenerError};

use futures::future;

use tokio::sync::broadcast;
use tokio::sync::RwLock;
use tokio::task::{self, JoinHandle};

use tracing::error;

/// An abstract distributed `System` that performs some user defined task
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
        let mut iter = initial.into_iter();
        let keys = iter.by_ref().map(|x| x.0).collect::<Vec<_>>();
        let connections = future::join_all(iter.map(|x| x.1))
            .await
            .drain(..)
            .zip(keys)
            .filter_map(|(result, pkey)| match result {
                Ok(connection) => Some((pkey, connection)),
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
    pub async fn new_with_connector<
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
    pub async fn add_listener<C, L>(&mut self, mut listener: L)
    where
        C: fmt::Display + Sync + Send,
        L: Listener<Candidate = C> + 'static,
    {
        let tx = self.peer_tx.clone();

        let handle = task::spawn(async move {
            let tx = tx;

            loop {
                match listener.accept().await {
                    Err(e) => return Err(e),
                    Ok(_) => todo!("insert incoming connections in the system"),
                }
            }
        });

        self.listeners.push(handle);
    }

    /// Get a `Stream` that will produce new peers as they join the `System`
    pub fn peer_source(&mut self) -> broadcast::Receiver<ConnectionManager<M>> {
        self.peer_tx.subscribe()
    }
}

pub(crate) enum PeerChange {
    Add(PublicKey),
    Remove(PublicKey),
}

impl<M: Message> From<ConnectionManager<M>> for PeerChange {
    fn from(conn: ConnectionManager<M>) -> Self {
        PeerChange::Add(*conn.public())
    }
}
