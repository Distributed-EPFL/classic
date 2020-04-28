use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::net::Ipv4Addr;
use std::pin::Pin;
use std::sync::Arc;

use drop::crypto::key::exchange::PublicKey;
use drop::net::{ConnectError, Connection, Connector, Listener, ListenerError};

use futures::future;
use futures::stream::{select_all, Stream, StreamExt};

use tokio::sync::mpsc;
use tokio::task::{self, JoinHandle};

use tracing::{debug_span, error, info, warn};
use tracing_futures::Instrument;

/// Types that can be turned into a `System`
pub trait System {
    /// Get all the `Connection`s known to this `System`.
    /// The returned `Connection`s will be removed from the system.
    fn connections(&mut self) -> Vec<Connection>;

    /// Get a `Stream` that will produce incoming `Connection`s to the
    /// `System`. The exact behavior will depend on which `System` is used.
    fn peer_source(&mut self)
        -> Pin<Box<dyn Stream<Item = Connection> + Send>>;
}

/// A representation of a distributed `System` that manages connections to and
/// from other peers.
pub struct Basic {
    connections: HashMap<PublicKey, Connection>,
    listeners: Vec<JoinHandle<Result<(), ListenerError>>>,
    _listener_handles: Vec<JoinHandle<Result<(), ListenerError>>>,
    peer_input: Vec<mpsc::Receiver<Connection>>,
}

impl Basic {
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
        .map(|(pkey, connection)| (pkey, connection))
        .collect::<HashMap<_, _>>();

        let mut result = Self::default();

        result.connections = connections;

        result
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

        self.connections.insert(*public, connection);

        Ok(())
    }

    /// Add many peers to this `System` using the provided `Connector`
    pub async fn add_peers<CD, C>(
        &mut self,
        connector: &C,
        candidates: &[(CD, PublicKey)],
    ) -> impl Iterator<Item = ConnectError>
    where
        CD: fmt::Display + Send + Sync,
        C: Connector<Candidate = CD>,
    {
        let (ok, err): (Vec<_>, Vec<_>) = connector
            .connect_many(candidates)
            .await
            .drain(..)
            .zip(candidates.iter().map(|x| x.1))
            .map(|(result, pkey)| match result {
                Ok(connection) => {
                    info!("connected to {}", pkey);
                    Ok((pkey, connection))
                }
                Err(e) => {
                    error!("failed to connect to {}: {}", pkey, e);
                    Err(e)
                }
            })
            .partition(Result::is_ok);

        self.connections.extend(ok.into_iter().map(Result::unwrap));

        err.into_iter().map(Result::unwrap_err)
    }

    /// Add a `Listener` to this `System` that will accept incoming peer
    /// `Connection`s
    pub async fn add_listener<C, L>(
        &mut self,
        mut listener: L,
    ) -> impl Stream<Item = ListenerError>
    where
        C: fmt::Display + Sync + Send,
        L: Listener<Candidate = C> + 'static,
    {
        let (mut err_tx, err_rx) = mpsc::channel(1);
        let (mut peer_tx, peer_rx) = mpsc::channel(32);

        let handle =
            task::spawn(async move {
                loop {
                    match listener.accept().await {
                        Err(e) => {
                            if let Err(e) = err_tx.send(e).await {
                                warn!(
                                    "lost error from listener on {}: {}",
                                    listener.local_addr().unwrap_or_else(
                                        || (Ipv4Addr::UNSPECIFIED, 0).into()
                                    ),
                                    e,
                                );
                            }
                        }
                        Ok(connection) => {
                            let _ = peer_tx.send(connection).await;
                        }
                    }
                }
            });

        self.peer_input.push(peer_rx);
        self.listeners.push(handle);

        err_rx
    }
}

impl System for Basic {
    /// Get a `Stream` that produces incoming `Connection`s from all registered
    /// `Listener`s. Subsequent calls to this method will only produces peers
    /// from `Listener`s that have been added *after* the previous call.
    fn peer_source(
        &mut self,
    ) -> Pin<Box<dyn Stream<Item = Connection> + Send>> {
        select_all(self.peer_input.drain(..)).boxed()
    }

    fn connections(&mut self) -> Vec<Connection> {
        self.connections.drain().map(|x| x.1).collect()
    }
}

impl Default for Basic {
    fn default() -> Self {
        Basic {
            connections: Default::default(),
            listeners: Default::default(),
            _listener_handles: Vec::new(),
            peer_input: Vec::new(),
        }
    }
}

/// A permissioned `System` that places restrictions on which peers can join the
/// `System`
pub struct Permissioned<F>
where
    F: Fn(Connection) -> Option<Connection> + Send + Sync,
{
    /// Use this to add peers to the `System`
    pub system: Basic,
    filter: Arc<F>,
}

impl<F> Permissioned<F>
where
    F: Fn(Connection) -> Option<Connection> + Send + Sync,
{
    /// Create a new `Permissioned` `System` that will filter connections
    /// using the provided filter closure.
    pub fn new(filter: F) -> Self {
        Self {
            system: Basic::default(),
            filter: Arc::new(filter),
        }
    }
}

impl<F> System for Permissioned<F>
where
    F: Fn(Connection) -> Option<Connection> + Send + Sync + 'static,
{
    fn peer_source(
        &mut self,
    ) -> Pin<Box<dyn Stream<Item = Connection> + Send>> {
        let filter = self.filter.clone();

        self.system
            .peer_source()
            .filter_map(move |x| {
                let filter = filter.clone();

                async move { filter(x) }
            })
            .boxed()
    }

    fn connections(&mut self) -> Vec<Connection> {
        let filter = self.filter.clone();

        self.system
            .connections()
            .drain(..)
            .filter_map(move |x| (filter)(x))
            .collect()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::*;

    use drop::crypto::key::exchange::Exchanger;
    use drop::net::{TcpConnector, TcpListener};

    use futures::StreamExt;

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
        let mut system: Basic = Default::default();
        let connector = TcpConnector::new(Exchanger::random());
        let errors = system.add_peers(&connector, &candidates).await;

        let mut connections = system.connections();

        assert_eq!(errors.count(), 0, "error connecting to peers");

        future::join_all(connections.iter_mut().map(|x| async move {
            x.send(&0usize).await.expect("send failed");
        }))
        .await;

        future::join_all(receivers.into_iter().map(|(_, handle)| handle)).await;

        assert_eq!(connections.len(), 11, "not all connections opened");
    }

    #[tokio::test]
    async fn add_listener() {
        let mut system = Basic::default();
        let (exchanger, addr) = test_addrs(1).pop().unwrap();
        let pkey = *exchanger.keypair().public();

        let _ = system
            .add_listener(
                TcpListener::new(addr, exchanger)
                    .await
                    .expect("listen failed"),
            )
            .await;

        let exchanger = Exchanger::random();
        let client_pkey = *exchanger.keypair().public();
        let connector = TcpConnector::new(exchanger);

        connector
            .connect(&pkey, &addr)
            .await
            .expect("connect failed");

        assert_eq!(system.peer_input.len(), 1, "listener not added to system");

        let peer = system
            .peer_source()
            .next()
            .await
            .expect("unexpected end of stream");

        assert_eq!(
            peer.remote_key().unwrap(),
            client_pkey,
            "different addresses"
        );
    }
}
