use std::env;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU16, Ordering};

use crate::System;

use drop::crypto::key::exchange::{Exchanger, PublicKey};
use drop::net::{Connection, Listener, TcpConnector, TcpListener};

use futures::future;

use tokio::task::{self, JoinHandle};

use tracing::info;
use tracing_subscriber::FmtSubscriber;

static PORT_OFFSET: AtomicU16 = AtomicU16::new(0);

/// Initialize an asynchronous logger for test environment
pub fn init_logger() {
    if let Some(level) = env::var("RUST_LOG").ok().map(|x| x.parse().ok()) {
        let subscriber =
            FmtSubscriber::builder().with_max_level(level).finish();

        let _ = tracing::subscriber::set_global_default(subscriber);
    }
}

pub fn next_test_ip4() -> SocketAddr {
    (
        Ipv4Addr::LOCALHOST,
        10000 + PORT_OFFSET.fetch_add(1, Ordering::AcqRel),
    )
        .into()
}

pub fn test_addrs(count: usize) -> Vec<(Exchanger, SocketAddr)> {
    (0..count)
        .map(|_| (Exchanger::random(), next_test_ip4()))
        .collect()
}

pub async fn create_receivers<
    I: Iterator<Item = (Exchanger, SocketAddr)>,
    F: Future<Output = ()> + Send + Sync,
    C: Fn(Connection) -> F + Send + Sync + Clone + 'static,
>(
    addrs: I,
    callback: C,
) -> Vec<(PublicKey, JoinHandle<()>)> {
    let mut output = Vec::new();

    for (exchanger, addr) in addrs {
        let pkey = *exchanger.keypair().public();
        let mut listener = TcpListener::new(addr, exchanger)
            .await
            .expect("listen failed");

        let callback = callback.clone();

        let handle = task::spawn(async move {
            let connection = listener.accept().await.expect("accept failed");

            info!("secure connection accepted");

            (callback)(connection).await;
        });

        output.push((pkey, handle));
    }

    output
}

pub async fn create_system<
    C: Fn(Connection) -> F + Clone + Sync + Send + 'static,
    F: Future<Output = ()> + Send + Sync,
>(
    size: usize,
    closure: C,
) -> (Vec<(PublicKey, SocketAddr)>, JoinHandle<()>, System) {
    init_logger();
    let tcp = TcpConnector::new(Exchanger::random());
    let mut addrs = test_addrs(size);
    let mut public =
        create_receivers(addrs.clone().into_iter(), move |connection| {
            (closure)(connection)
        })
        .await;
    let pkeys = public.iter().map(|x| x.0).collect::<Vec<_>>();
    let handle = task::spawn(async move {
        future::join_all(public.drain(..).map(|x| x.1))
            .await
            .drain(..)
            .collect::<Result<Vec<_>, _>>()
            .expect("connection closure failed");
    });
    let candidates_iter = pkeys.into_iter().zip(addrs.drain(..).map(|x| x.1));

    let output = candidates_iter.collect::<Vec<_>>();

    (
        output.clone(),
        handle,
        System::new_with_connector_zipped(&tcp, output).await,
    )
}
