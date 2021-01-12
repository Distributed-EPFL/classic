use std::ops::Deref;
use std::sync::Arc;

use drop::async_trait;
use drop::crypto::key::exchange::PublicKey;
use drop::system::manager::Handle;
use drop::system::{Message, Processor, Sampler, Sender, SenderError};

use snafu::{OptionExt, ResultExt, Snafu};

use tokio::sync::{mpsc, Mutex};

use tracing::debug;

#[derive(Debug, Snafu)]
/// Errors encountered by `BestEffort`
pub enum BestEffortError {
    #[snafu(display("channel is closed"))]
    /// Channel for delivery is closed
    Channel,
    #[snafu(display("send error: {}", source))]
    /// Network error while broadcasting
    Network {
        /// Underlying network error
        source: SenderError,
    },
    #[snafu(display("handle not setup properly"))]
    /// Handle was not setup properly
    Setup,
}

/// An implementation of best-effort broadcasting
pub struct BestEffort<M> {
    delivery: Mutex<Option<mpsc::Sender<M>>>,
}

impl<M> BestEffort<M>
where
    M: Message,
{
    /// Create a new `BestEffort` broadcast primitive
    pub fn new() -> Self {
        Self {
            delivery: Default::default(),
        }
    }
}

impl<M> Default for BestEffort<M> {
    fn default() -> Self {
        Self {
            delivery: Default::default(),
        }
    }
}

#[async_trait]
impl<M, S> Processor<M, M, M, S> for BestEffort<M>
where
    M: Message + 'static,
    S: Sender<M> + 'static,
{
    type Handle = BestEffortHandle<M, S>;

    type Error = BestEffortError;

    async fn process(
        self: Arc<Self>,
        message: Arc<M>,
        from: PublicKey,
        _: Arc<S>,
    ) -> Result<(), Self::Error> {
        debug!("received {:?} from {}", message, from);

        self.delivery
            .lock()
            .await
            .as_mut()
            .context(Setup)?
            .send(message.deref().clone())
            .await
            .map_err(|_| snafu::NoneError)
            .context(Channel)?;

        Ok(())
    }

    async fn output<SA: Sampler>(
        &mut self,
        _: Arc<SA>,
        sender: Arc<S>,
    ) -> Self::Handle {
        let (delivery_tx, delivery_rx) = mpsc::channel(32);

        self.delivery.lock().await.replace(delivery_tx);

        BestEffortHandle::new(delivery_rx, sender)
    }
}

/// A `Handle` for interacting with a running `BestEffort`
pub struct BestEffortHandle<M, S>
where
    M: Message + 'static,
    S: Sender<M> + 'static,
{
    delivery: mpsc::Receiver<M>,
    sender: Arc<S>,
}

impl<M, S> BestEffortHandle<M, S>
where
    M: Message + 'static,
    S: Sender<M> + 'static,
{
    pub(crate) fn new(delivery: mpsc::Receiver<M>, sender: Arc<S>) -> Self {
        Self { delivery, sender }
    }
}

#[async_trait]
impl<M, S> Handle<M, M> for BestEffortHandle<M, S>
where
    M: Message + 'static,
    S: Sender<M> + 'static,
{
    type Error = BestEffortError;

    async fn deliver(&mut self) -> Result<M, Self::Error> {
        self.delivery.recv().await.context(Channel)
    }

    fn try_deliver(&mut self) -> Result<Option<M>, Self::Error> {
        use tokio::sync::mpsc::error::TryRecvError;

        match self.delivery.try_recv() {
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Closed) => Channel.fail(),
            Ok(message) => Ok(Some(message)),
        }
    }

    async fn broadcast(&mut self, message: &M) -> Result<(), Self::Error> {
        let dest = self.sender.keys().await;

        self.sender
            .clone()
            .send_many(Arc::new(message.clone()), dest.iter())
            .await
            .context(Network)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::iter;

    use drop::test::*;

    static SIZE: usize = 50;

    #[tokio::test]
    async fn broadcast() {
        let keys: Vec<_> = keyset(SIZE).collect();
        let manager = DummyManager::with_key(iter::empty(), keys.clone());
        let sender = manager.sender();

        let besteffort = BestEffort::new();

        let mut handle = manager.run(besteffort).await;

        handle
            .broadcast(&0usize)
            .await
            .expect("failed to broadcast");

        let messages = sender.messages().await;

        assert_eq!(messages.len(), SIZE);
        assert!(messages
            .into_iter()
            .all(|(key, x)| { keys.contains(&key) && x == Arc::new(0usize) }));
    }

    #[tokio::test]
    async fn delivery() {
        let keys: Vec<_> = keyset(SIZE).collect();
        let manager =
            DummyManager::with_key(iter::once((keys[0], 0usize)), keys);

        let sender = manager.sender();

        let besteffort = BestEffort::new();

        let mut handle = manager.run(besteffort).await;

        let message: usize = handle.deliver().await.expect("failed to deliver");

        assert_eq!(message, 0usize, "incorrect message delivered");
        assert!(
            sender.messages().await.is_empty(),
            "sent messages when not supposed to"
        );
    }
}
