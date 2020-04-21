use super::{Message, System};

use drop::async_trait;
use drop::crypto::key::exchange::PublicKey;
use drop::net::SendError;

mod besteffort;
pub use besteffort::BestEffort;

mod multishot;
pub use multishot::ReliableMultiShot;

// mod uniform;
// pub use uniform::UniformReliable;

#[async_trait]
pub trait Broadcast<M: Message> {
    /// Broadcast a `Message` providing guarantees according to the actual
    /// implementation of this trait.
    async fn broadcast(&mut self, message: &M) -> Vec<(PublicKey, SendError)>;

    /// Get a the next delivered `Message` from this `Broadcast` instance
    async fn delivered(&mut self, system: &mut System) -> Option<M>;
}

/// This trait is used to broadcast `Message`s using a given `System` providing
/// guarantees depending on the actual implementation of the trait.
#[async_trait]
pub trait Broadcaster<M: Message> {
    /// Broadcast a `Message`  using the given `System`.
    /// Guarantees provided vary according to the actual implementation of
    /// this trait.
    async fn broadcast(&mut self, message: &M) -> Vec<(PublicKey, SendError)>;
}

#[async_trait]
pub trait Deliverer<M: Message> {
    /// Get the next delivered `Message` on the provided `System`
    async fn deliver(&mut self) -> Option<(PublicKey, M)>;
}
