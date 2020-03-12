#![deny(missing_docs)]

//! This crate provides common abstraction used in distributed systems
//! on top of the lower level `drop` crate.

use std::fmt;
use std::hash::Hash;

use serde::{Deserialize, Serialize};

mod broadcast;
pub use broadcast::BestEffort;
pub use broadcast::ReliableMultiShot;

mod connection;
pub use connection::ConnectionManager;
pub use connection::ConnectionStream;

mod system;
pub use system::System;

/// A trait bound for types that can be used as messages
pub trait Message:
    for<'de> Deserialize<'de>
    + Serialize
    + fmt::Debug
    + Send
    + Sync
    + Copy
    + Clone
    + Hash
    + PartialEq
    + Eq
{
}
