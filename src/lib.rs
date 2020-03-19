#![deny(missing_docs)]
#![recursion_limit = "1024"]

//! This crate provides common abstraction used in distributed systems
//! on top of the lower level `drop` crate.

#[macro_use]
extern crate error_chain;

use std::fmt;
use std::hash::Hash;

pub use serde::{Deserialize, Serialize};

mod broadcast;
pub use broadcast::BestEffort;
pub use broadcast::ReliableMultiShot;

mod connection;
pub use connection::ConnectionManager as Connection;
pub use connection::ConnectionStream;

mod system;
pub use system::System;

#[cfg(test)]
pub mod test;

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
