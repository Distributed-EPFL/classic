#![deny(missing_docs)]
#![recursion_limit = "1024"]

//! This crate provides common abstraction used in distributed systems
//! on top of the lower level `drop` crate.

use std::fmt;
use std::hash::Hash;

pub use serde::{Deserialize, Serialize};

mod broadcast;
pub use broadcast::*;

mod system;
pub use system::Basic;
pub use system::Permissioned;
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
    + Clone
    + Hash
    + PartialEq
    + Eq
{
}
