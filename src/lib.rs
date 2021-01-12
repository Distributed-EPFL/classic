#![deny(missing_docs)]
#![recursion_limit = "1024"]

//! This crate provides common abstraction used in distributed systems
//! on top of the lower level `drop` crate.

pub use serde::{Deserialize, Serialize};

/// Provides different broadcast primitives
pub mod broadcast;
