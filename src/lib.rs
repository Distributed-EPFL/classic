#![deny(missing_docs)]
#![recursion_limit = "1024"]

//! This crate provides common abstraction used in distributed systems
//! on top of the lower level `drop` crate.

use std::collections::VecDeque;
use std::fmt;
use std::hash::Hash;

pub use serde::{Deserialize, Serialize};

mod broadcast;
pub use broadcast::*;

mod system;
pub use system::System;

mod connection;
pub use connection::{ConnectionError, ConnectionHandle};

mod manager;
pub use manager::{
    CollectingSender, ConvertSender, Processor, Sender, SenderError,
    SystemManager, WrappingSender,
};

mod sampler;
pub use sampler::Sampler;

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

macro_rules! impl_m {
    ( $($t:ty),* ) => {
        $( impl Message for $t {} )*
    };
}

impl_m!(u8, i8, u16, i16, u32, i32, u64, i64, u128, i128, usize, String);

impl<T: Message> Message for Vec<T> {}
impl<T: Message> Message for VecDeque<T> {}
