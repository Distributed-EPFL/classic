use classic_derive::message;

use serde::{Deserialize, Serialize};

pub trait Message {}

#[message]
struct T(u8, u16, u32);

#[message]
struct Gen<T> {
    other: T,
}

#[test]
fn eq() {
    let t1 = T(0, 1, 2);
    let t2 = T(0, 1, 2);

    assert_eq!(t1, t2);
}

#[test]
fn generics() {
    let g1 = Gen { other: 182usize };
    let g2 = Gen { other: 182usize };

    assert_eq!(g1, g2);
}
