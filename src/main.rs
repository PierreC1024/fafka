// Allows to run
// $ cargo build --features "strict"
// which will fail on warnings (Useful in CI)
#![cfg_attr(feature = "strict", deny(warnings))]
#![feature(test)]
#![allow(dead_code)] // Temporary
extern crate test;

mod store;

fn main() {
    println!("Hello, world!");
}
