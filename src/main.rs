// Allows to run
// $ cargo build --features "strict" 
// which will fail on warnings (Useful in CI)
#![cfg_attr(feature = "strict", deny(warnings))]

mod log;

fn main() {
    println!("Hello, world!");
}
