//! The executable crate for Basango.
//!
//! A Cargo package can contain both a library crate (`lib.rs`) and a binary
//! crate (`main.rs`). Keeping this file tiny is intentional: the library owns
//! the application logic, while this binary only provides the Tokio runtime and
//! converts an error into a non-zero process exit.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // `basango` refers to the sibling library crate. We do not write
    // `mod lib;`: that would incorrectly make `lib.rs` a child of this binary.
    basango::run_cli().await
}
