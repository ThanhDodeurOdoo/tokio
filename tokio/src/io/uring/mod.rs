//! io_uring submission with callbacks.
//!
//! Unstable. Linux, `tokio_unstable`, `rt`, `fs`, `io-uring`.
//! TODO: wire up the driver.

mod callback;
pub use callback::{OperationId, Uring};

pub(crate) mod open;
pub(crate) mod read;
pub(crate) mod rename;
pub(crate) mod statx;
pub(crate) mod utils;
pub(crate) mod write;
