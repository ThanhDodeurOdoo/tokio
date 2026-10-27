//! Custom `io_uring` operations with completion callbacks.
//!
//! TODO: rustdoc.

mod handle;
pub use handle::{CompletionAction, OperationId, Submission, Uring};

pub(crate) mod open;
pub(crate) mod read;
pub(crate) mod rename;
pub(crate) mod statx;
pub(crate) mod utils;
pub(crate) mod write;
