// note: callbacks let adapters chain I/O without going through a future poll.
// Ownership and cancellation are left to the adapters, so the primitive doesn't
// force a policy for either. I kept the fs work later to keep this PR small.
// reused io-uring's SQE/CQE types to avoid wrappers, witch ties callers to
// our io-uring version (is that ok while the API is unstable?)
mod callback;
pub use callback::{OperationId, Uring};

pub(crate) mod open;
pub(crate) mod read;
pub(crate) mod rename;
pub(crate) mod statx;
pub(crate) mod utils;
pub(crate) mod write;
