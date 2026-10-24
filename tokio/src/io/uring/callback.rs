// TODO
// - tests, check loom
// - cleanup comments
// ---- P2
// - slabless
// - fs::read()
use crate::loom::sync::{Arc, Weak};
use crate::runtime::io::UringShared;
use io_uring::{cqueue, opcode, squeue};
use std::io;

/// Handle to a runtime ring
///
/// - A handle and its clones stay bound to one ring. Callbacks receive that
///   ring's handle for follow-ups and cancellation.
///   This should fit future rnig-per-thread goal (#8249) (which ring `new(&Handle)` selects
///   would still need deciding though).
/// - Callbacks run serially on the I/O driver, outside the ring lock
///   (allows same-ring resubmission)
/// - Use the callback's handle for follow-ups. During shutdown there may be
///   no entered runtime, so you cant just use `Handle::current()`
/// - `submit` never calls callbacks directly
///   (avoid recursive dispatch)
/// - Callbacks should return promptly.
///   (a CQE count limit does not bound callback time)
/// - During scheduler I/O turns, `block_in_place` and Tokio's `block_on` panic.
///   Waiting for I/O from this driver can deadlock.
/// - During scheduler I/O turns, callback panics followthe runtime's `unhandled_panic` policy
///   the driver finishes the batch first (with `Ignore` a future can stay pending)
///   During shutdown, callback panics are contained while the remaining callbacks are drained.
#[derive(Clone, Debug)]
pub struct Uring {
    // weak handle so stranded callback state doesn't also keep
    // the context, slab and waker alive.
    // also io-uring isn't supported with loom: fs is disabled there, and looms test Arc dont provide this.
    // need to decide for whether to keep that limit for this PR, explicitly
    // exclude the real driver under loom or support the combined build?
    shared: Weak<UringShared>,
}

/// Identifies one operation on this ring, so it can be cancelled.
/// - values may be reused after the registration is removed
/// - copying an Id does not extend its validity for cancellation
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct OperationId(pub(crate) u64);

impl OperationId {
    /// `user_data` for `AsyncCancel` on the same ring.
    /// Its cancellation lifetime is define by [`Uring::submit`]'s safety contract.
    /// Target's terminal CQE still needed before freeing its resources
    /// (the cancel request has its own completion)
    pub fn user_data(self) -> u64 {
        self.0
    }
}

impl Uring {
    /// Returns a handle to this runtime's ring, initializing it if needed.
    ///
    /// Setup blocks the calling thread. Concurrent callers may wait.
    /// Unsupported results are cached. Other setup errors let later calls retry.
    ///
    /// # Errors
    ///
    ///  - [`io::ErrorKind::Unsupported`] if runtime I/O is disabled or io_uring is unavailable
    ///  - [`io::ErrorKind::Other`] if the runtime has shut down.
    ///  - [`io::Error`] for other setup errors or a failed ring's stored OS error.
    ///    A failed ring is not reinitialized.
    pub fn new(handle: &crate::runtime::Handle) -> io::Result<Self> {
        let driver = handle.inner.driver().io.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "runtime I/O is disabled. Call enable_io on the runtime builder",
            )
        })?;
        let available = driver.check_and_init(opcode::Nop::CODE)?;
        // fs can fall back to blocking io, but custom callers need the stored ring failure.
        // Initialization errors ENOSYS and EPERM still become Unsupported below.
        driver.check_uring_admission()?;
        if !available {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "io_uring is unavailable",
            ));
        }
        Ok(Self::from_io(driver))
    }

    pub(crate) fn from_io(handle: &crate::runtime::io::Handle) -> Self {
        Self {
            shared: Arc::downgrade(&handle.uring),
        }
    }

    /// Submit a SQE with a completion callback
    /// just listing as things come up for now, will format better later
    ///
    /// - `Err` means no kernel use or callback. `Ok` means accepted
    ///   (`io_uring_enter` failure alone is not enough to return `Err`)
    /// - Fatal errors may strand accepted requests and their resources
    ///   (Reclamation needs proof that kernel use has ended)
    /// - `WouldBlock` has no capacity wakeup, yield before retrying
    ///   capacity waiters could avoid repeated polling.
    /// - Caller provide cancellation logic and synchronizes it with completion.
    /// - The registration stays reserved while its callback runs. CQEs with
    ///   `F_MORE` keep it registered. A terminal CQE (without `F_MORE`) removes
    ///   it after the completion batch, even if the callback panics.
    ///   Removing a registration does not free caller state.
    ///
    /// # Safety
    ///
    /// - Keep buffers, fds and state valid through all kernel/callback uses,
    ///   including unwind, cancellation and future drop.
    /// - callbacks may run on another thread before return. Keep `self` alive
    ///   independently of state they can free.
    /// - Kernel reads need initialized bytes and writes need exclusive storage.
    ///   Synchronize state shared across threads or rings
    /// - Use valid `SQEs` for a default ring (64-byte `SQEs`, 16-byte `CQEs`).
    ///   Preserve Tokio's `user_data`.
    /// - Each request needs a terminal CQE, optionally preceded by `F_MORE`.
    ///   (A panic must leave state usable by later `CQEs` and submitted successors)
    /// - Stop publishing requests targeting an ID before its terminal callback
    ///   returns or unwinds. Synchronize this with concurrent users,
    ///   including prepared SQEs and retries. Completion can happen before `submit` returning.
    ///   Already-published requests must not be able to affect a later reuse of that ID.
    ///   Publication order alone is not enough if lookup can be deferred.
    ///   Generations wrap, so keeping an id does not keep it valid.
    /// - No `SKIP_SUCCESS`, `IO_LINK`, `IO_HARDLINK`, `IO_DRAIN`, injected `CQEs`
    ///   or registered resources.
    ///   (`IO_DRAIN` can block the cancellations needed at shutdown)
    pub unsafe fn submit(
        &self,
        entry: squeue::Entry,
        callback: unsafe fn(&Uring, cqueue::Entry, *mut ()),
        state: *mut (),
    ) -> io::Result<OperationId> {
        let shared = self
            .shared
            .upgrade()
            .ok_or_else(|| io::Error::other(crate::util::error::RUNTIME_SHUTTING_DOWN_ERROR))?;
        // SAFETY: if the above is resspected
        unsafe { shared.submit(entry, callback, state) }
    }
}
