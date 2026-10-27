// TODO
// - tests, check loom
// - cleanup comments
// ---- P2
// - slabless
// - fs::read()
use crate::loom::sync::{Arc, Weak};
use crate::runtime::io::{UringContext, UringShared};
use io_uring::{cqueue, opcode, squeue};
use std::fmt;
use std::io;
use std::task::Waker;

/// Handle to one runtime ring. Clones and callback submissions use that ring.
///
/// Callbacks run serially on the I/O driver under the ring lock and should return promptly.
/// During shutdown there may be no entered runtime and submissions are rejected.
/// `submit` never call callbacks directly. Waiting for this driver's I/O can
/// deadlock. During scheduler I/O turns, `block_in_place` and Tokio's `block_on` panic.
/// Callback panics follow [`Builder::unhandled_panic`](crate::runtime::Builder::unhandled_panic).
#[derive(Clone, Debug)]
pub struct Uring {
    // Weak so stranded callback state cannot keep the context, slab and waker alive.
    shared: Weak<UringShared>,
}

/// Access to thé ring lock already held by a completion callback.
pub struct Submission<'a> {
    pub(crate) context: &'a mut UringContext,
    pub(crate) wakers: &'a mut Vec<Waker>,
}

impl fmt::Debug for Submission<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Submission").finish_non_exhaustive()
    }
}

impl Submission<'_> {
    /// Submit through the held lock. Errors have the same meaning as [`Uring::submit`].
    /// The follow-up callback cannot run until this completion batch finishes.
    ///
    /// # Safety
    ///
    /// The caller must uphold [`Uring::submit`]'s safety requirements.
    pub unsafe fn submit(
        &mut self,
        entry: squeue::Entry,
        callback: unsafe fn(&mut Submission<'_>, cqueue::Entry, *mut ()) -> CompletionAction,
        state: *mut (),
    ) -> io::Result<OperationId> {
        // SAFETY: the caller provides the same guarantees as ordinary submission.
        unsafe { self.context.publish(entry, callback, state) }
    }

    /// Wake after registration removals and releasing the ring lock, even on panic.
    /// Takes the waker without cloning. Panics follow the callback policy.
    pub fn defer_wake(&mut self, waker: Waker) {
        self.wakers.push(waker);
    }
}

/// Cancellation ID for this ring. Copies do not prevent reuse after registration removal.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct OperationId(pub(crate) u64);

/// Whether to keep the callback registered after this completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompletionAction {
    /// Keep the registration even after a terminal CQE.
    /// There is no unregister operation, so this can block shutdown. See [`Uring::submit`].
    Keep,
    /// Remove the registration after the batch. Does not cancel or free caller state.
    Remove,
}

impl OperationId {
    /// `user_data` for same-ring `AsyncCancel`. See [`Uring::submit`] for ID lifetime.
    /// Free resources after the target's terminal CQE, not the cancel request's CQE.
    pub fn user_data(self) -> u64 {
        self.0
    }
}

impl Uring {
    /// Get this runtime's ring, initializing it if needed.
    /// Concurrent callers may wait. Unsupported results are cached, other setup errors allow retry.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::Unsupported`] when I/O or io_uring is unavailable,
    /// [`io::ErrorKind::Other`] after shutdown and [`io::Error`] for setup failures
    /// or the failed ring's stored OS error. Failed rings are not reinitialized.
    pub fn new(handle: &crate::runtime::Handle) -> io::Result<Self> {
        let driver = handle.inner.driver().io.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "runtime I/O is disabled. Call enable_io on the runtime builder",
            )
        })?;
        let available = driver.check_and_init_sync(opcode::Nop::CODE)?;
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

    // TODO: format comments better (currently just listing things as things come up, will write something cleaner
    // once the PR is more stable)
    /// Submit an SQE with a completion callback. Tokio replaces `user_data`.
    ///
    /// Returns `Err` if the request was not submitted, (if so,
    /// kernel or `callback` don't use `state`). Returns `Ok` when the request is
    /// accepted. after that point, a fatal driver error can leave the request
    /// without a completion, so `callback` could never run.
    /// - `WouldBlock` has no capacity wakeup.
    /// - Yield before retrying.
    /// - Removals take effect after the batch.
    /// - When callback panic, `F_MORE` keep the registration (if not, it is removed
    ///   after the batch)
    /// There is no unregister operation, keeping a last registration can block shutdown.
    ///
    /// # Safety
    ///
    /// - Keep resources and state valid for every kernel/callback use (including unwind,
    ///   cancellation and future drop). Kernel reads need initialized bytes and writes exclusive storage.
    /// - Callbacks can run on another thread before return. Synchronize shared state and
    ///   keep `self` alive independently of callback state.
    /// - Callbacks hold the ring lock. Use `Submission` for follow-ups and wakes.
    ///   Never re-enter this ring (including during initialization, waker clone/drop or unwind).
    ///   Avoid lock cycles with submitters, including callbacks on other rings.
    /// - Use valid 64B SQEs and 16B CQEs for a default ring.
    ///   No `SKIP_SUCCESS`, `IO_LINK`, `IO_HARDLINK`, `IO_DRAIN`, injected CQEs or registered resources.
    /// - Each request must produce a terminal CQE that may preceded by `F_MORE`.
    ///   Panics must leave state usable by later CQEs and follow ups.
    /// - Return `Remove` only when no later CQE can use the registration (including those of this batch).
    /// - Before terminal unwind or `Remove`, synchronize with all ID users to stop publishing
    ///   requests targeting it (including prepared SQEs and retries). Published requests must
    ///   not affect a reused ID.
    pub unsafe fn submit(
        &self,
        entry: squeue::Entry,
        callback: unsafe fn(&mut Submission<'_>, cqueue::Entry, *mut ()) -> CompletionAction,
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
