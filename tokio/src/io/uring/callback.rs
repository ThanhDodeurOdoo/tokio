use crate::loom::sync::{Arc, Mutex};
use crate::runtime::io::UringContext;
use io_uring::{cqueue, opcode, squeue};
use std::io;
use std::sync::Weak;

/// Weak handle to a runtime's ring
///
/// - it dsoes not keep the ring alive
/// - Callbacks run serially on the I/O driver, outside the ring lock
///   (allows same-ring resubmission)
/// - `submit` never calls callbacks directly
///   (avoid recursive dispatch)
/// - Callbacks should return promptly.
///   (a CQE count limit does not bound callback time)
/// - Callback panics followthe runtime's `unhandled_panic` policy
///   (cntainment protects the driver, but `Ignore` can lead a future pending)
#[derive(Clone, Debug)]
pub struct Uring {
    context: Weak<Mutex<UringContext>>,
    waker: Arc<mio::Waker>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct OperationId(pub(crate) u64);

impl OperationId {
    /// `user_data` for `AsyncCancel` on the same ring.
    /// Target's terminal CQE still needed before freeing its resources
    /// (the cancel request has its own completion)
    pub fn user_data(self) -> u64 {
        self.0
    }
}

impl Uring {

    pub async fn new(handle: &crate::runtime::Handle) -> io::Result<Self> {
        let driver =
            handle.inner.driver().io.as_ref().ok_or_else(|| {
                io::Error::new(io::ErrorKind::Unsupported, "runtime I/O is disabled")
            })?;
        let available = driver.check_and_init(opcode::Nop::CODE).await?;
        // Preserve fatal/closed errors before treating fase as unsupported
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
            context: Arc::downgrade(&handle.uring_context),
            waker: handle.waker.clone(),
        }
    }

    /// Submit a SQE with a completion callback
    ///
    /// - `Err` means no kernel use or callback. `Ok` means accepted
    ///   (Enter failure alone is not enough to return `Err`)
    /// - Fatal errors may strand accepted requests and their resources
    ///   (Reclamation needs proof that kernel use has ended)
    /// - `WouldBlock` has no capacity wakeup, yield before retrying
    ///  capacity waiters could avoid repeated polling.
    ///
    /// # TODO: Safety
    ///
    /// - Keep buffers, fds and state valid through all kernel/callback uses,
    ///   including unwind, cancellation and future drop.
    /// - callbacks may run on another thread before return. Keep `self` alive
    ///   independently of state they can free.
    /// - Kernel reads need initialized bytes and writes need exclusive storage.
    ///   Synchronize state shared across threads or rings
    /// - Use valid SQEs for a default ring (64-byte SQEs, 16-byte CQEs).
    ///   Preserve Tokio's `user_data`.
    /// - each request needs a terminal CQE, optionally preecded by `F_MORE`
    ///   (A panic must leave state usable by later CQEs and submitted successors)
    /// - No skip-success, linked SQEs, `IO_DRAIN`, injected CQEs or registered resources.
    ///   (`IO_DRAIN` can block the cancellations needed at shutdown)
    pub unsafe fn submit(
        &self,
        entry: squeue::Entry,
        callback: unsafe fn(&Uring, cqueue::Entry, *mut ()),
        state: *mut (),
    ) -> io::Result<OperationId> {
        // Caller enforces supported flags because Entry has no flags getter.
        let context = self.context.upgrade().ok_or_else(closed)?;
        let mut context = context.lock();
        // SAFETY: caller guarantees SQE resources and callback state lifetimes.
        let result = unsafe { context.publish(entry, callback, state) };
        let needs_flush = context.needs_flush();
        drop(context); // Wake outside the ring lock.
        if needs_flush {
            // Wake queued SQEs without panicking on wake failure after publication.
            let _ = self.waker.wake();
        }
        result
    }
}

pub(crate) fn closed() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "io_uring admission is closed")
}
