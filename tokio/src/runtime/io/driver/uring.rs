use crate::io::Interest;
use crate::loom::sync::atomic::{AtomicU32, Ordering};
use crate::loom::sync::{Arc, Mutex, MutexGuard};
use crate::runtime::driver::op::{CancelData, Cancellable, CqeResult, Lifecycle};
use io_uring::{cqueue, opcode, squeue::Entry, IoUring, Probe};
use mio::unix::SourceFd;
use slab::Slab;
use std::any::Any;
use std::io;
use std::mem;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::OnceLock;
use std::task::{Poll, Waker};
use std::time::Duration;

use super::{Handle, TOKEN_WAKEUP};

const DEFAULT_RING_SIZE: u32 = 256;
// Bound all CQEs, including cancel replies, so refill loops can't starve tasks.
const COMPLETION_BATCH_SIZE: usize = 32;
const SHUTDOWN_POLL_TIMEOUT_MS: libc::c_int = 100;
const SHUTDOWN_RETRY_DELAY: Duration = Duration::from_millis(1);
const CANCEL_USER_DATA: u64 = u64::MAX;
const SHUTDOWN_BIT: u32 = 1 << 31;
const ERROR_MASK: u32 = SHUTDOWN_BIT - 1;

/// Outcome of registering an op with the io_uring.
pub(crate) enum RegisterResult {
    /// In-flight at this slab index.
    Submitted(usize),

    /// Submit failed before reaching the kernel; the slot was removed.
    NotPushed(io::Error),

    /// Submit failed after reaching the kernel; the slot is kept and the caller
    /// must cancel it to keep its resources alive until completion.
    Pushed { index: usize, error: io::Error },
}

pub(crate) struct UringShared {
    context: Mutex<UringContext>,
    waker: Arc<mio::Waker>,
    admission: Arc<UringAdmission>,
    probe: OnceLock<Option<Probe>>,
}

impl UringShared {
    pub(super) fn new(waker: Arc<mio::Waker>) -> Self {
        let admission = Arc::new(UringAdmission::new());
        Self {
            context: Mutex::new(UringContext::new(admission.clone())),
            waker,
            admission,
            probe: OnceLock::new(),
        }
    }

    fn unlock(&self, mut context: MutexGuard<'_, UringContext>) {
        let needs_flush = context.needs_flush();
        drop(context);
        if needs_flush {
            // A failed wake can't undo accepted requests, so it must not turn
            // their Ok results into errors.
            let _ = self.waker.wake();
        }
    }
}

fn closed() -> io::Error {
    io::Error::other(crate::util::error::RUNTIME_SHUTTING_DOWN_ERROR)
}

struct UringAdmission {
    state: AtomicU32,
}

impl UringAdmission {
    fn new() -> Self {
        Self {
            state: AtomicU32::new(0),
        }
    }

    fn check(&self) -> io::Result<()> {
        let state = self.state.load(Ordering::Acquire);
        if state & SHUTDOWN_BIT != 0 {
            Err(closed())
        } else if state != 0 {
            Err(io::Error::from_raw_os_error(state as i32))
        } else {
            Ok(())
        }
    }

    fn failed(&self) -> bool {
        self.state.load(Ordering::Acquire) & ERROR_MASK != 0
    }

    fn close(&self) {
        self.state.fetch_or(SHUTDOWN_BIT, Ordering::Release);
    }

    fn record_result(&self, result: io::Result<usize>) {
        if let Err(error) = result {
            match error.raw_os_error() {
                // ENOMEM can be transient during kernel setup for this thread.
                Some(libc::EINTR | libc::EBUSY | libc::EAGAIN | libc::ENOMEM) => {}
                error => {
                    self.fail(error.unwrap_or(libc::EIO));
                }
            }
        }
    }

    fn fail(&self, error: i32) {
        debug_assert!(error > 0);
        // Writers have exclusive access to the context.
        let state = self.state.load(Ordering::Relaxed);
        if state & ERROR_MASK == 0 {
            self.state.store(state | error as u32, Ordering::Release);
        }
    }
}

fn release_cancelled(data: CancelData, cqe: cqueue::Entry) {
    let _fd = match (&data, CqeResult::from(cqe).result) {
        // SAFETY: this successful Open CQE yields a new file descriptor.
        (CancelData::Open(_), Ok(fd)) => Some(unsafe { OwnedFd::from_raw_fd(fd as i32) }),
        _ => None,
    };
    drop(data);
}

struct CompletionBatch {
    cancelled: [Option<(CancelData, cqueue::Entry)>; COMPLETION_BATCH_SIZE],
    wakers: Vec<Waker>,
    consumed: usize,
}

impl CompletionBatch {
    fn new() -> Self {
        Self {
            cancelled: std::array::from_fn(|_| None),
            wakers: Vec::new(),
            consumed: 0,
        }
    }

    // Runs after unlocking. One panic must not skip later cleanup or task wakes.
    fn finish(mut self) -> bool {
        let mut panicked = false;
        for cancelled in &mut self.cancelled[..self.consumed] {
            if let Some((data, cqe)) = cancelled.take() {
                panicked |= catch_panic(|| release_cancelled(data, cqe));
            }
        }
        for waker in self.wakers.drain(..) {
            // ffffff
            panicked |= catch_panic(|| waker.wake());
        }
        !panicked
    }
}

fn catch_panic(f: impl FnOnce()) -> bool {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(()) => false,
        Err(payload) => {
            drop_panic(payload);
            true
        }
    }
}

fn drop_panic(payload: Box<dyn Any + Send>) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
        std::mem::forget(payload);
    }
}

pub(crate) struct UringContext {
    uring: Option<IoUring>,
    ops: Slab<Lifecycle>,
    admission: Arc<UringAdmission>,
}

impl Drop for UringContext {
    fn drop(&mut self) {
        for operation in self.ops.drain() {
            if let Lifecycle::Cancelled(data) = operation {
                // Without a terminal CQE, the kernel may still use this data.
                mem::forget(data);
            }
        }
    }
}

impl UringContext {
    fn new(admission: Arc<UringAdmission>) -> Self {
        Self {
            uring: None,
            ops: Slab::new(),
            admission,
        }
    }

    fn ring(&self) -> &IoUring {
        self.uring.as_ref().expect("io_uring not initialized")
    }

    fn ring_mut(&mut self) -> &mut IoUring {
        self.uring.as_mut().expect("io_uring not initialized")
    }

    // Immediate submission for now. Batching: https://github.com/tokio-rs/tokio/pull/7761.
    fn flush(&mut self) {
        if !self.needs_flush() {
            return;
        }
        let result = self.ring().submit();
        self.admission.record_result(result);
        self.check_cq_loss();
    }

    fn check_cq_loss(&mut self) {
        let Some(ring) = &mut self.uring else {
            return;
        };
        let dropped = ring.completion().overflow();
        if dropped != 0 {
            // Older kernels may only report CQE loss here, without EBADR.
            self.admission.fail(libc::EBADR);
        }
    }

    fn needs_flush(&mut self) -> bool {
        self.check_cq_loss();
        if self.admission.failed() {
            // Another enter could submit rejected SQEs whose resources were freed.
            return false;
        }
        let Some(ring) = &mut self.uring else {
            return false;
        };
        let sq = ring.submission();
        !sq.is_empty() || sq.cq_overflow()
    }

    /// # Safety
    ///
    /// The entry's resources must stay valid until its terminal CQE.
    /// A rejected `operation` drops under the lock, so it must own nothing.
    unsafe fn push(&mut self, entry: Entry, operation: Lifecycle) -> io::Result<usize> {
        self.admission.check()?;
        if self.ring_mut().submission().is_full() {
            self.flush();
            self.admission.check()?;
            if self.ring_mut().submission().is_full() {
                return Err(io::ErrorKind::WouldBlock.into());
            }
        }
        let index = self.ops.insert(operation);
        let entry = entry.user_data(index as u64);
        // SAFETY: the caller guarantees the entry and callback lifetimes.
        unsafe { self.ring_mut().submission().push(&entry) }
            .expect("submission queue capacity checked under the ring lock");
        self.flush();
        if let Err(error) = self.admission.check() {
            // Without SQPOLL, a nonempty refreshed SQ proves this newest SQE wasn't
            // consumed. Failed rings never enter again, so rejecting it is safe.
            // Earlier accepted entries have no such proof.
            if !self.ring_mut().submission().is_empty() {
                self.ops.remove(index);
                return Err(error);
            }
        }
        Ok(index)
    }

    fn completions(&mut self, limit: usize) -> CompletionBatch {
        self.check_cq_loss();
        let mut batch = CompletionBatch::new();
        let Some(ring) = &mut self.uring else {
            return batch;
        };
        let mut cq = ring.completion();
        for cancelled in batch.cancelled.iter_mut().take(limit) {
            let Some(cqe) = cq.next() else {
                break;
            };
            batch.consumed += 1;
            if cqe.user_data() == CANCEL_USER_DATA {
                continue;
            }
            let index = cqe.user_data() as usize;
            match self.ops.get_mut(index) {
                Some(lifecycle @ Lifecycle::Waiting(_)) => {
                    let Lifecycle::Waiting(waker) =
                        mem::replace(lifecycle, Lifecycle::Completed(cqe))
                    else {
                        unreachable!()
                    };
                    batch.wakers.push(waker);
                }
                Some(Lifecycle::Cancelled(_)) => {
                    let Lifecycle::Cancelled(data) = self.ops.remove(index) else {
                        unreachable!()
                    };
                    *cancelled = Some((data, cqe));
                }
                _ => {
                    // Keep the valid completions already collected when a CQE can't be routed.
                    self.admission.fail(libc::EIO);
                    break;
                }
            }
        }
        batch
    }

    fn awaits_cqes(&self) -> bool {
        self.ops
            .iter()
            .any(|(_, lifecycle)| !matches!(lifecycle, Lifecycle::Completed(_)))
    }

    fn needs_turn(&mut self) -> bool {
        self.uring
            .as_mut()
            .is_some_and(|ring| !ring.completion().is_empty())
            || self.needs_flush()
    }
}

impl Handle {
    fn add_uring_source(&self, uringfd: RawFd) -> io::Result<()> {
        let mut source = SourceFd(&uringfd);
        self.registry
            .register(&mut source, TOKEN_WAKEUP, Interest::READABLE.to_mio())
    }

    pub(super) fn dispatch_completions(&self) {
        if self.uring.probe.get().and_then(Option::as_ref).is_none() {
            return;
        }
        let batch;
        let needs_turn = {
            let mut ctx = self.uring.context.lock();
            batch = ctx.completions(COMPLETION_BATCH_SIZE);
            ctx.flush();
            ctx.needs_turn()
        };
        if needs_turn {
            // Remaining CQEs may not produce another I/O notification.
            let _ = self.waker.wake();
        }
        batch.finish();
    }

    pub(super) fn shutdown_uring(&self) {
        let ids = {
            let ctx = self.uring.context.lock();
            // No new registrations may reuse these IDs while shutdown queues cancels.
            ctx.admission.close();
            if ctx.uring.is_none() {
                return;
            }
            ctx.ops
                .iter()
                .filter(|(_, lifecycle)| !matches!(lifecycle, Lifecycle::Completed(_)))
                .map(|(index, _)| index as u64)
                .collect::<Vec<_>>()
        };
        let mut cancels = ids.into_iter().peekable();
        let mut failed_remaining = None;
        loop {
            let batch;
            let made_progress = {
                let mut ctx = self.uring.context.lock();
                ctx.check_cq_loss();
                if ctx.admission.failed() && failed_remaining.is_none() {
                    // Multishot CQEs can keep arriving after failure disables cancellation.
                    failed_remaining = Some(ctx.ring_mut().completion().len());
                }
                batch = ctx.completions(failed_remaining.unwrap_or(COMPLETION_BATCH_SIZE));
                if let Some(remaining) = &mut failed_remaining {
                    *remaining -= batch.consumed;
                }
                let queued = ctx.ring_mut().submission().len();
                ctx.flush();
                let mut made_progress =
                    batch.consumed != 0 || ctx.ring_mut().submission().len() < queued;
                // Submit all targets before their cancels, including SQEs left queued by EINTR.
                if !ctx.admission.failed() && ctx.ring_mut().submission().is_empty() {
                    while let Some(&id) = cancels.peek() {
                        let entry = opcode::AsyncCancel::new(id)
                            .build()
                            .user_data(CANCEL_USER_DATA);
                        // SAFETY: AsyncCancel doesn't point at any memory.
                        if unsafe { ctx.ring_mut().submission().push(&entry) }.is_err() {
                            break;
                        }
                        cancels.next();
                        made_progress = true;
                    }
                    ctx.flush();
                }
                made_progress |= !ctx.ring_mut().completion().is_empty();
                made_progress
            };
            batch.finish();
            if failed_remaining == Some(0) || !self.uring.context.lock().awaits_cqes() {
                break;
            }
            if failed_remaining.is_some() {
                continue;
            }
            let waited = cancels.peek().is_none() && self.wait_for_uring_completions();
            if !made_progress && !waited && !self.uring.admission.failed() {
                std::thread::sleep(SHUTDOWN_RETRY_DELAY);
            }
        }
        self.uring.context.lock().uring = None;
    }

    fn wait_for_uring_completions(&self) -> bool {
        let mut ctx = self.uring.context.lock();
        ctx.check_cq_loss();
        if ctx.admission.failed() || !ctx.ring_mut().submission().is_empty() {
            return false;
        }
        // Admission is closed, so only shutdown uses the ring while the mutex is released.
        debug_assert!(ctx.admission.state.load(Ordering::Acquire) & SHUTDOWN_BIT != 0);
        let mut ring = ctx.uring.take().unwrap();
        drop(ctx);
        let result = ring.submit();
        let result = result.and_then(|submitted| {
            if ring.completion().overflow() != 0 {
                return Err(io::Error::from_raw_os_error(libc::EBADR));
            }
            // TODO recheck doc
            // Older kernels may not wake enter on CQE loss. Timed poll rechecks loss
            // without EXT_ARG. Nonblocking enter runs task work and drains overflow.
            let mut fd = libc::pollfd {
                fd: ring.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: fd is one initialized pollfd and the ring keeps its fd open.
            let result = unsafe { libc::poll(&mut fd, 1, SHUTDOWN_POLL_TIMEOUT_MS) };
            if result < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(submitted)
            }
        });
        let mut ctx = self.uring.context.lock();
        ctx.uring = Some(ring);
        let waited = result.is_ok();
        ctx.admission.record_result(result);
        ctx.check_cq_loss();
        waited
    }

    #[cfg_attr(test, allow(dead_code))]
    pub(crate) fn is_uring_ready(&self, opcode: u8) -> bool {
        self.uring.admission.state.load(Ordering::Acquire) == 0
            && self
                .uring
                .probe
                .get()
                .and_then(|opt| opt.as_ref())
                .is_some_and(|probe| probe.is_supported(opcode))
    }

    #[cfg_attr(test, allow(dead_code))]
    pub(crate) fn is_uring_probed(&self) -> bool {
        self.uring.probe.get().is_some()
    }

    pub(crate) async fn check_and_init(&self, opcode: u8) -> io::Result<bool> {
        self.check_and_init_sync(opcode)
    }

    /// Unsupported rings/opcodes and failed rings return `Ok(false)`.
    /// Shutdown returns the runtime shutdown error. Other setup errors pass through.
    pub(crate) fn check_and_init_sync(&self, opcode: u8) -> io::Result<bool> {
        let probe = match self.uring.probe.get() {
            Some(probe) => probe,
            None => self.init_probe()?,
        };
        let state = self.uring.admission.state.load(Ordering::Acquire);
        if state & SHUTDOWN_BIT != 0 {
            return Err(closed());
        }
        if state & ERROR_MASK != 0 {
            return Ok(false);
        }

        Ok(probe
            .as_ref()
            .is_some_and(|probe| probe.is_supported(opcode)))
    }

    fn init_probe(&self) -> io::Result<&Option<Probe>> {
        let mut guard = self.uring.context.lock();
        if let Some(probe) = self.uring.probe.get() {
            return Ok(probe);
        }
        guard.admission.check()?;
        let mut probe = Probe::new();
        let probe = match self.try_init(&mut guard, &mut probe) {
            Ok(()) => Some(probe),
            Err(e) if e.raw_os_error() == Some(libc::ENOSYS) => None,
            // Cache EPERM for seccomp-restricted hosts: https://github.com/tokio-rs/tokio/issues/7691.
            Err(e) if e.raw_os_error() == Some(libc::EPERM) => None,
            Err(e) => return Err(e),
        };
        // Publish before unlocking so another initializer cannot replace the ring.
        Ok(self.uring.probe.get_or_init(|| probe))
    }

    fn try_init(&self, guard: &mut UringContext, probe: &mut Probe) -> io::Result<()> {
        let uring = IoUring::new(DEFAULT_RING_SIZE)?;
        // NODROP buffers overflowing CQEs, though allocation failure can still lose them.
        if !uring.params().is_feature_nodrop() {
            return Err(io::Error::from_raw_os_error(libc::ENOSYS));
        }
        match uring.submitter().register_probe(probe) {
            Ok(()) => {}
            Err(e) if e.raw_os_error() == Some(libc::EINVAL) => {
                return Err(io::Error::from_raw_os_error(libc::ENOSYS));
            }
            Err(e) => return Err(e),
        }
        self.add_uring_source(uring.as_raw_fd())?;
        guard.uring = Some(uring);
        Ok(())
    }

    /// Register an operation with the io_uring.
    ///
    /// # Safety
    ///
    /// Callers must ensure that parameters of the entry (such as buffer) are valid and will
    /// be valid for the entire duration of the operation, otherwise it may cause memory problems.
    pub(crate) unsafe fn register_op(&self, entry: Entry, waker: Waker) -> RegisterResult {
        let mut ctx = self.uring.context.lock();
        let operation = Lifecycle::Submitted;
        // SAFETY: entry is valid for the entire duration of the operation
        let result = match unsafe { ctx.push(entry, operation) } {
            Ok(index) => {
                ctx.ops[index] = Lifecycle::Waiting(waker);
                match ctx.admission.check() {
                    Ok(()) => RegisterResult::Submitted(index),
                    Err(error) => RegisterResult::Pushed { index, error },
                }
            }
            Err(error) => RegisterResult::NotPushed(error),
        };
        self.uring.unlock(ctx);
        result
    }

    pub(crate) fn poll_op(&self, index: usize, waker: &Waker) -> Poll<cqueue::Entry> {
        // Declared before the guard so a clone always drops unlocked.
        let mut replacement = None;
        loop {
            let mut ctx = self.uring.context.lock();
            let Some(lifecycle) = ctx.ops.get_mut(index) else {
                panic!("Lifecycle must be present");
            };
            match lifecycle {
                Lifecycle::Completed(_) if replacement.is_none() => {
                    let Lifecycle::Completed(cqe) = ctx.ops.remove(index) else {
                        unreachable!()
                    };
                    return Poll::Ready(cqe);
                }
                Lifecycle::Completed(_) => {}
                Lifecycle::Waiting(current) if current.will_wake(waker) => return Poll::Pending,
                Lifecycle::Waiting(current) => {
                    if let Some(replacement) = replacement.take() {
                        let previous = mem::replace(current, replacement);
                        drop(ctx);
                        drop(previous);
                        return Poll::Pending;
                    }
                }
                Lifecycle::Submitted | Lifecycle::Cancelled(_) => {
                    unreachable!("Unexpected lifecycle for a polled op");
                }
            }
            drop(ctx);
            // Wakers can re-enter or panic. Keep any CQE in the slot while cloning/dropping unlocked.
            match replacement.take() {
                Some(stale) => drop(stale),
                None => replacement = Some(waker.clone()),
            }
        }
    }

    pub(crate) fn cancel_op<T: Cancellable>(&self, index: usize, data: Option<T>) {
        let cancel_data = data.expect("Data should be present").cancel();
        let mut ctx = self.uring.context.lock();
        let Some(lifecycle) = ctx.ops.get_mut(index) else {
            panic!("Lifecycle must be present");
        };

        // This Op will be cancelled. Here, we don't remove the lifecycle from the slab to keep
        // uring data alive until the operation completes.

        match mem::replace(lifecycle, Lifecycle::Cancelled(cancel_data)) {
            previous @ (Lifecycle::Submitted | Lifecycle::Waiting(_)) => {
                drop(ctx);
                drop(previous);
            }
            // The driver saw the completion, but it was never polled.
            Lifecycle::Completed(cqe) => {
                let Lifecycle::Cancelled(data) = ctx.ops.remove(index) else {
                    unreachable!()
                };
                drop(ctx);
                release_cancelled(data, cqe);
            }
            prev => {
                drop(ctx);
                panic!("Unexpected state: {prev:?}");
            }
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests;
