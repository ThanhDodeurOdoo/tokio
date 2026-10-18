use crate::io::uring::{OperationId, Uring};
use crate::io::Interest;
use crate::loom::sync::atomic::{AtomicU32, Ordering};
use crate::loom::sync::{Arc, Mutex};
use crate::runtime::{context, scheduler};
use crate::sync::OnceCell;
use io_uring::{cqueue, opcode, squeue::Entry, IoUring, Probe};
use mio::unix::SourceFd;
use slab::Slab;
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::time::Duration;

use super::{Handle, TOKEN_WAKEUP};

const DEFAULT_RING_SIZE: u32 = 256;
// note: I kept each turn to one batch so refill loops don't starve tasks and
// other I/O. Is 32 a reasonable starting point? one slow callback can still hold
// things up.
const COMPLETION_BATCH_SIZE: usize = 32;
// note: the timed poll lets us notice lost CQEs, but shutdown can still wait
// forever on work that can't be cancelled.
const SHUTDOWN_POLL_TIMEOUT_MS: libc::c_int = 100;
const SHUTDOWN_RETRY_DELAY: Duration = Duration::from_millis(1);
// Shutdown only cares about the target's CQE.
// Generations skip zero even after wrapping, so real IDs cannot collide with this.
const CANCEL_USER_DATA: u64 = 0;
const INDEX_BITS: u32 = 16;
const INDEX_MASK: u64 = (1 << INDEX_BITS) - 1;
const MAX_GENERATION: u64 = u64::MAX >> INDEX_BITS;
const SHUTDOWN_BIT: u32 = 1 << 31;
const ERROR_MASK: u32 = SHUTDOWN_BIT - 1;

fn user_data(generation: u64, index: usize) -> u64 {
    (generation << INDEX_BITS) | index as u64
}

pub(crate) struct UringShared {
    context: Mutex<UringContext>,
    // out of the mutex so waking the driver doesn't need the ring lock
    waker: Arc<mio::Waker>,
    admission: Arc<UringAdmission>,
    probe: OnceCell<Option<Probe>>,
}

impl UringShared {
    pub(super) fn new(waker: Arc<mio::Waker>) -> Self {
        let admission = Arc::new(UringAdmission::new());
        Self {
            context: Mutex::new(UringContext::new(admission.clone())),
            waker,
            admission,
            probe: OnceCell::new(),
        }
    }

    /// # Safety
    ///
    /// The caller must respect `Uring::submit`'s safety requirements.
    pub(crate) unsafe fn submit(
        &self,
        entry: Entry,
        callback: unsafe fn(&Uring, cqueue::Entry, *mut ()),
        state: *mut (),
    ) -> io::Result<OperationId> {
        // Caller enforces supported flags because Entry has no flags getter.
        let mut context = self.context.lock();
        // SAFETY: caller guarantees SQE ressources and callback state lifetimes.
        let result = unsafe { context.publish(entry, callback, state) };
        let needs_flush = context.needs_flush();
        drop(context);
        if needs_flush {
            // a failed wake can't undo an accepted request, so it shouldnt turn
            // Ok into Err and let the caller reclaim its ressources.
            let _ = self.waker.wake();
        }
        result
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
        // Preserve the errno so shutdown won't retry io_uring_enter on a failed ring.
        self.state.fetch_or(SHUTDOWN_BIT, Ordering::Release);
    }

    fn record_result(&self, result: io::Result<usize>) {
        if let Err(error) = result {
            match error.raw_os_error() {
                // note: ENOMEM can be temporary while thé kernel sets up io_uring for this
                // thread. Only shutdown back off stalled retries (there's no deadline).
                Some(libc::EINTR | libc::EBUSY | libc::EAGAIN | libc::ENOMEM) => {}
                error => {
                    self.fail(error.unwrap_or(libc::EIO));
                }
            }
        }
    }

    fn fail(&self, error: i32) {
        debug_assert!(error > 0);
        let error = error as u32;
        let _ = self
            .state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                (state & ERROR_MASK == 0).then_some(state | error)
            });
    }
}

#[derive(Clone, Copy)]
struct Callback {
    function: unsafe fn(&Uring, cqueue::Entry, *mut ()),
    state: *mut (),
}

// SAFETY: `submit`'s contract already require callback state to be usable from
// the driver thread. The driver itself never dereferences the pointer.
unsafe impl Send for Callback {}

struct Slot {
    // the generation here so detached callbacks don't carry it around too
    generation: u64,
    callback: Callback,
}

struct CompletionBatch {
    entries: [Option<(Callback, cqueue::Entry)>; COMPLETION_BATCH_SIZE],
    consumed: usize,
}

impl CompletionBatch {
    fn new() -> Self {
        Self {
            entries: std::array::from_fn(|_| None),
            consumed: 0,
        }
    }

    // Returns true if no callback panicked. All callbacks and terminal-slot
    // removals finish before returning.
    fn run(self, handle: &Handle) -> bool {
        let uring = Uring::from_io(handle);
        let mut panicked = false;
        for (callback, cqe) in self.entries.iter().flatten() {
            // Even after a panic, a later CQE may be needed to reclaim caller state.
            panicked |= catch_panic(|| {
                // SAFETY: submit's caller keeps this state valid until this CQE is handled.
                unsafe { (callback.function)(&uring, cqe.clone(), callback.state) };
            });
        }
        let mut removals = self
            .entries
            .into_iter()
            .flatten()
            .filter(|(_, cqe)| !cqueue::more(cqe.flags()))
            .peekable();
        if removals.peek().is_some() {
            // NOTE: callbacks must finish cancellation bookkeeping before slot reuse.
            let mut ctx = handle.uring.context.lock();
            for (_, cqe) in removals {
                ctx.ops.remove((cqe.user_data() & INDEX_MASK) as usize);
            }
        }
        !panicked
    }
}

fn catch_panic(f: impl FnOnce()) -> bool {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(()) => false,
        Err(payload) => {
            // Dropping the panic payload can panic too .
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
                std::mem::forget(payload);
            }
            true
        }
    }
}

struct UringContext {
    uring: Option<IoUring>,
    // note: i kept the slab so shutdown can enumrate cancellation targets.
    // Direct pointers need fixed-address headers and another tracking or teardown
    // system. kernel cancel-all is an alternative to cosnider.
    ops: Slab<Slot>,
    // Generations wrap to avoid a lifetime submission limit. Callers must retire
    // cancellation authority, including queued cancels, before a id can be reused.
    next_generation: u64,
    admission: Arc<UringAdmission>,
}

impl UringContext {
    fn new(admission: Arc<UringAdmission>) -> Self {
        Self {
            uring: None,
            ops: Slab::new(),
            next_generation: 1,
            admission,
        }
    }

    fn ring(&self) -> &IoUring {
        self.uring.as_ref().expect("io_uring not initialized")
    }

    fn ring_mut(&mut self) -> &mut IoUring {
        self.uring.as_mut().expect("io_uring not initialized")
    }

    // note: flushing immediately for now, batching can be done separately there is already a pr
    // https://github.com/tokio-rs/tokio/pull/7761. Callback follow-ups need
    // submission/wakeup coordination across threads too.
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
            // older kernels may only report loss here, without EBADR
            // we don't know which CQEs were lost, so we can't reclaim there resources
            self.admission.fail(libc::EBADR);
        }
    }

    fn needs_flush(&mut self) -> bool {
        self.check_cq_loss();
        if self.admission.failed() {
            // NOTE: for now a failed ring never calls io_uring_enter again. A normal submit could
            // pick up rejected SQEs whose ressources are already fred, so we only drain
            // CQEs that are already visible. Could io_uring_enter with to_submit = 0 and
            // GETEVENTS recover buffered CQEs in a follow-up? That would need to show
            // that rejected SQEs stay untouched and that draining stay bounded.
            // Even then it woulnd't recover lost CQEs or submit accepted SQEs left queued.
            return false;
        }
        let Some(ring) = &mut self.uring else {
            return false;
        };
        let sq = ring.submission();
        !sq.is_empty() || sq.cq_overflow()
    }

    unsafe fn publish(
        &mut self,
        entry: Entry,
        function: unsafe fn(&Uring, cqueue::Entry, *mut ()),
        state: *mut (),
    ) -> io::Result<OperationId> {
        self.admission.check()?;
        if self.uring.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "io_uring is uninitialized",
            ));
        }
        if self.ring_mut().submission().is_full() {
            self.flush();
            self.admission.check()?;
            if self.ring_mut().submission().is_full() {
                return Err(io::ErrorKind::WouldBlock.into());
            }
        }
        // idle requests shouldn't be able to fill thé slab and lock out cancellation.
        // Cancels still need SQ space and a working ring. Checking the next index isn't
        // enough, since a vacant index can sit below an active cancel.
        if self.ops.len() >= INDEX_MASK as usize
            && entry.get_opcode() != u32::from(opcode::AsyncCancel::CODE)
        {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let generation = self.next_generation;
        let index = self.ops.vacant_key();
        if index as u64 > INDEX_MASK {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let id = OperationId(user_data(generation, index));
        // slot insertion can allocate, so finish it before publishing the SQE.
        self.ops.insert(Slot {
            generation,
            callback: Callback { function, state },
        });
        self.next_generation = generation % MAX_GENERATION + 1;
        // SAFETY: the caller guarantees the entry and callback lifetimes.
        unsafe { self.ring_mut().submission().push(&entry.user_data(id.0)) }
            .expect("submission queue capacity checked under the ring lock");
        self.flush();
        if let Err(error) = self.admission.check() {
            // This is the newest SQE and we still hold the mutex. Without SQPOLL,
            // io_uring_enter consumes SQEs in order, so if the SQ is still nonempty after
            // the refresh, this one wasn't consumed. Rejecting it is only safe because a
            // failed ring never calls io_uring_enter again. Earlier accepted entries don't
            // get that proof, so we keep them.
            // NOTE: SQPOLL would break this reasoning. The kernel could consume this SQE
            // without another io_uring_enter, so we'd need a different way to retire it. We can
            // revisit this if SQPOLL lands?
            if !self.ring_mut().submission().is_empty() {
                self.ops.remove(index);
                return Err(error);
            }
        }
        Ok(id)
    }

    fn completions(&mut self, limit: usize) -> CompletionBatch {
        self.check_cq_loss();
        let mut batch = CompletionBatch::new();
        let Some(ring) = &mut self.uring else {
            return batch;
        };
        let mut cq = ring.completion();
        // A spam of cancel CQEs shouldn't hog a turn, even though we drop them.
        for entry in batch.entries.iter_mut().take(limit) {
            let Some(cqe) = cq.next() else {
                break;
            };
            batch.consumed += 1;
            if cqe.user_data() == CANCEL_USER_DATA {
                continue;
            }
            let index = (cqe.user_data() & INDEX_MASK) as usize;
            let generation = cqe.user_data() >> INDEX_BITS;
            let Some(slot) = self
                .ops
                .get(index)
                .filter(|slot| slot.generation == generation)
            else {
                // We can't trust where this CQE points, but throwing away the batch would
                // also leak the valid completions we already collected.
                self.admission.fail(libc::EIO);
                break;
            };
            *entry = Some((slot.callback, cqe));
        }
        batch
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
        // NOTE: here keeping mio wait avoids rewriting the event loop. Sharing a
        // uring backend with mio could allow avoid routing CQ notifications through epoll
        // and combine submission with waiting, the callback API could stay the same, but
        // it would be a bigger change and this PR is already quite big
        // See https://github.com/tokio-rs/mio/issues/1591.
        let mut source = SourceFd(&uringfd);
        self.registry
            .register(&mut source, TOKEN_WAKEUP, Interest::READABLE.to_mio())
    }

    pub(super) fn dispatch_completions(&self) {
        // callbacks can submit again, including through wakers they invoke.
        // Running them under the ring lock would deadlock.
        let (batch, needs_turn) = {
            let mut ctx = self.uring.context.lock();
            let batch = ctx.completions(COMPLETION_BATCH_SIZE);
            ctx.flush();
            (batch, ctx.needs_turn())
        };
        if needs_turn {
            // We only drain one batch, so compeltions may remain in the queue
            // Those entries may not produce another I/O notification. Wake the driver
            // so it processes another batch and retries any pending flush.
            // A wake error must not skip delivery of the detached callbacks below.
            let _ = self.waker.wake();
        }
        if batch.consumed != 0 && !batch.run(self) {
            // NOTE: i used the task panic policy for callbacks for now. Unwinding
            // out of here could lose the parket driver, and aborting would be harsher
            // than what tasks get. Does sharing their policy make sense here?
            // We finish the batch before applying it, since ShutdownRuntime can drop
            // tasks that are stillwaiting.
            catch_panic(|| {
                // the park path runs under this driver's owning scheduler, so
                // its panic policy applies without another identity check.
                context::with_scheduler(|context| {
                    if let Some(scheduler::Context::CurrentThread(context)) = context {
                        context.unhandled_io_panic();
                    }
                    // Multi-thread only supports Ignore anyway.
                });
            });
        }
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
                .map(|(index, slot)| user_data(slot.generation, index))
                .collect::<Vec<_>>()
        };
        let mut cancels = ids.into_iter().peekable();
        let mut failed_remaining = None;
        loop {
            let (batch, made_progress) = {
                let mut ctx = self.uring.context.lock();
                ctx.check_cq_loss();
                if ctx.admission.failed() && failed_remaining.is_none() {
                    failed_remaining = Some(ctx.ring_mut().completion().len());
                }
                let batch = ctx.completions(failed_remaining.unwrap_or(COMPLETION_BATCH_SIZE));
                if let Some(remaining) = &mut failed_remaining {
                    *remaining -= batch.consumed;
                }
                let queued = ctx.ring_mut().submission().len();
                ctx.flush();
                // An io_uring_enter error can still consume SQEs.
                let mut made_progress =
                    batch.consumed != 0 || ctx.ring_mut().submission().len() < queued;
                // Every target, including SQEs left behind by EINTR, has to reach the
                // kernel before its cancel does.
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
                if ctx.admission.failed() && failed_remaining.is_none() {
                    // once failure disables cancellation, multishot requests can keep
                    // adding CQEs, so waiting for an empty CQ could hang.
                    failed_remaining = Some(ctx.ring_mut().completion().len());
                }
                made_progress |= !ctx.ring_mut().completion().is_empty();
                (batch, made_progress)
            };
            // We're already shutting down and might not have a scheduler in TLS to
            // report panics to.
            batch.run(self);
            if failed_remaining == Some(0) || self.uring.context.lock().ops.is_empty() {
                break;
            }
            if failed_remaining.is_some() {
                continue;
            }
            // Waiting on an idle target before its cancel goes out could hang shutdown.
            let waited = cancels.peek().is_none() && self.wait_for_uring_completions();
            if !made_progress && !waited && !self.uring.admission.failed() {
                // yield_now can still spin. A short sleep throttles stalled
                // retries without delaying turns that make progress.
                std::thread::sleep(SHUTDOWN_RETRY_DELAY);
            }
        }
        // closing the ring doesn't mean the kernel is done with whatever is still
        // pending. Without a terminal CQE we can't reclaim it, so it leaks.
        self.uring.context.lock().uring = None;
    }

    fn wait_for_uring_completions(&self) -> bool {
        let mut ctx = self.uring.context.lock();
        ctx.check_cq_loss();
        // Cancels still queued after EINTR or a short submit might never go out if
        // we wait now.
        if ctx.admission.failed() || !ctx.ring_mut().submission().is_empty() {
            return false;
        }
        // Admission is closed, so shutdown is the only one using the ring now. Callers
        // shouldnt sit behind an uncancellable operation only to get rejected.
        debug_assert!(ctx.admission.state.load(Ordering::Acquire) & SHUTDOWN_BIT != 0);
        let mut ring = ctx.uring.take().unwrap();
        drop(ctx);
        let result = ring.submit();
        let result = result.and_then(|submitted| {
            if ring.completion().overflow() != 0 {
                return Err(io::Error::from_raw_os_error(libc::EBADR));
            }
            // TODO recheck doc
            // older kernels might not wake a blocked io_uring_enter when CQEs get lost.
            // A timed poll lets us recheck for loss without needing EXT_ARG. The
            // nonblocking io_uring_enter above still runs task work and flush CQ overflow.
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
        // A successful poll already waited or found CQ work. Errors may skip it entirely.
        let waited = result.is_ok();
        ctx.admission.record_result(result);
        ctx.check_cq_loss();
        waited
    }

    pub(crate) fn check_uring_admission(&self) -> io::Result<()> {
        self.uring.admission.check()
    }

    /// Check the cached probe for opcode support, without initializing anything.
    /// Returns false if io_uring is unsupported, has failed or is shut down.
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

    /// Returns weather initialization has cached a probe or an unsupported result.
    /// Unexpected initialization errors leave the probe unset so callers can retry.
    #[cfg_attr(test, allow(dead_code))]
    pub(crate) fn is_uring_probed(&self) -> bool {
        self.uring.probe.get().is_some()
    }

    /// Initialize if needed, then check opcode support.
    /// Returns `Ok(false)` if the ring is unsupported or failed, or if the opcode isn't
    /// supported. After shutdown it returns the runtime-shutdown error recognized by
    /// `crate::runtime::is_rt_shutdown_err`. Other unexpected syscall errors are passed through.
    ///
    /// TODO: This would like to be a synchronous function,
    /// but we require `OnceLock::get_or_try_init`.
    /// <https://github.com/rust-lang/rust/issues/109737>
    pub(crate) async fn check_and_init(&self, opcode: u8) -> io::Result<bool> {
        let state = self.uring.admission.state.load(Ordering::Acquire);
        if state & SHUTDOWN_BIT != 0 {
            return Err(closed());
        }
        if state & ERROR_MASK != 0 {
            return Ok(false);
        }
        let probe = self
            .uring
            .probe
            .get_or_try_init(|| async {
                let mut probe = Probe::new();
                match self.try_init(&mut probe) {
                    Ok(()) => Ok(Some(probe)),
                    Err(e) if e.raw_os_error() == Some(libc::ENOSYS) => Ok(None),
                    // EPERM can mean seccomp blocked io_uring. Remember that so later calls
                    // go straight to spawn_blocking.
                    // https://github.com/tokio-rs/tokio/issues/7691
                    Err(e) if e.raw_os_error() == Some(libc::EPERM) => Ok(None),
                    Err(e) => Err(e),
                }
            })
            .await?;

        Ok(probe
            .as_ref()
            .is_some_and(|probe| probe.is_supported(opcode)))
    }

    fn try_init(&self, probe: &mut Probe) -> io::Result<()> {
        let mut guard = self.uring.context.lock();
        guard.admission.check()?;
        let uring = IoUring::new(DEFAULT_RING_SIZE)?;
        // We can't reclaim resources without their CQEs. with NODROP the kernel buffers
        // overflowing CQEs instead of dropping them. that allocation can still fail though
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
}

#[cfg(all(test, not(loom)))]
mod tests;
