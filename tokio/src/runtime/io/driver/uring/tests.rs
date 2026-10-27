use super::super::{Driver, Handle};
use crate::io::uring::{CompletionAction, Submission, Uring};

use io_uring::opcode;
use std::os::fd::AsRawFd;

// Builds a driver whose io_uring context is initialized, or returns `None` when
// io_uring is unavailable on the host.
fn ready_handle() -> Option<(Driver, Handle)> {
    let (driver, handle) = Driver::new(1024, None).unwrap();

    handle
        .check_and_init_sync(opcode::Nop::CODE)
        .unwrap()
        .then_some((driver, handle))
}

// Makes the next submission fail with a non-`EBUSY` error by pointing the ring's
// file descriptor at `/dev/null`, then restores it so the driver can shut down.
struct BrokenRing {
    ring_fd: i32,
    saved: i32,
}

impl BrokenRing {
    fn new(handle: &Handle) -> Self {
        let ring_fd = handle.uring.context.lock().ring().as_raw_fd();
        let null = std::fs::File::open("/dev/null").unwrap();
        // SAFETY: `dup` and `dup2` are valid for any open file descriptor.
        let saved = unsafe { libc::dup(ring_fd) };
        assert!(saved >= 0);
        let rc = unsafe { libc::dup2(null.as_raw_fd(), ring_fd) };
        assert_eq!(rc, ring_fd);
        Self { ring_fd, saved }
    }
}

impl Drop for BrokenRing {
    fn drop(&mut self) {
        // SAFETY: `saved` is a live descriptor duplicated from the ring.
        unsafe {
            libc::dup2(self.saved, self.ring_fd);
            libc::close(self.saved);
        }
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn publish_failure_preserves_accepted_entry() {
    unsafe fn complete(
        _: &mut Submission<'_>,
        _: io_uring::cqueue::Entry,
        _: *mut (),
    ) -> CompletionAction {
        CompletionAction::Remove
    }
    let Some((_driver, handle)) = ready_handle() else {
        return;
    };
    let ring = Uring::from_io(&handle);
    // SAFETY: NOP doesn't point at any memory and the callback ignores its state.
    let accepted =
        unsafe { ring.submit(opcode::Nop::new().build(), complete, std::ptr::null_mut()) }.unwrap();
    handle
        .uring
        .context
        .lock()
        .ring()
        .submit_and_wait(1)
        .unwrap();
    let broken = BrokenRing::new(&handle);
    // SAFETY: same as above, this NOP and its callback don't reference anything.
    let rejected =
        unsafe { ring.submit(opcode::Nop::new().build(), complete, std::ptr::null_mut()) };
    assert!(rejected.is_err());
    {
        let ctx = handle.uring.context.lock();
        assert_eq!(ctx.ops.len(), 1);
        assert!(ctx.ops.contains(accepted.user_data() as usize));
    }
    drop(broken);
    handle.dispatch_completions();
    let mut ctx = handle.uring.context.lock();
    assert!(ctx.ops.is_empty());
    assert_eq!(ctx.ring_mut().submission().len(), 1);
}

#[test]
#[cfg_attr(miri, ignore)]
fn callback_resubmits_then_defers_wake() {
    use super::UringShared;
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
    use std::sync::Arc;
    use std::task::{Wake, Waker};

    struct Probe {
        shared: Arc<UringShared>,
        calls: AtomicUsize,
        wakes: AtomicUsize,
    }
    impl Wake for Probe {
        fn wake(self: Arc<Self>) {
            assert!(self.shared.context.try_lock().is_some());
            self.wakes.fetch_add(1, SeqCst);
        }
    }
    unsafe fn complete(
        submission: &mut Submission<'_>,
        cqe: io_uring::cqueue::Entry,
        state: *mut (),
    ) -> CompletionAction {
        // SAFETY: each accepted NOP transfers one Arc to its callback
        let probe = unsafe { Arc::from_raw(state.cast::<Probe>()) };
        assert_eq!(cqe.result(), 0);
        assert!(probe.shared.context.try_lock().is_none());
        if probe.calls.load(SeqCst) == 0 {
            let next = Arc::into_raw(probe.clone());
            // SAFETY: NOP has no kernel pointers and complete takes back its Arc
            let result = unsafe {
                submission.submit(opcode::Nop::new().build(), complete, next.cast_mut().cast())
            };
            if result.is_err() {
                // SAFETY: rejection guarantees no callback or kernel access.
                drop(unsafe { Arc::from_raw(next) });
            }
            result.unwrap();
        } else {
            submission.defer_wake(Waker::from(probe.clone()));
        }
        // Count successes last so a assertion panic inskde still fails the test.
        probe.calls.fetch_add(1, SeqCst);
        CompletionAction::Remove
    }
    let Some((_driver, handle)) = ready_handle() else {
        return;
    };
    let probe = Arc::new(Probe {
        shared: handle.uring.clone(),
        calls: AtomicUsize::new(0),
        wakes: AtomicUsize::new(0),
    });
    let state = Arc::into_raw(probe.clone());
    // SAFETY: NOP has no kernel pointers and complete takes back its Arc.
    let result = unsafe {
        Uring::from_io(&handle).submit(
            opcode::Nop::new().build(),
            complete,
            state.cast_mut().cast(),
        )
    };
    if result.is_err() {
        // SAFETY: rejection guarantees no callback or kernel access
        drop(unsafe { Arc::from_raw(state) });
    }
    result.unwrap();
    for turn in 1..=2 {
        {
            let ctx = handle.uring.context.lock();
            assert_eq!(ctx.ops.len(), 1);
            ctx.ring().submit_and_wait(1).unwrap();
        }
        handle.dispatch_completions();
        assert_eq!(probe.calls.load(SeqCst), turn);
        assert_eq!(probe.wakes.load(SeqCst), turn - 1);
    }
    assert!(handle.uring.context.lock().ops.is_empty());
}

#[test]
#[cfg_attr(miri, ignore)]
fn builtin_and_callback_share_slab() {
    use super::{RegisterResult, UringShared};
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
    use std::sync::Arc;
    use std::task::{Poll, Wake, Waker};

    struct Probe {
        shared: Arc<UringShared>,
        wakes: AtomicUsize,
    }
    impl Wake for Probe {
        fn wake(self: Arc<Self>) {
            assert!(self.shared.context.try_lock().is_some());
            self.wakes.fetch_add(1, SeqCst);
        }
    }
    unsafe fn complete(
        _: &mut Submission<'_>,
        _: io_uring::cqueue::Entry,
        state: *mut (),
    ) -> CompletionAction {
        // SAFETY: state points at the test's counter, which outlives the driver turn.
        unsafe { &*state.cast::<AtomicUsize>() }.fetch_add(1, SeqCst);
        CompletionAction::Remove
    }
    let Some((_driver, handle)) = ready_handle() else {
        return;
    };
    let probe = Arc::new(Probe {
        shared: handle.uring.clone(),
        wakes: AtomicUsize::new(0),
    });
    let waker = Waker::from(probe.clone());
    // SAFETY: NOP doesn't point at any memory.
    let result = unsafe { handle.register_op(opcode::Nop::new().build(), waker.clone()) };
    let RegisterResult::Submitted(builtin) = result else {
        panic!("builtin NOP was not submitted");
    };
    let calls = AtomicUsize::new(0);
    let state = (&calls as *const AtomicUsize).cast_mut().cast();
    // SAFETY: NOP doesn't point at any memory and `calls` outlives its callback.
    unsafe { Uring::from_io(&handle).submit(opcode::Nop::new().build(), complete, state) }.unwrap();
    handle
        .uring
        .context
        .lock()
        .ring()
        .submit_and_wait(2)
        .unwrap();
    handle.dispatch_completions();
    assert_eq!(calls.load(SeqCst), 1);
    assert_eq!(probe.wakes.load(SeqCst), 1);
    handle.shutdown_uring();
    assert!(handle.uring.context.lock().ops.contains(builtin));
    let Poll::Ready(cqe) = handle.poll_op(builtin, &waker) else {
        panic!("completed builtin lost its CQE");
    };
    assert_eq!(cqe.result(), 0);
    assert!(handle.uring.context.lock().ops.is_empty());
}
