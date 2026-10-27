use super::super::{Driver, Handle};
use super::RegisterResult;

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

// Reject only the newest SQE proven unconsumed after a fatal enter failure.
#[test]
#[cfg_attr(miri, ignore)]
fn register_failure_preserves_accepted_entry() {
    let Some((_driver, handle)) = ready_handle() else {
        return;
    };
    // SAFETY: NOP does not reference any buffer.
    let accepted =
        unsafe { handle.register_op(opcode::Nop::new().build(), futures::task::noop_waker()) };
    let RegisterResult::Submitted(accepted) = accepted else {
        panic!("first NOP was not submitted");
    };
    handle
        .uring
        .context
        .lock()
        .ring()
        .submit_and_wait(1)
        .unwrap();
    let broken = BrokenRing::new(&handle);
    // SAFETY: NOP does not reference any buffer.
    let rejected =
        unsafe { handle.register_op(opcode::Nop::new().build(), futures::task::noop_waker()) };
    assert!(matches!(rejected, RegisterResult::NotPushed(_)));
    {
        let ctx = handle.uring.context.lock();
        assert_eq!(ctx.ops.len(), 1);
        assert!(ctx.ops.contains(accepted));
    }
    drop(broken);
    handle.dispatch_completions();
    assert!(handle
        .poll_op(accepted, &futures::task::noop_waker())
        .is_ready());
    let mut ctx = handle.uring.context.lock();
    assert!(ctx.ops.is_empty());
    assert_eq!(ctx.ring_mut().submission().len(), 1);
}

// A completed builtin keeps its slot through shutdown until its future takes the CQE.
#[test]
#[cfg_attr(miri, ignore)]
fn completed_builtin_survives_shutdown() {
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
    handle
        .uring
        .context
        .lock()
        .ring()
        .submit_and_wait(1)
        .unwrap();
    handle.dispatch_completions();
    assert_eq!(probe.wakes.load(SeqCst), 1);
    handle.shutdown_uring();
    assert!(handle.uring.context.lock().ops.contains(builtin));
    let Poll::Ready(cqe) = handle.poll_op(builtin, &waker) else {
        panic!("completed builtin lost its CQE");
    };
    assert_eq!(cqe.result(), 0);
    assert!(handle.uring.context.lock().ops.is_empty());
}
