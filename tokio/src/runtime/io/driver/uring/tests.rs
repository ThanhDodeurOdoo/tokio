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
