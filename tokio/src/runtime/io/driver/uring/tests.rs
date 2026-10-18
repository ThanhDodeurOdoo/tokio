use super::super::{Driver, Handle};
use super::INDEX_MASK;
use crate::io::uring::Uring;

use io_uring::{opcode, Probe};
use std::os::fd::AsRawFd;

// Builds a driver whose io_uring context is initialized, or returns `None` when
// io_uring is unavailable on the host.
fn ready_handle() -> Option<(Driver, Handle)> {
    let (driver, handle) = Driver::new(1024, None).ok()?;

    let mut probe = Probe::new();
    handle.try_init(&mut probe).ok()?;
    handle.uring.probe.set(Some(probe)).unwrap();

    Some((driver, handle))
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

// A enter error can't retire an operation that was already accepted, the newest
// SQE can only be rejected when the refreshed SQ head shows it wasn't consumed.
//
// can it stay a private fault test for now? We need enter itself to fail after
// publication, not a file error coming back in a CQE. if it it a integration
// test would need a way to trigger that failur without exposing the ring just
// for the test.
#[test]
#[cfg_attr(miri, ignore)]
fn publish_failure_preserves_accepted_entry() {
    unsafe fn complete(_: &Uring, _: io_uring::cqueue::Entry, _: *mut ()) {}
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
        assert!(ctx
            .ops
            .contains((accepted.user_data() & INDEX_MASK) as usize));
    }
    drop(broken);
    handle.dispatch_completions();
    let mut ctx = handle.uring.context.lock();
    assert!(ctx.ops.is_empty());
    assert_eq!(ctx.ring_mut().submission().len(), 1);
}
