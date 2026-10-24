use super::super::{Driver, Handle};
use super::INDEX_MASK;
use crate::io::uring::Uring;

use io_uring::opcode;
use std::os::fd::AsRawFd;

// Builds a driver whose io_uring context is initialized, or returns `None` when
// io_uring is unavailable on the host.
fn ready_handle() -> Option<(Driver, Handle)> {
    let (driver, handle) = Driver::new(1024, None).ok()?;

    handle
        .check_and_init(opcode::Nop::CODE)
        .ok()?
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

#[cfg(panic = "unwind")]
mod panic_tests {
    use super::*;
    use crate::runtime::{Builder, UnhandledPanic};
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    struct Completion {
        calls: Arc<Mutex<Vec<(bool, i32)>>>,
        panic: bool,
    }

    unsafe fn complete(_: &Uring, cqe: io_uring::cqueue::Entry, state: *mut ()) {
        // SAFETY: each NOP owns one Box and make exactly one terminal CQE.
        let state = unsafe { Box::from_raw(state.cast::<Completion>()) };
        state
            .calls
            .lock()
            .unwrap()
            .push((state.panic, cqe.result()));
        if state.panic {
            panic!("completion callback panic");
        }
    }

    fn same_batch_panic(builder: &mut Builder, shutdown: bool) {
        let rt = builder
            .enable_all()
            .unhandled_panic(if shutdown {
                UnhandledPanic::ShutdownRuntime
            } else {
                UnhandledPanic::Ignore
            })
            .build()
            .unwrap();
        match Uring::new(rt.handle()) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::Unsupported => return,
            Err(error) => panic!("io_uring initialization failed: {error}"),
        }
        let handle = rt.handle().inner.driver().io();
        let calls = Arc::new(Mutex::new(Vec::new()));
        {
            // prevent the worker from detaching the first CQE before the second is ready.
            let mut ctx = handle.uring.context.lock();
            for (index, panic) in [true, false].into_iter().enumerate() {
                let state = Box::into_raw(Box::new(Completion {
                    calls: calls.clone(),
                    panic,
                }));
                // SAFETY: NOP has no kernel pointers. The last callback reclaims
                // its Box (including on unwind, without borrowing this test's stack).
                let result =
                    unsafe { ctx.publish(opcode::Nop::new().build(), complete, state.cast()) };
                if result.is_err() {
                    // SAFETY: rejection guarantees no callback or kernel use.
                    drop(unsafe { Box::from_raw(state) });
                }
                result.unwrap();
                // CQE order before publishing the next unrelated request.
                ctx.ring().submit_and_wait(index + 1).unwrap();
            }
            assert_eq!(ctx.ring_mut().completion().len(), 2);
        }
        let result = catch_unwind(AssertUnwindSafe(|| {
            rt.block_on(async {
                crate::time::timeout(Duration::from_secs(5), async {
                    loop {
                        // A callback can finish before the worker remove its slot.
                        if calls.lock().unwrap().len() == 2
                            && handle.uring.context.lock().ops.is_empty()
                        {
                            break;
                        }
                        crate::task::yield_now().await;
                    }
                    if shutdown {
                        std::future::pending::<()>().await;
                    }
                })
                .await
            })
        }));
        if shutdown {
            let panic = result.expect_err("callback panic did not shut down the runtime");
            assert_eq!(
                panic.downcast_ref::<&str>(),
                Some(&"the runtime is configured to shut down on unhandled panic")
            );
        } else {
            result.unwrap().expect("completion batch did not finish");
            rt.block_on(async {});
        }
        assert_eq!(*calls.lock().unwrap(), [(true, 0), (false, 0)]);
        assert!(handle.uring.context.lock().ops.is_empty());
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn same_batch_panic_current_thread_ignore() {
        same_batch_panic(&mut Builder::new_current_thread(), false);
    }

    #[test]
    #[cfg(feature = "rt-multi-thread")]
    #[cfg_attr(miri, ignore)]
    fn same_batch_panic_multi_thread_ignore() {
        same_batch_panic(Builder::new_multi_thread().worker_threads(1), false);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn same_batch_panic_current_thread_shutdown() {
        same_batch_panic(&mut Builder::new_current_thread(), true);
    }
}
