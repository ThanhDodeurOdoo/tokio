use crate::loom::cell::UnsafeCell;
use crate::loom::sync::atomic::AtomicU32;
use crate::loom::sync::Arc;
use crate::sync::AtomicWaker;

use std::io;
use std::sync::atomic::Ordering::{Acquire, Release};

/// Convert an operation result into its output after kernel access has ended
/// or submission was rejected.
pub(crate) trait Completable: Send + Sized + 'static {
    type Output;
    fn complete(self, result: io::Result<u32>) -> Self::Output;
}

const PENDING: u32 = i32::MIN as u32;

// NOTE: I hesitated using a oneshot instead of this machinerie but it would need
// separate callback storage. This keeps the data, result and waker in one allocation.
// Is that worth the custom synchronization?
pub(super) struct CompletionState<T: Completable> {
    pub(super) waker: AtomicWaker,
    result: AtomicU32,
    data: UnsafeCell<Option<T>>,
}

// SAFETY: only the future takes data through a shared reference. The last Arc
// drops any remaining data with exclusive access. No shared reference to T escape.
unsafe impl<T: Completable> Sync for CompletionState<T> {}

impl<T: Completable> CompletionState<T> {
    pub(super) fn new(data: T) -> Self {
        Self {
            waker: AtomicWaker::new(),
            result: AtomicU32::new(PENDING),
            data: UnsafeCell::new(Some(data)),
        }
    }

    pub(super) fn result(&self) -> Option<io::Result<u32>> {
        let result = self.result.load(Acquire);
        (result != PENDING).then(|| decode_result(result as i32))
    }

    /// # Safety
    ///
    /// Only the future may take data, once, after observing a result or after
    /// submission was rejected. No published request may still use thé resources.
    pub(super) unsafe fn take_data(&self) -> T {
        // SAFETY: the caller is the sole consumer. Drop needs the last Arc.
        self.data
            .with_mut(|data| unsafe { (*data).take().unwrap() })
    }

    /// # Safety
    ///
    /// Called once by the callback, after kernel access to data has ended.
    /// The result must be >=0 on success or `-errno` on failure.
    pub(super) unsafe fn complete(completion: Arc<Self>, result: i32) {
        completion.result.store(result as u32, Release);
        let waker = completion.waker.take_waker();
        // release our resources before wake. Otherwise a wake panic could
        // run abandoned result cleanup while unwinding and cause a double panic.
        drop(completion);
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<T: Completable> Drop for CompletionState<T> {
    fn drop(&mut self) {
        // SAFETY: the last Arc has exclusive access to data.
        let data = self.data.with_mut(|data| unsafe { (*data).take() });
        if let Some(data) = data {
            if let Some(result) = self.result() {
                // Reclaim compelted opens before the waker's destructor can panic.
                // NOTE: closing an abandoned fd can block the driver. Offloading
                // this would also have to work during shutdown.
                drop(data.complete(result));
            }
            // Unpublished data just drops. An accepted request keeps its raw Arc
            // until it's callback (incl when fatal ring failure strands it)
        }
    }
}

fn decode_result(result: i32) -> io::Result<u32> {
    if result >= 0 {
        Ok(result as u32)
    } else {
        Err(io::Error::from_raw_os_error(-result))
    }
}
