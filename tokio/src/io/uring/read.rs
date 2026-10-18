use crate::io::blocking::Buf;
use crate::runtime::driver::op::{Completable, Op};

use io_uring::{opcode, types};
use std::io;
use std::os::fd::AsRawFd;

/// Trait for buffers that can be used with io-uring read operations.
///
/// # Safety
///
/// After `uring_read_prepare`, the returned region must remain valid for
/// exclusive kernel writes of the returned length while the buffer is kept
/// alive and has not been borrowed mutably since that call, even when moved
/// between threads.
pub(crate) unsafe trait ReadBuffer: Send + 'static {
    /// Prepare the buffer for a read operation.
    /// Returns a pointer and length for the io-uring SQE.
    fn uring_read_prepare(&mut self, max_len: usize) -> (*mut u8, u32);

    /// Complete a read of `n` bytes.
    ///
    /// # Safety
    ///
    /// The caller must ensure the kernel wrote exactly `n` bytes
    /// into the buffer at the pointer returned by `uring_read_prepare`.
    unsafe fn uring_read_complete(&mut self, n: u32);
}

// SAFETY: the spare capacity belongs to this Vec and moving it preserves the allocation.
unsafe impl ReadBuffer for Vec<u8> {
    fn uring_read_prepare(&mut self, max_len: usize) -> (*mut u8, u32) {
        let len = u32::try_from(max_len).unwrap_or(u32::MAX);
        assert!(self.spare_capacity_mut().len() >= len as usize);
        let ptr = self.spare_capacity_mut().as_mut_ptr().cast();
        (ptr, len)
    }

    unsafe fn uring_read_complete(&mut self, n: u32) {
        // SAFETY: the kernel wrote `n` bytes into spare capacity starting
        // at the old self.len(), so self.len() + n bytes are now initialized.
        unsafe { self.set_len(self.len() + n as usize) };
    }
}

// SAFETY: Buf prepares its private Vec's spare capacity, which is unaffected by moves.
unsafe impl ReadBuffer for Buf {
    fn uring_read_prepare(&mut self, max_len: usize) -> (*mut u8, u32) {
        self.prepare_uring_read(max_len)
    }

    unsafe fn uring_read_complete(&mut self, n: u32) {
        // SAFETY: caller guarantees kernel wrote exactly n bytes.
        unsafe { self.complete_uring_read(n as usize) };
    }
}

pub(crate) struct Read<B, F> {
    fd: F,
    buf: B,
}

impl<B: ReadBuffer, F: Send + 'static> Completable for Read<B, F> {
    type Output = (io::Result<u32>, F, B);

    fn complete(self, result: io::Result<u32>) -> Self::Output {
        let mut buf = self.buf;
        if let Ok(len) = result {
            // SAFETY: once the CQE arrives the kernel is done with the buffer.
            // `len` is how many bytes it actually wrote.
            unsafe { buf.uring_read_complete(len) };
        }
        (result, self.fd, buf)
    }
}

impl<B: ReadBuffer, F: AsRawFd + Send + 'static> Op<Read<B, F>> {
    /// Submit a read operation via io-uring.
    ///
    /// `max_len` is the maximum number of bytes to read.
    /// `offset` is the file offset; use `u64::MAX` for the current cursor.
    pub(crate) fn read_at(fd: F, mut buf: B, max_len: usize, offset: u64) -> Self {
        let (ptr, len) = buf.uring_read_prepare(max_len);

        let sqe = opcode::Read::new(types::Fd(fd.as_raw_fd()), ptr, len)
            .offset(offset)
            .build();

        // SAFETY: Read keeps the fd and buffer alive until the CQE, even if the
        // future gets dropped. ReadBuffer guarantees the pointer survives moves
        // and remains valid for exclusive kernel writes until completion.
        unsafe { Op::new(sqe, Read { fd, buf }) }
    }
}
