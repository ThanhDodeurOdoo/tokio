use crate::runtime::driver::op::{Completable, Op};
use crate::util::as_ref::OwnedBuf;

use io_uring::{opcode, types};
use std::io;
use std::os::fd::{AsRawFd, OwnedFd};

pub(crate) struct Write {
    buf: OwnedBuf,
    fd: OwnedFd,
}

impl Completable for Write {
    type Output = (io::Result<u32>, OwnedFd, OwnedBuf);
    fn complete(self, result: io::Result<u32>) -> Self::Output {
        (result, self.fd, self.buf)
    }
}

impl Op<Write> {
    /// Issue a write that starts at `buf_offset` within `buf` and writes some bytes
    /// into `file` at `file_offset`.
    pub(crate) fn write_at(
        fd: OwnedFd,
        buf: OwnedBuf,
        buf_offset: usize,
        file_offset: u64,
    ) -> Self {
        // There is a cap on how many bytes we can write in a single uring write operation.
        // ref: https://github.com/axboe/liburing/discussions/497
        let len = u32::try_from(buf.as_ref().len() - buf_offset).unwrap_or(u32::MAX);

        let ptr = buf.as_ref()[buf_offset..buf_offset + len as usize].as_ptr();

        let sqe = opcode::Write::new(types::Fd(fd.as_raw_fd()), ptr, len)
            .offset(file_offset)
            .build();

        // SAFETY: Write keeps the fd and buffer alive until the CQE. OwnedBuf's
        // Vec and Bytes keep their bytes at a fixed address across moves.
        unsafe { Op::new(sqe, Write { buf, fd }) }
    }
}
