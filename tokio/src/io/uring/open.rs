use super::utils::cstr;

use crate::fs::UringOpenOptions;
use crate::runtime::driver::op::{Completable, Op};

use io_uring::{opcode, types};
use std::ffi::CString;
use std::io;
use std::os::fd::{FromRawFd, OwnedFd};
use std::path::Path;

pub(crate) struct Open {
    /// This field will be read by the kernel during the operation, so we
    /// need to ensure it is valid for the entire duration of the operation.
    #[allow(dead_code)]
    path: CString,
}

impl Completable for Open {
    // CompletionState::drop also completes abandoned opens, so the output
    // only need to own the fd.
    type Output = io::Result<OwnedFd>;
    fn complete(self, result: io::Result<u32>) -> Self::Output {
        result
            // SAFETY: on success, OpenAt gives fd that nothing else owns.
            .map(|fd| unsafe { OwnedFd::from_raw_fd(fd as i32) })
    }
}

impl Op<Open> {
    /// Submit a request to open a file.
    pub(crate) fn open(path: &Path, options: &UringOpenOptions) -> io::Result<Op<Open>> {
        let path = cstr(path)?;

        let custom_flags = options.custom_flags;
        let flags = libc::O_CLOEXEC
            | options.access_mode()?
            | options.creation_mode()?
            | (custom_flags & !libc::O_ACCMODE);

        let open_op = opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), path.as_ptr())
            .flags(flags)
            .mode(options.mode)
            .build();

        // SAFETY: Open keeps the path allocation alive until the CQE arrives.
        Ok(unsafe { Op::new(open_op, Open { path }) })
    }
}

pub(crate) async fn open(path: &Path, options: &UringOpenOptions) -> io::Result<crate::fs::File> {
    loop {
        match Op::open(path, options)?.await {
            Ok(fd) => return Ok(crate::fs::File::from(fd)),
            // Submission EINTR keep the request, so only a CQE can report Interrupted.
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
}
