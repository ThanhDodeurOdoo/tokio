use super::utils::cstr;

use crate::runtime::driver::op::{Completable, Op};

use io_uring::{opcode, types};
use std::ffi::CString;
use std::io;
use std::path::Path;

pub(crate) struct Rename {
    /// This field will be read by the kernel during the operation, so we
    /// need to ensure it is valid for the entire duration of the operation.
    _from: CString,
    _to: CString,
}

impl Completable for Rename {
    type Output = io::Result<()>;

    fn complete(self, result: io::Result<u32>) -> Self::Output {
        result.map(drop)
    }
}

impl Op<Rename> {
    pub(crate) fn rename(from: &Path, to: &Path) -> io::Result<Op<Rename>> {
        let from = cstr(from)?;
        let to = cstr(to)?;

        let rename_op = opcode::RenameAt::new(
            types::Fd(libc::AT_FDCWD),
            from.as_ptr(),
            types::Fd(libc::AT_FDCWD),
            to.as_ptr(),
        )
        .build();

        // SAFETY: Parameters are valid for the entire duration of the operation
        Ok(unsafe {
            Op::new(
                rename_op,
                Rename {
                    _from: from,
                    _to: to,
                },
            )
        })
    }
}
