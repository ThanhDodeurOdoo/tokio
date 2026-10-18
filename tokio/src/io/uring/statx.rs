#![cfg(all(
    tokio_unstable,
    feature = "io-uring",
    feature = "rt",
    feature = "fs",
    // libc::statx is only supported on these platforms
    // FIXME: Add musl target env when our minimum supported
    // rust version is 1.93. To clarify, statx support is
    // introduced to musl in 1.25 as mentioned officially here:
    // https://musl.libc.org/releases.html.
    // However, rustup target_env building for *-linux-musl
    // uses 1.25 musl on all *-linux-musl platforms starting
    // in 1.93 stable rust version.
    // https://blog.rust-lang.org/2025/12/05/Updating-musl-1.2.5/
    any(target_env = "gnu", target_os = "android")
))]

use crate::fs::{ArcFd, File};
use crate::io::uring::utils::cstr;
use crate::runtime::driver::op::{Completable, Op};
use io_uring::{opcode, types};
use libc::statx;
use std::ffi::CString;
use std::fmt::{Debug, Formatter};
use std::io;
use std::mem::MaybeUninit;
use std::os::fd::AsRawFd;
use std::path::Path;

pub(crate) struct Metadata(statx);

impl Metadata {
    /// Returns the size of the file, in bytes, this metadata is for.
    pub(crate) fn len(&self) -> u64 {
        self.0.stx_size
    }
}

impl Debug for Metadata {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("Metadata");
        debug.field("len", &self.len());
        debug.finish_non_exhaustive()
    }
}

pub(crate) struct Statx {
    // The SQE points into this path (fd queries pass a static empty path instead).
    _path: Option<CString>,
    // prevent the fd from being closed/reused if the metadata future is dropped
    _fd: Option<ArcFd>,
    // maybe a bit weird but vec avoids Box's aliasing restrictions when
    // statx moves after we take the kernel's pointer.
    buffer: Vec<MaybeUninit<statx>>,
}

impl Completable for Statx {
    type Output = io::Result<Metadata>;

    fn complete(self, result: io::Result<u32>) -> Self::Output {
        // SAFETY: On success, statx has filled in the output buffer.
        // https://man7.org/linux/man-pages/man2/statx.2.html
        result.map(|_| Metadata(unsafe { self.buffer[0].assume_init_read() }))
    }
}

impl Op<Statx> {
    /// Submit a request to retrieve a file's status.
    #[inline]
    fn statx(path: &Path, flags: i32) -> io::Result<Op<Statx>> {
        let path = cstr(path)?;
        let mut buffer = vec![MaybeUninit::<statx>::uninit()];

        let statx_op = opcode::Statx::new(
            types::Fd(libc::AT_FDCWD),
            path.as_ptr(),
            buffer.as_mut_ptr().cast(),
        )
        .flags(flags)
        .mask(libc::STATX_BASIC_STATS | libc::STATX_BTIME)
        .build();

        // SAFETY: Statx keeps the path and output buffer alive until the operation completes.
        Ok(unsafe {
            Op::new(
                statx_op,
                Statx {
                    _path: Some(path),
                    _fd: None,
                    buffer,
                },
            )
        })
    }

    /// Retrieves the metadata information of the given path, following symlinks
    /// if the path provided points to a symlink location.
    #[inline]
    pub(crate) fn metadata(path: &Path) -> io::Result<Op<Statx>> {
        Op::statx(path, libc::AT_STATX_SYNC_AS_STAT)
    }

    /// Retrieves the metadata information of the given file
    pub(crate) fn file_metadata(file: &File) -> Self {
        let fd = file.uring_fd();
        let mut buffer = vec![MaybeUninit::<statx>::uninit()];

        // io-uring was introduced in linux 5.1
        // pass in an empty path instead of null to target the file descriptor
        // status as specified by man:
        // https://man7.org/linux/man-pages/man2/statx.2.html
        let statx_op = opcode::Statx::new(
            types::Fd(fd.as_raw_fd()),
            c"".as_ptr(),
            buffer.as_mut_ptr().cast(),
        )
        .flags(libc::AT_STATX_SYNC_AS_STAT | libc::AT_EMPTY_PATH)
        .mask(libc::STATX_BASIC_STATS | libc::STATX_BTIME)
        .build();

        // SAFETY: Statx keeps the fd and output buffer alive until the operation completes.
        unsafe {
            Op::new(
                statx_op,
                Statx {
                    _path: None,
                    _fd: Some(fd),
                    buffer,
                },
            )
        }
    }

    // TODO: Once `Metadata::from_statx` is stabilized, we can use use this function
    // to enable io-uring support on `tokio::fs::symlink_metadata`.
    // See this PR for more detail: https://github.com/tokio-rs/tokio/pull/8080
    // See `Metadata::from_statx` tracking issue to see progress:
    // https://github.com/rust-lang/rust/issues/156268
    /// Retrieves the metadata information of the given path without following symlinks.
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn symlink_metadata(path: &Path) -> io::Result<Op<Statx>> {
        Op::statx(
            path,
            libc::AT_STATX_SYNC_AS_STAT | libc::AT_SYMLINK_NOFOLLOW,
        )
    }
}
