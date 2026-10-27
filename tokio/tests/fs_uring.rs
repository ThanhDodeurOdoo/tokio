//! Uring file operations tests.

#![cfg(all(
    tokio_unstable,
    feature = "io-uring",
    feature = "rt",
    feature = "fs",
    target_os = "linux"
))]

use futures::future::FutureExt;
use std::sync::mpsc;
use std::task::Poll;
use std::time::Duration;
use std::{future::poll_fn, path::PathBuf};
use tempfile::NamedTempFile;
use tokio::task::JoinSet;
use tokio::{
    fs::OpenOptions,
    runtime::{Builder, Runtime},
};

fn multi_rt(n: usize) -> Box<dyn Fn() -> Runtime> {
    Box::new(move || {
        Builder::new_multi_thread()
            .worker_threads(n)
            .enable_all()
            .build()
            .unwrap()
    })
}

fn current_rt() -> Box<dyn Fn() -> Runtime> {
    Box::new(|| Builder::new_current_thread().enable_all().build().unwrap())
}

fn rt_combinations() -> Vec<Box<dyn Fn() -> Runtime>> {
    vec![
        current_rt(),
        multi_rt(1),
        multi_rt(2),
        multi_rt(8),
        multi_rt(64),
        multi_rt(256),
    ]
}

#[test]
fn shutdown_runtime_while_performing_io_uring_ops() {
    fn run(rt: Runtime) {
        let (done_tx, done_rx) = mpsc::channel();

        let (_tmp, path) = create_tmp_files(1);
        rt.block_on(async move {
            let path = path[0].clone();
            let mut futs = vec![];

            // spawning a bunch of uring operations.
            for _ in 0..100 {
                let path = path.clone();
                let mut fut = Box::pin(async move {
                    let mut opt = OpenOptions::new();
                    opt.read(true);
                    opt.open(&path).await
                });
                if let Poll::Ready(result) = futures::poll!(fut.as_mut()) {
                    result.unwrap();
                }
                futs.push(fut);
            }
            tokio::spawn(async move {
                let _futs = futs;
                loop {
                    let path = path.clone();
                    tokio::spawn(async move {
                        let mut opt = OpenOptions::new();
                        opt.read(true);
                        opt.open(&path).await.unwrap();
                    });

                    // Avoid busy looping.
                    tokio::task::yield_now().await;
                }
            });
        });

        std::thread::spawn(move || {
            rt.shutdown_timeout(Duration::from_millis(300));
            done_tx.send(()).unwrap();
        });

        done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    }

    for rt in rt_combinations() {
        run(rt());
    }
}

#[test]
fn open_many_files() {
    fn run(rt: Runtime) {
        const NUM_FILES: usize = 512;

        let (_tmp_files, paths): (Vec<NamedTempFile>, Vec<PathBuf>) = create_tmp_files(NUM_FILES);

        rt.block_on(async move {
            let mut tasks = JoinSet::new();

            for i in 0..10_000 {
                let path = paths.get(i % NUM_FILES).unwrap().clone();
                tasks.spawn(async move {
                    let _file = OpenOptions::new().read(true).open(path).await.unwrap();
                });
            }
            tasks.join_all().await;
        });
    }

    for rt in rt_combinations() {
        run(rt());
    }
}

#[cfg(all(feature = "signal", feature = "time"))]
#[tokio::test]
async fn shutdown_retries_interrupted_wait() {
    use std::os::unix::thread::JoinHandleExt;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use tokio::signal::unix::{signal, SignalKind};
    use tokio::time::timeout;

    const TIMEOUT: Duration = Duration::from_secs(10);

    let Ok(ring) = io_uring::IoUring::new(2) else {
        return;
    };
    let mut probe = io_uring::Probe::new();
    if !ring.params().is_feature_nodrop()
        || ring.submitter().register_probe(&mut probe).is_err()
        || !probe.is_supported(io_uring::opcode::OpenAt::CODE)
    {
        return;
    }
    drop(ring);

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fifo");
    nix::unistd::mkfifo(&path, nix::sys::stat::Mode::S_IRWXU).unwrap();
    let mut signal = signal(SignalKind::user_defined1()).unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let open_path = path.clone();
    let completed = Arc::new(AtomicBool::new(false));
    let shutdown_completed = completed.clone();
    let (release_tx, release_rx) = mpsc::channel();
    let shutdown = std::thread::spawn(move || {
        let rt = current_rt()();
        // Initialise io_uring before polling the operation we will cancel.
        rt.block_on(tokio::fs::File::open("/dev/null")).unwrap();
        {
            let _enter = rt.enter();
            // O_CREAT forces io_uring to use its blocking open path for the FIFO.
            let mut open = tokio_test::task::spawn(tokio::fs::File::create(open_path));
            tokio_test::assert_pending!(open.poll());
        }
        // SAFETY: gettid take no arguments and returns this thread's ID.
        tx.send(unsafe { libc::syscall(libc::SYS_gettid) }).unwrap();
        drop(rt);
        shutdown_completed.store(true, Ordering::Release);
        // Keep the signal target alive until the interrupt attempt finishes.
        let _ = release_rx.recv();
    });

    let pthread = shutdown.as_pthread_t();
    let interrupt_result = tokio::spawn(timeout(TIMEOUT, async move {
        let tid = rx.await.unwrap();
        let syscall_path = format!("/proc/self/task/{tid}/syscall");
        let syscall_number = format!("{} ", libc::SYS_poll);
        let wait_for_shutdown_wait = || async {
            while !std::fs::read_to_string(&syscall_path)
                .unwrap()
                .starts_with(&syscall_number)
            {
                if completed.load(Ordering::Acquire) {
                    return false;
                }
                tokio::task::yield_now().await;
            }
            true
        };
        // Cancellation can finish shutdown before it needs to wait.
        if !wait_for_shutdown_wait().await {
            return;
        }
        // SAFETY: the join handle keeps the thread ID valid, and SIGUSR1 has a handler.
        assert_eq!(
            unsafe { libc::pthread_kill(pthread as libc::pthread_t, libc::SIGUSR1) },
            0
        );
        signal.recv().await.unwrap();
        // After the signal, shutdown must either retry the wait or finish cancelling.
        wait_for_shutdown_wait().await;
    }))
    .await;

    // Release the FIFO before propagating failures. O_RDWR never waits for a peer.
    let _reader = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let _ = release_tx.send(());
    timeout(TIMEOUT, async {
        while !shutdown.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("runtime shutdown did not complete");
    shutdown
        .join()
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    interrupt_result.unwrap().unwrap();
}

#[tokio::test]
async fn cancel_op_future() {
    let (_tmp_file, path): (Vec<NamedTempFile>, Vec<PathBuf>) = create_tmp_files(1);

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let handle = tokio::spawn(async move {
        poll_fn(|cx| {
            let opt = {
                let mut opt = tokio::fs::OpenOptions::new();
                opt.read(true);
                opt
            };

            let fut = opt.open(&path[0]);

            // If io_uring is enabled (and not falling back to the thread pool),
            // the first poll should return Pending.
            let _pending = Box::pin(fut).poll_unpin(cx);

            tx.send(()).unwrap();

            Poll::<()>::Pending
        })
        .await;
    });

    // Wait for the first poll
    rx.recv().await.unwrap();

    handle.abort();

    let res = handle.await.unwrap_err();
    assert!(res.is_cancelled());
}

#[cfg(panic = "unwind")]
#[test]
fn completion_waker_panic_preserves_uring() {
    use io_uring::{opcode, IoUring, Probe};
    use std::panic::{catch_unwind, resume_unwind};
    use std::sync::Arc;
    use std::task::{Context, Wake, Waker};
    use tokio::fs::File;

    struct PanicWaker(tokio::sync::mpsc::UnboundedSender<()>);
    impl Wake for PanicWaker {
        fn wake(self: Arc<Self>) {
            self.0.send(()).unwrap();
            panic!("completion waker panic");
        }
    }
    // The blocking fallback also catches waker panics, so it cannot test this path.
    let Ok(ring) = IoUring::new(2) else {
        return;
    };
    let mut probe = Probe::new();
    if !ring.params().is_feature_nodrop()
        || ring.submitter().register_probe(&mut probe).is_err()
        || !probe.is_supported(opcode::OpenAt::CODE)
    {
        return;
    }
    drop(ring);
    let (done, completed) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let result = catch_unwind(|| {
            let rt = Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(tokio::fs::File::open("/dev/null")).unwrap();
            let (panicked, mut notified) = tokio::sync::mpsc::unbounded_channel();
            let mut open_fut = Box::pin(File::open("/dev/null"));
            {
                let _enter = rt.enter();
                let waker = Waker::from(Arc::new(PanicWaker(panicked)));
                let mut cx = Context::from_waker(&waker);
                tokio_test::assert_pending!(open_fut.as_mut().poll_unpin(&mut cx));
            }
            rt.block_on(async {
                notified.recv().await.unwrap();
                open_fut.await.unwrap();
                File::open("/dev/null").await.unwrap();
            });
            rt.block_on(File::open("/dev/null")).unwrap();
            drop(rt);
        });
        let _ = done.send(result);
    });
    let result = completed
        .recv_timeout(Duration::from_secs(5))
        .expect("completion waker panic stalled the runtime");
    worker.join().unwrap();
    if let Err(payload) = result {
        resume_unwind(payload);
    }
}

fn create_tmp_files(num_files: usize) -> (Vec<NamedTempFile>, Vec<PathBuf>) {
    let mut files = Vec::with_capacity(num_files);
    for _ in 0..num_files {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        files.push((tmp, path));
    }

    files.into_iter().unzip()
}
