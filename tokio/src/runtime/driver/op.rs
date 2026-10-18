use crate::io::uring::Uring;
use crate::loom::sync::Arc;
use crate::runtime::{context, Handle};
use crate::task::coop;

use io_uring::cqueue;
use io_uring::squeue::Entry;
use std::future::Future;
use std::io;
use std::mem;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

// NOTE/TODO: no loom cover for SQE publication race
mod completion;
pub(crate) use completion::Completable;
use completion::CompletionState;

enum State<T: Completable> {
    Initialize {
        entry: Entry,
        data: T,
    },
    Unsubmitted {
        entry: Entry,
        completion: Arc<CompletionState<T>>,
    },
    // NOTE: fatal ring failure can leave this pending forever. Read/write outputs
    // return the fd and buffer, so an error alone can't complete them before kernel
    // access ends. An early error would need an output that can omit those ressources.
    // Would you be OK with that retention for this PR, or do we need the output
    // change first? (didn't do it because it would make the PR even bigger)
    Submitted(Arc<CompletionState<T>>),
    Complete,
}

pub(crate) struct Op<T: Completable> {
    uring: Uring,
    state: State<T>,
}

impl<T: Completable> Op<T> {
    /// # Safety
    ///
    /// The entry can only point at ressources that `data` keeps alive until completion.
    /// Moving `data` must not invalidate those pointers. The entry must produce
    /// exactly one CQE, so no `multishot` and no `SKIP_SUCCESS`. its result must be
    /// nonnegative or a -errno, as for builtin filesystem operations.
    pub(crate) unsafe fn new(entry: Entry, data: T) -> Self {
        Self {
            uring: Uring::from_io(Handle::current().inner.driver().io()),
            state: State::Initialize { entry, data },
        }
    }
}

unsafe fn op_complete<T: Completable>(_: &Uring, cqe: cqueue::Entry, state: *mut ()) {
    // SAFETY: one CQE takes back the callback's Arc. Rejected submission instead
    // reclaims it in poll and cannot call us. The CQE ends kernel access to data.
    unsafe {
        CompletionState::complete(
            Arc::from_raw(state.cast::<CompletionState<T>>()),
            cqe.result(),
        )
    };
}

impl<T: Completable> Unpin for Op<T> {}

impl<T: Completable> Future for Op<T> {
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        ready!(crate::trace::trace_leaf(cx));
        let coop = ready!(coop::poll_proceed(cx));
        if matches!(this.state, State::Initialize { .. }) {
            let State::Initialize { entry, data } = mem::replace(&mut this.state, State::Complete)
            else {
                unreachable!();
            };
            this.state = State::Unsubmitted {
                entry,
                completion: Arc::new(CompletionState::new(data)),
            };
        }
        if let State::Unsubmitted { completion, .. } = &this.state {
            // register before publication so early completion wake the first Pending.
            completion.waker.register_by_ref(cx.waker());
            let State::Unsubmitted { entry, completion } =
                mem::replace(&mut this.state, State::Complete)
            else {
                unreachable!();
            };
            let state = Arc::into_raw(completion.clone());
            // mark submitted before publication. If submit unwinds, repoll must
            // not publish these resources again. Only proven rejection allows retry.
            this.state = State::Submitted(completion);
            // dropping the future abandons its result. The callback keeps the
            // data even if the future is forgotten. Cancel-on-drop needs a separate
            // SQE and still cannot release resources before the target CQE.
            // SAFETY: new's contract keeps SQE resources in completion. Only Err allow
            // reclaiming the callback Arc. On Ok the callback may already have run.
            if let Err(error) = unsafe {
                this.uring
                    .submit(entry.clone(), op_complete::<T>, state.cast_mut().cast())
            } {
                // SAFETY: rejection guarantees no kernel use and no callback.
                drop(unsafe { Arc::from_raw(state) });
                let State::Submitted(completion) = mem::replace(&mut this.state, State::Complete)
                else {
                    unreachable!();
                };
                if error.kind() == io::ErrorKind::WouldBlock {
                    this.state = State::Unsubmitted { entry, completion };
                    // NOTE: pressure retries still repoll without capacity changing.
                    // Waiters need notifications on SQ consumption, slot release and
                    // failure/shutdown. Required before merge, or a separate change?
                    context::defer(cx.waker());
                    return Poll::Pending;
                }
                coop.made_progress();
                // SAFETY: rejection guarantees no kernel use or callback. Keep completion
                // alive until the output exists, before dropping its possibly panicking waker.
                return Poll::Ready(unsafe { completion.take_data() }.complete(Err(error)));
            }
            // Keep the first-poll yield, including completion before submit returns.
            return Poll::Pending;
        }
        let State::Submitted(completion) = &this.state else {
            panic!("Future polled after completion");
        };
        let result = completion.result().or_else(|| {
            completion.waker.register_by_ref(cx.waker());
            completion.result()
        });
        let Some(result) = result else {
            return Poll::Pending;
        };
        let State::Submitted(completion) = mem::replace(&mut this.state, State::Complete) else {
            unreachable!();
        };
        coop.made_progress();
        // SAFETY: this future alone takes data, after acquiring the terminal result.
        // Construct the output before completion drops, so a waker drop panic cannot
        // lose a successful open's raw fd.
        Poll::Ready(unsafe { completion.take_data() }.complete(result))
    }
}

impl<T: Completable> Drop for Op<T> {
    fn drop(&mut self) {
        // A stranded callback shouldnt keep the tasks waker.
        let waker = match &self.state {
            State::Unsubmitted { completion, .. } | State::Submitted(completion) => {
                completion.waker.take_waker()
            }
            _ => None,
        };
        // If this is the last Arc, finish cleanup before the waker can panic.
        drop(mem::replace(&mut self.state, State::Complete));
        drop(waker);
    }
}
