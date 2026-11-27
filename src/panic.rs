//! Turning panics into ordinary failures.
//!
//! A panic inside a worker unwinds its task. Without catching it, the task just
//! ends: the supervisor is never told the child died, never restarts it, and
//! keeps listing it as running. In a supervision tree a crash is the main event
//! to handle, so every place user code runs is guarded and a panic is reported
//! as an abnormal exit instead.

use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Wraps a future so a panic while polling it resolves to `Err(message)`.
///
/// After a panic the inner future is in an unknown state, so it is never
/// polled again: the wrapper resolves immediately with the error.
pub(crate) struct CatchUnwind<F>(pub(crate) F);

impl<F: Future + Unpin> Future for CatchUnwind<F> {
    type Output = Result<F::Output, String>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = &mut self.0;
        match std::panic::catch_unwind(AssertUnwindSafe(|| Pin::new(inner).poll(cx))) {
            Ok(Poll::Ready(output)) => Poll::Ready(Ok(output)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => Poll::Ready(Err(panic_message(payload.as_ref()))),
        }
    }
}

/// Calls `f`, turning a panic into `Err(message)`.
pub(crate) fn catch<T>(f: impl FnOnce() -> T) -> Result<T, String> {
    std::panic::catch_unwind(AssertUnwindSafe(f)).map_err(|payload| panic_message(payload.as_ref()))
}

/// Renders a panic payload, which is a `&str` or `String` for `panic!` calls.
pub(crate) fn panic_message(payload: &(dyn Any + Send)) -> String {
    let detail = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload");
    format!("panicked: {detail}")
}
