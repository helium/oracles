//! A trait for tasks that consume items from one or more channels.
//!
//! Implement [`ChannelConsumer`] for the unit of work that processes each
//! item, then wrap with [`channel_consumer`] when registering with a
//! [`crate::TaskManager`].

use std::{future::Future, ops::ControlFlow};

use crate::{spawn, BoxError, ManagedTask, TaskFuture};

/// A task that processes items received from a channel (or several).
///
/// The adapter handles the [`tokio::select!`] loop, shutdown coordination, and
/// `ManagedTask` impl. Implementors describe the per-item work, where the next
/// item comes from, and (optionally) one-shot setup or handling of source
/// closure.
///
/// # Cancel safety
///
/// [`recv`](ChannelConsumer::recv) is called inside a `tokio::select!`. If
/// shutdown wins the select, the `recv` future is dropped without yielding an
/// item, so its implementation **must be cancel-safe**.
/// [`tokio::sync::mpsc::Receiver::recv`] is cancel-safe per tokio's contract,
/// and `tokio::select!` composed of cancel-safe branches is itself cancel-safe.
///
/// # Single-channel example
///
/// ```ignore
/// struct Ingestor { rx: Receiver<File>, /* … */ }
///
/// impl task_manager::ChannelConsumer for Ingestor {
///     type Item = File;
///     type Error = anyhow::Error;
///
///     async fn recv(&mut self) -> Option<File> {
///         self.rx.recv().await
///     }
///
///     async fn handle(&mut self, file: File) -> anyhow::Result<()> {
///         self.process_file(file).await
///     }
/// }
/// ```
///
/// # Multi-channel example
///
/// ```ignore
/// enum Msg { A(Foo), B(Bar) }
///
/// impl task_manager::ChannelConsumer for MyTask {
///     type Item = Msg;
///     type Error = anyhow::Error;
///
///     async fn recv(&mut self) -> Option<Msg> {
///         tokio::select! {
///             m = self.chan_a.recv() => m.map(Msg::A),
///             m = self.chan_b.recv() => m.map(Msg::B),
///         }
///     }
///
///     async fn handle(&mut self, msg: Msg) -> anyhow::Result<()> {
///         match msg {
///             Msg::A(foo) => self.handle_a(foo).await,
///             Msg::B(bar) => self.handle_b(bar).await,
///         }
///     }
/// }
/// ```
pub trait ChannelConsumer: Send + Sync + 'static {
    type Item: Send + 'static;
    type Error: Into<BoxError> + Send;

    /// Wait for the next item. Return `None` when the source(s) are exhausted.
    ///
    /// Must be cancel-safe — if this future is dropped before completing, no
    /// item should be silently lost.
    fn recv(&mut self) -> impl Future<Output = Option<Self::Item>> + Send;

    /// Process a single item.
    fn handle(&mut self, item: Self::Item) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Optional one-shot setup. Returning [`ControlFlow::Break`] skips the loop
    /// entirely (useful for tasks constructed in a no-op state).
    fn on_start(&mut self) -> impl Future<Output = Result<ControlFlow<()>, Self::Error>> + Send {
        async { Ok(ControlFlow::Continue(())) }
    }

    /// Called when [`recv`](ChannelConsumer::recv) returns `None`. Default:
    /// exit the loop gracefully ([`ControlFlow::Break`]). Override to either
    /// return an error or keep looping (e.g. if `None` only means one of
    /// several sources closed and the others can still produce).
    fn on_receiver_closed(
        &mut self,
    ) -> impl Future<Output = Result<ControlFlow<()>, Self::Error>> + Send {
        async { Ok(ControlFlow::Break(())) }
    }
}

/// Wrap a [`ChannelConsumer`] task so it can be registered with
/// [`crate::TaskManager`].
pub fn channel_consumer<T: ChannelConsumer>(task: T) -> impl ManagedTask {
    ChannelConsumerAdapter(task)
}

struct ChannelConsumerAdapter<T>(T);

impl<T: ChannelConsumer> ManagedTask for ChannelConsumerAdapter<T> {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> TaskFuture {
        let mut task = self.0;
        spawn(async move {
            if let ControlFlow::Break(()) = task.on_start().await.map_err(Into::into)? {
                return Ok::<(), BoxError>(());
            }

            let mut shutdown = shutdown;
            loop {
                let received = tokio::select! {
                    biased;
                    _ = &mut shutdown => return Ok(()),
                    msg = task.recv() => msg,
                };

                match received {
                    Some(item) => task.handle(item).await.map_err(Into::into)?,
                    None => match task.on_receiver_closed().await.map_err(Into::into)? {
                        ControlFlow::Break(()) => return Ok(()),
                        ControlFlow::Continue(()) => continue,
                    },
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };

    use tokio::sync::mpsc;

    use crate::TaskManager;

    use super::*;

    struct CountingConsumer {
        rx: mpsc::Receiver<u32>,
        handled: Arc<AtomicUsize>,
        started: Arc<AtomicUsize>,
        closed: Arc<AtomicUsize>,
        fail_on: Option<u32>,
        on_close_break: bool,
        skip_on_start: bool,
    }

    impl ChannelConsumer for CountingConsumer {
        type Item = u32;
        type Error = std::io::Error;

        async fn recv(&mut self) -> Option<Self::Item> {
            self.rx.recv().await
        }

        async fn handle(&mut self, item: u32) -> Result<(), Self::Error> {
            if Some(item) == self.fail_on {
                return Err(std::io::Error::other("boom"));
            }
            self.handled.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn on_start(&mut self) -> Result<ControlFlow<()>, Self::Error> {
            self.started.fetch_add(1, Ordering::SeqCst);
            if self.skip_on_start {
                Ok(ControlFlow::Break(()))
            } else {
                Ok(ControlFlow::Continue(()))
            }
        }

        async fn on_receiver_closed(&mut self) -> Result<ControlFlow<()>, Self::Error> {
            self.closed.fetch_add(1, Ordering::SeqCst);
            if self.on_close_break {
                Ok(ControlFlow::Break(()))
            } else {
                Err(std::io::Error::other("unexpected close"))
            }
        }
    }

    fn build(
        rx: mpsc::Receiver<u32>,
        on_close_break: bool,
        skip_on_start: bool,
        fail_on: Option<u32>,
    ) -> (
        CountingConsumer,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
    ) {
        let handled = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(AtomicUsize::new(0));
        let closed = Arc::new(AtomicUsize::new(0));
        (
            CountingConsumer {
                rx,
                handled: handled.clone(),
                started: started.clone(),
                closed: closed.clone(),
                fail_on,
                on_close_break,
                skip_on_start,
            },
            handled,
            started,
            closed,
        )
    }

    #[tokio::test]
    async fn handles_items_then_exits_on_close() {
        let (tx, rx) = mpsc::channel(8);
        let (consumer, handled, started, closed) = build(rx, true, false, None);

        let manager = TaskManager::builder()
            .add_task(channel_consumer(consumer))
            .build();
        let (_trigger, shutdown) = triggered::trigger();

        tx.send(1).await.unwrap();
        tx.send(2).await.unwrap();
        tx.send(3).await.unwrap();
        drop(tx);

        let result = Box::new(manager).start_task(shutdown).await;
        assert!(result.is_ok(), "got {result:?}");
        assert_eq!(handled.load(Ordering::SeqCst), 3);
        assert_eq!(started.load(Ordering::SeqCst), 1);
        assert_eq!(closed.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn on_receiver_closed_err_propagates() {
        let (tx, rx) = mpsc::channel::<u32>(1);
        let (consumer, _h, _s, closed) = build(rx, false, false, None);

        let manager = TaskManager::builder()
            .add_task(channel_consumer(consumer))
            .build();
        let (_trigger, shutdown) = triggered::trigger();

        drop(tx);
        let result = Box::new(manager).start_task(shutdown).await;
        assert!(result.is_err());
        assert_eq!(closed.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn on_start_break_skips_loop() {
        let (_tx, rx) = mpsc::channel::<u32>(1);
        let (consumer, handled, started, closed) = build(rx, true, true, None);

        let manager = TaskManager::builder()
            .add_task(channel_consumer(consumer))
            .build();
        let (_trigger, shutdown) = triggered::trigger();

        let result = Box::new(manager).start_task(shutdown).await;
        assert!(result.is_ok());
        assert_eq!(started.load(Ordering::SeqCst), 1);
        assert_eq!(handled.load(Ordering::SeqCst), 0);
        assert_eq!(closed.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn handle_error_propagates() {
        let (tx, rx) = mpsc::channel(4);
        let (consumer, _h, _s, _c) = build(rx, true, false, Some(42));

        let manager = TaskManager::builder()
            .add_task(channel_consumer(consumer))
            .build();
        let (_trigger, shutdown) = triggered::trigger();

        tx.send(1).await.unwrap();
        tx.send(42).await.unwrap();

        let result = Box::new(manager).start_task(shutdown).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn shutdown_breaks_loop_cleanly() {
        let (tx, rx) = mpsc::channel::<u32>(1);
        let (consumer, _h, _s, _c) = build(rx, true, false, None);

        let manager = TaskManager::builder()
            .add_task(channel_consumer(consumer))
            .build();
        let (trigger, shutdown) = triggered::trigger();

        let handle = tokio::spawn(async move { Box::new(manager).start_task(shutdown).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        trigger.trigger();
        drop(tx);
        let result = handle.await.expect("join");
        assert!(result.is_ok(), "got {result:?}");
    }
}
