//! A trait for periodic interval-driven tasks.
//!
//! Implement [`Periodic`] for the unit of work you want to run on an interval,
//! then wrap with [`periodic`] when registering with a [`crate::TaskManager`].

use std::{future::Future, time::Duration};

use crate::{spawn, BoxError, ManagedTask, TaskFuture};

/// A task that runs a fixed unit of work on an interval.
///
/// The adapter handles the [`tokio::select!`] loop, shutdown coordination, and
/// `ManagedTask` impl. Implementors only describe the interval, the per-tick
/// work, and (optionally) one-shot setup.
///
/// # Example
///
/// ```ignore
/// use std::time::Duration;
///
/// struct Purger { pool: PgPool, interval: Duration }
///
/// impl task_manager::Periodic for Purger {
///     type Error = anyhow::Error;
///     fn interval(&self) -> Duration { self.interval }
///     async fn tick(&mut self) -> anyhow::Result<()> {
///         purge(&self.pool).await
///     }
/// }
///
/// TaskManager::builder()
///     .add_task(task_manager::periodic(purger))
///     .build()
///     .start()
///     .await?;
/// ```
pub trait Periodic: Send + Sync + 'static {
    type Error: Into<BoxError> + Send;

    /// The interval between ticks.
    fn interval(&self) -> Duration;

    /// Work to perform on each tick.
    fn tick(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Optional one-shot setup, awaited once before entering the loop.
    fn on_start(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }
}

/// Wrap a [`Periodic`] task so it can be registered with [`crate::TaskManager`].
pub fn periodic<T: Periodic>(task: T) -> impl ManagedTask {
    PeriodicAdapter(task)
}

struct PeriodicAdapter<T>(T);

impl<T: Periodic> ManagedTask for PeriodicAdapter<T> {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> TaskFuture {
        let mut task = self.0;
        spawn(async move {
            task.on_start().await.map_err(Into::into)?;
            let mut shutdown = shutdown;
            let mut timer = tokio::time::interval(task.interval());
            loop {
                tokio::select! {
                    biased;
                    _ = &mut shutdown => return Ok::<(), BoxError>(()),
                    _ = timer.tick() => task.tick().await.map_err(Into::into)?,
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use crate::TaskManager;

    use super::*;

    struct CountingPeriodic {
        ticks: Arc<AtomicUsize>,
        started: Arc<AtomicUsize>,
        interval: Duration,
        fail_on_tick: Option<usize>,
    }

    impl Periodic for CountingPeriodic {
        type Error = std::io::Error;

        fn interval(&self) -> Duration {
            self.interval
        }

        async fn tick(&mut self) -> Result<(), Self::Error> {
            let n = self.ticks.fetch_add(1, Ordering::SeqCst) + 1;
            if Some(n) == self.fail_on_tick {
                return Err(std::io::Error::other("boom"));
            }
            Ok(())
        }

        async fn on_start(&mut self) -> Result<(), Self::Error> {
            self.started.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn shutdown_breaks_loop_cleanly() {
        let ticks = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(AtomicUsize::new(0));
        let task = periodic(CountingPeriodic {
            ticks: ticks.clone(),
            started: started.clone(),
            interval: Duration::from_millis(10),
            fail_on_tick: None,
        });

        let manager = TaskManager::builder().add_task(task).build();
        let (trigger, shutdown) = triggered::trigger();

        let handle = tokio::spawn(async move { Box::new(manager).start_task(shutdown).await });

        tokio::time::sleep(Duration::from_millis(50)).await;
        trigger.trigger();
        let result = handle.await.expect("join");

        assert!(result.is_ok(), "got {result:?}");
        assert_eq!(
            started.load(Ordering::SeqCst),
            1,
            "on_start should run once"
        );
        assert!(ticks.load(Ordering::SeqCst) >= 1, "at least one tick");
    }

    #[tokio::test]
    async fn tick_error_propagates() {
        let ticks = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(AtomicUsize::new(0));
        let task = periodic(CountingPeriodic {
            ticks: ticks.clone(),
            started: started.clone(),
            interval: Duration::from_millis(5),
            fail_on_tick: Some(2),
        });

        let manager = TaskManager::builder().add_task(task).build();
        let (_trigger, shutdown) = triggered::trigger();

        let result = Box::new(manager).start_task(shutdown).await;
        assert!(result.is_err(), "expected error, got {result:?}");
        assert_eq!(started.load(Ordering::SeqCst), 1);
    }
}
