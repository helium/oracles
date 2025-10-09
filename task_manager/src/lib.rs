mod select_all;

use std::pin::pin;

use crate::select_all::select_all;
use futures::{future::LocalBoxFuture, Future, FutureExt, StreamExt};
use tokio::signal;

pub type TaskLocalBoxFuture = LocalBoxFuture<'static, anyhow::Result<()>>;

/// Helper for starting tasks that can be spawned into their own tokio task.
pub fn spawn<F>(fut: F) -> TaskLocalBoxFuture
where
    F: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let handle = tokio::spawn(fut);
    Box::pin(async move { handle.await? })
}

/// Helper for tasks that do not satisfy the `Send` requirement of `spawn`.
///
/// The aim should be to have tasks spawned in their own tasks, unless there's a
/// good reason for them not to be.
pub fn run<F>(fut: F) -> TaskLocalBoxFuture
where
    F: Future<Output = anyhow::Result<()>> + 'static,
{
    Box::pin(fut)
}

pub trait ManagedTask {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> TaskLocalBoxFuture;
}

pub struct TaskManager {
    tasks: Vec<Box<dyn ManagedTask>>,
}

impl ManagedTask for TaskManager {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> TaskLocalBoxFuture {
        crate::run(self.do_start(Box::pin(shutdown)))
    }
}

pub struct TaskManagerBuilder {
    tasks: Vec<Box<dyn ManagedTask>>,
}

struct StoppableLocalFuture {
    shutdown_trigger: triggered::Trigger,
    future: LocalBoxFuture<'static, anyhow::Result<()>>,
}

impl Future for StoppableLocalFuture {
    type Output = anyhow::Result<()>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        pin!(&mut self.future).poll(cx)
    }
}

impl<F, O> ManagedTask for F
where
    O: Future<Output = anyhow::Result<()>> + 'static,
    F: FnOnce(triggered::Listener) -> O,
{
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(self(shutdown))
    }
}

impl Default for TaskManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskManager {
    pub fn new() -> Self {
        Self { tasks: Vec::new() }
    }

    pub fn builder() -> TaskManagerBuilder {
        TaskManagerBuilder { tasks: Vec::new() }
    }

    pub fn add(&mut self, task: impl ManagedTask + 'static) {
        self.tasks.push(Box::new(task));
    }

    pub async fn start(self) -> anyhow::Result<()> {
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;
        let shutdown = Box::pin(
            futures::future::select(
                Box::pin(async move { sigterm.recv().await }),
                Box::pin(signal::ctrl_c()),
            )
            .map(|_| ()),
        );
        self.do_start(shutdown).await
    }

    async fn do_start(self, mut shutdown: LocalBoxFuture<'static, ()>) -> anyhow::Result<()> {
        let mut futures = start_futures(self.tasks);

        loop {
            if futures.is_empty() {
                break;
            }

            let mut select = select_all(futures);

            tokio::select! {
                _ = &mut shutdown => {
                    return stop_all(select.into_inner()).await;
                }
                (result, _index, remaining) = &mut select => match result {
                    Ok(_) => {
                        futures = remaining;
                    }
                    Err(err) => {
                        let _ = stop_all(remaining).await;
                        return Err(err);
                    }
                }
            }
        }

        Ok(())
    }
}

impl TaskManagerBuilder {
    pub fn add_task(mut self, task: impl ManagedTask + 'static) -> Self {
        self.tasks.push(Box::new(task));
        self
    }

    pub fn build(self) -> TaskManager {
        TaskManager { tasks: self.tasks }
    }
}

fn start_futures(tasks: Vec<Box<dyn ManagedTask>>) -> Vec<StoppableLocalFuture> {
    tasks
        .into_iter()
        .map(|task| {
            let (trigger, listener) = triggered::trigger();
            StoppableLocalFuture {
                shutdown_trigger: trigger,
                future: task.start_task(listener),
            }
        })
        .collect()
}

async fn stop_all(futures: Vec<StoppableLocalFuture>) -> anyhow::Result<()> {
    #[allow(clippy::manual_try_fold)]
    futures::stream::iter(futures.into_iter().rev())
        .then(|local| async move {
            local.shutdown_trigger.trigger();
            local.future.await
        })
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
    use futures::TryFutureExt;
    use tokio::sync::mpsc;

    struct TestTask {
        name: &'static str,
        delay: u64,
        result: anyhow::Result<()>,
        sender: mpsc::Sender<&'static str>,
    }

    impl ManagedTask for TestTask {
        fn start_task(
            self: Box<Self>,
            shutdown_listener: triggered::Listener,
        ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
            let handle = tokio::spawn(async move {
                tokio::select! {
                    _ = shutdown_listener.clone() => (),
                    _ = tokio::time::sleep(std::time::Duration::from_millis(self.delay)) => (),
                }
                self.sender.send(self.name).await.expect("unable to send");
                self.result
            });

            Box::pin(
                handle
                    .map_err(|err| err.into())
                    .and_then(|result| async move { result }),
            )
        }
    }

    #[tokio::test]
    async fn stop_when_all_tasks_have_completed() {
        let (sender, mut receiver) = mpsc::channel(5);

        let result = TaskManager::builder()
            .add_task(TestTask {
                name: "1",
                delay: 50,
                result: Ok(()),
                sender: sender.clone(),
            })
            .add_task(TestTask {
                name: "2",
                delay: 100,
                result: Ok(()),
                sender: sender.clone(),
            })
            .build()
            .start()
            .await;

        assert_eq!(Some("1"), receiver.recv().await);
        assert_eq!(Some("2"), receiver.recv().await);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn will_stop_all_in_reverse_order_after_error() {
        let (sender, mut receiver) = mpsc::channel(5);

        let result = TaskManager::builder()
            .add_task(TestTask {
                name: "1",
                delay: 1000,
                result: Ok(()),
                sender: sender.clone(),
            })
            .add_task(TestTask {
                name: "2",
                delay: 50,
                result: Err(anyhow!("error")),
                sender: sender.clone(),
            })
            .add_task(TestTask {
                name: "3",
                delay: 1000,
                result: Ok(()),
                sender: sender.clone(),
            })
            .build()
            .start()
            .await;

        assert_eq!(Some("2"), receiver.recv().await);
        assert_eq!(Some("3"), receiver.recv().await);
        assert_eq!(Some("1"), receiver.recv().await);
        assert_eq!("error", result.unwrap_err().to_string());
    }

    #[tokio::test]
    async fn will_return_first_error_returned() {
        let (sender, mut receiver) = mpsc::channel(5);

        let result = TaskManager::builder()
            .add_task(TestTask {
                name: "1",
                delay: 1000,
                result: Ok(()),
                sender: sender.clone(),
            })
            .add_task(TestTask {
                name: "2",
                delay: 50,
                result: Err(anyhow!("error")),
                sender: sender.clone(),
            })
            .add_task(TestTask {
                name: "3",
                delay: 200,
                result: Err(anyhow!("second")),
                sender: sender.clone(),
            })
            .build()
            .start()
            .await;

        assert_eq!(Some("2"), receiver.recv().await);
        assert_eq!(Some("3"), receiver.recv().await);
        assert_eq!(Some("1"), receiver.recv().await);
        assert_eq!("error", result.unwrap_err().to_string());
    }

    #[tokio::test]
    async fn nested_tasks_will_stop_parent_then_move_up() {
        let (sender, mut receiver) = mpsc::channel(10);

        let result = TaskManager::builder()
            .add_task(TestTask {
                name: "task-1",
                delay: 500,
                result: Ok(()),
                sender: sender.clone(),
            })
            .add_task(
                TaskManager::builder()
                    .add_task(TestTask {
                        name: "task-2-1",
                        delay: 500,
                        result: Ok(()),
                        sender: sender.clone(),
                    })
                    .add_task(TestTask {
                        name: "task-2-2",
                        delay: 100,
                        result: Err(anyhow!("error")),
                        sender: sender.clone(),
                    })
                    .add_task(TestTask {
                        name: "task-2-3",
                        delay: 500,
                        result: Ok(()),
                        sender: sender.clone(),
                    })
                    .add_task(TestTask {
                        name: "task-2",
                        delay: 500,
                        result: Ok(()),
                        sender: sender.clone(),
                    })
                    .build(),
            )
            .add_task(
                TaskManager::builder()
                    .add_task(TestTask {
                        name: "task-3-1",
                        delay: 1000,
                        result: Ok(()),
                        sender: sender.clone(),
                    })
                    .add_task(TestTask {
                        name: "task-3-2",
                        delay: 1000,
                        result: Ok(()),
                        sender: sender.clone(),
                    })
                    .add_task(TestTask {
                        name: "task-3",
                        delay: 1000,
                        result: Ok(()),
                        sender: sender.clone(),
                    })
                    .build(),
            )
            .build()
            .start()
            .await;

        assert_eq!(Some("task-2-2"), receiver.recv().await);
        assert_eq!(Some("task-2"), receiver.recv().await);
        assert_eq!(Some("task-2-3"), receiver.recv().await);
        assert_eq!(Some("task-2-1"), receiver.recv().await);
        assert_eq!(Some("task-3"), receiver.recv().await);
        assert_eq!(Some("task-3-2"), receiver.recv().await);
        assert_eq!(Some("task-3-1"), receiver.recv().await);
        assert_eq!(Some("task-1"), receiver.recv().await);
        assert!(result.is_err());
    }
}
