mod select_all;

use std::pin::pin;

use crate::select_all::select_all;
use futures::{future::LocalBoxFuture, Future, FutureExt, StreamExt};
use tokio::signal;

/// A boxed error type for task errors from user code.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Error type returned by task operations.
#[derive(Debug, thiserror::Error)]
pub enum TaskError {
    /// Error setting up signal handlers.
    #[error("signal handler setup failed: {0}")]
    Signal(#[from] std::io::Error),

    /// Error from a managed task.
    #[error("task failed: {0}")]
    Task(#[source] BoxError),
}

impl TaskError {
    pub fn from_err<E: Into<BoxError>>(err: E) -> Self {
        TaskError::Task(err.into())
    }
}

impl From<BoxError> for TaskError {
    fn from(err: BoxError) -> Self {
        TaskError::Task(err)
    }
}

/// Result type for task operations.
pub type TaskResult = Result<(), TaskError>;

pub type TaskLocalBoxFuture = LocalBoxFuture<'static, TaskResult>;

/// Helper for starting tasks that can be spawned into their own tokio task.
pub fn spawn<F, E>(fut: F) -> TaskLocalBoxFuture
where
    F: Future<Output = Result<(), E>> + Send + 'static,
    E: Into<BoxError> + Send + 'static,
{
    // tokio::spawn returns Result<Result<(), E>, JoinError>
    Box::pin(tokio::spawn(fut).map(|result| match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(TaskError::from_err(e)),
        Err(e) => Err(TaskError::from_err(e)),
    }))
}

/// Helper for tasks that do not satisfy the `Send` requirement of `spawn`.
///
/// The aim should be to have tasks spawned in their own tasks, unless there's a
/// good reason for them not to be.
pub fn run<F, E>(fut: F) -> TaskLocalBoxFuture
where
    F: Future<Output = Result<(), E>> + 'static,
    E: Into<BoxError> + 'static,
{
    Box::pin(async move { fut.await.map_err(TaskError::from_err) })
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
    future: LocalBoxFuture<'static, TaskResult>,
}

impl Future for StoppableLocalFuture {
    type Output = TaskResult;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        pin!(&mut self.future).poll(cx)
    }
}

impl<F, O> ManagedTask for F
where
    O: Future<Output = TaskResult> + 'static,
    F: FnOnce(triggered::Listener) -> O,
{
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> LocalBoxFuture<'static, TaskResult> {
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

    pub async fn start(self) -> TaskResult {
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

    async fn do_start(self, mut shutdown: LocalBoxFuture<'static, ()>) -> TaskResult {
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

async fn stop_all(futures: Vec<StoppableLocalFuture>) -> TaskResult {
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
    use futures::TryFutureExt;
    use tokio::sync::mpsc;

    #[derive(Debug)]
    struct TestError(&'static str);

    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl std::error::Error for TestError {}

    fn test_err(msg: &'static str) -> TaskError {
        TaskError::from_err(TestError(msg))
    }

    struct TestTask {
        name: &'static str,
        delay: u64,
        result: TaskResult,
        sender: mpsc::Sender<&'static str>,
    }

    impl ManagedTask for TestTask {
        fn start_task(
            self: Box<Self>,
            shutdown_listener: triggered::Listener,
        ) -> LocalBoxFuture<'static, TaskResult> {
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
                    .map_err(TaskError::from_err)
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
                result: Err(test_err("error")),
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
        assert_eq!("task failed: error", result.unwrap_err().to_string());
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
                result: Err(test_err("error")),
                sender: sender.clone(),
            })
            .add_task(TestTask {
                name: "3",
                delay: 200,
                result: Err(test_err("second")),
                sender: sender.clone(),
            })
            .build()
            .start()
            .await;

        assert_eq!(Some("2"), receiver.recv().await);
        assert_eq!(Some("3"), receiver.recv().await);
        assert_eq!(Some("1"), receiver.recv().await);
        assert_eq!("task failed: error", result.unwrap_err().to_string());
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
                        result: Err(test_err("error")),
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
