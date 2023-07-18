mod select_all;

use std::pin::pin;

use crate::select_all::select_all;
use futures::{future::LocalBoxFuture, Future, StreamExt};
use tokio::signal;

pub trait ManagedTask {
    fn start_task(
        self: Box<Self>,
        shutdown_listener: triggered::Listener,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>>;
}

pub struct TaskManager {
    tasks: Vec<Box<dyn ManagedTask>>,
}

pub struct TaskManagerBuilder {
    tasks: Vec<Box<dyn ManagedTask>>,
}

pub struct StopableLocalFuture {
    shutdown_listener: triggered::Listener,
    future: LocalBoxFuture<'static, anyhow::Result<()>>,
}

impl Future for StopableLocalFuture {
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
        shutdown_listener: triggered::Listener,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(self(shutdown_listener))
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
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate()).unwrap();

        let (shutdown_trigger, shutdown_listener) = triggered::trigger();
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;
        tokio::spawn(async move {
            tokio::select! {
                _ = sigterm.recv() => shutdown_trigger.trigger(),
                _ = signal::ctrl_c() => shutdown_trigger.trigger(),
            }
        });

        let mut futures = start_futures(shutdown_listener.clone(), self.tasks);

        // let mut shutdown_listener =
        //     futures::future::select(Box::pin(sigterm.recv()), Box::pin(signal::ctrl_c()));

        loop {
            if futures.len() == 0 {
                break;
            }

            let mut select = select_all(futures.into_iter());

            tokio::select! {
                _ = &mut shutdown_listener => {
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
    pub fn add(mut self, task: impl ManagedTask + 'static) -> Self {
        self.tasks.push(Box::new(task));
        self
    }

    pub fn start(self) -> impl Future<Output = anyhow::Result<()>> {
        let manager = TaskManager { tasks: self.tasks };
        manager.start()
    }
}

fn start_futures(
    shutdown_listener: triggered::Listener,
    tasks: Vec<Box<dyn ManagedTask>>,
) -> Vec<StopableLocalFuture> {
    tasks
        .into_iter()
        .map(|task| StopableLocalFuture {
            shutdown_listener: shutdown_listener.clone(),
            future: task.start_task(shutdown_listener),
        })
        .collect()
}

async fn stop_all(futures: Vec<StopableLocalFuture>) -> anyhow::Result<()> {
    futures::stream::iter(futures.into_iter().rev())
        .fold(Ok(()), |last_result, local| async move {
            local.shutdown_listener.tri.cancel();
            let result = local.future.await;
            last_result.and(result)
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
    use futures::TryFutureExt;
    use tokio::sync::mpsc;

    struct TestTask {
        id: u64,
        delay: u64,
        result: anyhow::Result<()>,
        sender: mpsc::Sender<u64>,
    }

    impl ManagedTask for TestTask {
        fn start_task(
            self: Box<Self>,
            shutdown_listener: triggered::Listener,
        ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
            let handle = tokio::spawn(async move {
                tokio::select! {
                    _ = shutdown_listener.cancelled() => (),
                    _ = tokio::time::sleep(std::time::Duration::from_millis(self.delay)) => (),
                }

                self.sender.send(self.id).await.expect("unable to send");
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
            .add(TestTask {
                id: 1,
                delay: 50,
                result: Ok(()),
                sender: sender.clone(),
            })
            .add(TestTask {
                id: 2,
                delay: 100,
                result: Ok(()),
                sender: sender.clone(),
            })
            .start()
            .await;

        assert_eq!(Some(1), receiver.recv().await);
        assert_eq!(Some(2), receiver.recv().await);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn will_stop_all_in_reverse_order_after_error() {
        let (sender, mut receiver) = mpsc::channel(5);

        let result = TaskManager::builder()
            .add(TestTask {
                id: 1,
                delay: 1000,
                result: Ok(()),
                sender: sender.clone(),
            })
            .add(TestTask {
                id: 2,
                delay: 50,
                result: Err(anyhow!("error")),
                sender: sender.clone(),
            })
            .add(TestTask {
                id: 3,
                delay: 1000,
                result: Ok(()),
                sender: sender.clone(),
            })
            .start()
            .await;

        assert_eq!(Some(2), receiver.recv().await);
        assert_eq!(Some(3), receiver.recv().await);
        assert_eq!(Some(1), receiver.recv().await);
        assert_eq!("error", result.unwrap_err().to_string());
    }

    #[tokio::test]
    async fn will_return_first_error_returned() {
        let (sender, mut receiver) = mpsc::channel(5);

        let result = TaskManager::builder()
            .add(TestTask {
                id: 1,
                delay: 1000,
                result: Ok(()),
                sender: sender.clone(),
            })
            .add(TestTask {
                id: 2,
                delay: 50,
                result: Err(anyhow!("error")),
                sender: sender.clone(),
            })
            .add(TestTask {
                id: 3,
                delay: 200,
                result: Err(anyhow!("second")),
                sender: sender.clone(),
            })
            .start()
            .await;

        assert_eq!(Some(2), receiver.recv().await);
        assert_eq!(Some(3), receiver.recv().await);
        assert_eq!(Some(1), receiver.recv().await);
        assert_eq!("error", result.unwrap_err().to_string());
    }
}
