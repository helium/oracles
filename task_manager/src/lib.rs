mod select_all;

use std::pin::pin;

use crate::select_all::select_all;
use futures::{future::LocalBoxFuture, Future, StreamExt};
use tokio::signal;
use tokio_util::sync::CancellationToken;

pub trait ManagedTask {
    fn run(
        self: Box<Self>,
        token: CancellationToken,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>>;
}

pub struct TaskManager {
    tasks: Vec<Box<dyn ManagedTask>>,
}

pub struct TaskManagerBuilder {
    tasks: Vec<Box<dyn ManagedTask>>,
}

pub struct CancellableLocalFuture {
    token: CancellationToken,
    future: LocalBoxFuture<'static, anyhow::Result<()>>,
}

impl Future for CancellableLocalFuture {
    type Output = anyhow::Result<()>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        pin!(&mut self.future).poll(cx)
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

        let tokens = create_tokens(self.tasks.len());
        let mut futures = start_futures(tokens.clone(), self.tasks);

        loop {
            if futures.len() == 0 {
                break;
            }

            let mut select = select_all(futures.into_iter());

            tokio::select! {
                _ = sigterm.recv() => {
                    return stop_all(select.into_inner()).await;
                }
                _ = signal::ctrl_c() => {
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
    tokens: Vec<CancellationToken>,
    tasks: Vec<Box<dyn ManagedTask>>,
) -> Vec<CancellableLocalFuture> {
    tokens
        .into_iter()
        .zip(tasks.into_iter())
        .map(|(token, task)| CancellableLocalFuture {
            token: token.clone(),
            future: task.run(token),
        })
        .collect()
}

async fn stop_all(futures: Vec<CancellableLocalFuture>) -> anyhow::Result<()> {
    futures::stream::iter(futures.into_iter().rev())
        .fold(Ok(()), |last_result, local| async move {
            local.token.cancel();
            let result = local.future.await;
            last_result.and(result)
        })
        .await
}

fn create_tokens(n: usize) -> Vec<CancellationToken> {
    (0..n).fold(Vec::new(), |mut vec, _| {
        vec.push(CancellationToken::new());
        vec
    })
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
        fn run(
            self: Box<Self>,
            token: CancellationToken,
        ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
            let handle = tokio::spawn(async move {
                tokio::select! {
                    _ = token.cancelled() => (),
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
