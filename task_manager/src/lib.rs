#![allow(dead_code, unused)]

use futures::{
    future::{self, BoxFuture, Shared},
    Future, StreamExt, TryFutureExt,
};
use tokio::{
    signal,
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

pub trait ManagedTask {
    fn start(self: Box<Self>, token: CancellationToken) -> BoxFuture<'static, anyhow::Result<()>>;
}

pub struct TaskManager {
    tasks: Vec<Box<dyn ManagedTask>>,
}

pub struct TaskManagerBuilder {
    tasks: Vec<Box<dyn ManagedTask>>,
}

enum Messages {
    TaskStopped(usize),
    StopAll(usize),
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
        let (stop_sender, mut stop_receiver) = mpsc::channel(1);
        let (message_sender, mut message_receiver) = mpsc::channel(5);
        register_signal_listeners(stop_sender);

        let mut tokens = create_tokens(self.tasks.len());

        let mut futures = start_futures(tokens.clone(), self.tasks);
        let handle = tokio::spawn(async move { handle_futures(futures, message_sender).await });

        tokio::spawn(async move { handle_stopping(tokens, stop_receiver, message_receiver).await });

        handle
            .await
            .map_err(|err| err.into())
            .and_then(|result| result)
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

async fn handle_futures(
    futures: Vec<BoxFuture<'static, anyhow::Result<()>>>,
    sender: mpsc::Sender<Messages>,
) -> anyhow::Result<()> {
    let mut select_all = future::select_all(futures.into_iter());
    let mut last_result: Option<anyhow::Result<()>> = None;

    loop {
        let (result, index, remaining) = select_all.await;
        match result {
            Ok(_) => {
                sender.send(Messages::TaskStopped(index)).await;
            }
            Err(err) => {
                sender.send(Messages::StopAll(index)).await;
                if last_result.is_none() {
                    last_result = Some(Err(err));
                }
            }
        }

        if remaining.len() == 0 {
            break;
        } else {
            select_all = future::select_all(remaining.into_iter());
        }
    }

    last_result.unwrap_or(Ok(()))
}

async fn handle_stopping(
    mut tokens: Vec<CancellationToken>,
    mut stop_receiver: mpsc::Receiver<bool>,
    mut receiver: mpsc::Receiver<Messages>,
) {
    let mut stopping = false;
    loop {
        if tokens.len() == 0 {
            break;
        }

        if stopping {
            if let Some(token) = tokens.last() {
                token.cancel();
            }
        }

        tokio::select! {
            _ = stop_receiver.recv() => {
                stopping = true;
            }
            Some(msg) = receiver.recv() => match msg {
                Messages::TaskStopped(index) => {
                    tokens.remove(index);
                }
                Messages::StopAll(index) => {
                    tokens.remove(index);
                    stopping = true;
                }
            },
        }
    }
}

fn start_futures(
    tokens: Vec<CancellationToken>,
    tasks: Vec<Box<dyn ManagedTask>>,
) -> Vec<BoxFuture<'static, anyhow::Result<()>>> {
    tokens
        .into_iter()
        .zip(tasks.into_iter())
        .map(|(token, task)| task.start(token))
        .collect()
}

fn register_signal_listeners(sender: mpsc::Sender<bool>) {
    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate()).unwrap();
    tokio::spawn(async move {
        tokio::select! {
            _ = sigterm.recv() => (),
            _ = signal::ctrl_c() => (),
        }

        sender.send(true).await;
    });
}

fn create_tokens(n: usize) -> Vec<CancellationToken> {
    let mut result = Vec::new();
    for i in 0..n {
        result.push(CancellationToken::new());
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;

    struct TestTask {
        id: u64,
        delay: u64,
        result: anyhow::Result<()>,
        sender: mpsc::Sender<u64>,
    }

    impl ManagedTask for TestTask {
        fn start(
            self: Box<Self>,
            token: CancellationToken,
        ) -> BoxFuture<'static, anyhow::Result<()>> {
            let handle = tokio::spawn(async move {
                tokio::select! {
                    _ = token.cancelled() => (),
                    _ = tokio::time::sleep(std::time::Duration::from_millis(self.delay)) => (),
                }

                self.sender.send(self.id).await;
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
