#![allow(dead_code, unused)]

use futures::{
    future::{BoxFuture, Shared},
    stream::StreamExt,
    Future, FutureExt,
};
use tokio::signal;
use tokio_util::sync::CancellationToken;

pub trait ManagedTask {
    type Error;

    fn start(&self, token: CancellationToken) -> BoxFuture<'static, Result<(), Self::Error>>;
}

pub struct TaskManager<E> {
    tasks: Vec<Box<dyn ManagedTask<Error = E>>>,
}

enum Messages {
    Shutdown,
}

impl<E> TaskManager<E>
where
    E: Clone,
{
    pub fn new() -> Self {
        Self { tasks: Vec::new() }
    }

    pub fn add(&mut self, task: impl ManagedTask<Error = E> + 'static) {
        self.tasks.push(Box::new(task));
    }

    pub async fn start(self) -> Result<(), E> {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(5);
        register_signal_listeners(sender);
        let mut tokens = create_tokens(self.tasks.len());

        let mut futures: Vec<Shared<BoxFuture<'static, Result<(), E>>>> = tokens
            .clone()
            .into_iter()
            .zip(self.tasks.into_iter())
            .map(|(token, task)| task.start(token).shared())
            .collect();

        loop {
            if futures.len() == 0 {
                break;
            }

            let stopping_futures = futures.clone();
            let select_all = futures::future::select_all(futures.into_iter());

            tokio::select! {
                msg = receiver.recv() => {
                    stop_all(tokens, stopping_futures).await;
                    break;
                }
                (result, index, remaining) = select_all => {
                    match result {
                        Ok(_) => {
                            futures = remaining;
                            tokens.remove(index);
                        }
                        Err(err) => {
                            tokens.remove(index);
                            stop_all(tokens, remaining).await;
                            return Err(err)
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

fn register_signal_listeners(sender: tokio::sync::mpsc::Sender<Messages>) {
    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate()).unwrap();
    tokio::spawn(async move {
        tokio::select! {
            _ = sigterm.recv() => (),
            _ = signal::ctrl_c() => (),
        }

        sender.send(Messages::Shutdown).await;
    });
}

fn create_tokens(n: usize) -> Vec<CancellationToken> {
    let mut result = Vec::new();
    for i in 0..n {
        result.push(CancellationToken::new());
    }
    result
}

async fn stop_all<E: Clone>(
    tokens: Vec<CancellationToken>,
    futures: Vec<Shared<BoxFuture<'static, Result<(), E>>>>,
) -> Result<(), E> {
    println!("Stopping all");
    let iter = tokens.into_iter().zip(futures.into_iter()).rev();

    futures::stream::iter(iter)
        .fold(Ok(()), |acc, (token, future)| async move {
            token.cancel();
            let result = future.await;
            acc.and(result)
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug)]
    enum TestError {
        Basic,
    }

    struct Test1 {
        delay: u64,
        result: Result<(), TestError>,
    }

    impl ManagedTask for Test1 {
        type Error = TestError;

        fn start(&self, token: CancellationToken) -> BoxFuture<'static, Result<(), Self::Error>> {
            println!("Calling start");
            let delay = self.delay;
            let result = self.result.clone();
            let handle = tokio::spawn(async move {
                tokio::select! {
                    _ = token.cancelled() => {
                        result
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_secs(delay)) => {
                        result
                    }
                }
            });

            Box::pin(async move {
                match handle.await {
                    Ok(Err(err)) => Err(TestError::Basic),
                    Err(err) => Err(TestError::Basic),
                    Ok(_) => Ok(()),
                }
            })
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn brian() {
        let mut manager = TaskManager::new();
        manager.add(Test1 {
            delay: 30,
            result: Ok(()),
        });
        manager.add(Test1 {
            delay: 20,
            result: Ok(()),
        });

        manager.add(Test1 {
            delay: 5,
            result: Err(TestError::Basic),
        });

        let result = manager.start().await;
        dbg!(result);
    }
}
