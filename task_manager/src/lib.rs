#![allow(dead_code, unused)]

use futures::{
    future::{BoxFuture, Shared},
    StreamExt,
};
use tokio::{
    signal,
    sync::{mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;

pub trait ManagedTask {
    fn start(self: Box<Self>, token: CancellationToken) -> BoxFuture<'static, anyhow::Result<()>>;
}

pub struct TaskManager {
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

    pub fn add(&mut self, task: impl ManagedTask + 'static) {
        self.tasks.push(Box::new(task));
    }

    pub async fn start(self) -> anyhow::Result<()> {
        let (stop_sender, mut stop_receiver) = mpsc::channel(1);
        let (message_sender, mut message_receiver) = mpsc::channel::<Messages>(5);
        register_signal_listeners(stop_sender);

        let mut tokens = create_tokens(self.tasks.len());

        let mut futures: Vec<_> = tokens
            .clone()
            .into_iter()
            .zip(self.tasks.into_iter())
            .map(|(token, task)| task.start(token))
            .collect();

        let handle = tokio::spawn(async move {
            let mut select_all = futures::future::select_all(futures.into_iter());
            let mut last_result: Option<anyhow::Result<()>> = None;

            loop {
                let (result, index, remaining) = select_all.await;
                match result {
                    Ok(_) => {
                        message_sender.send(Messages::TaskStopped(index)).await;
                    }
                    Err(err) => {
                        message_sender.send(Messages::StopAll(index)).await;
                        if last_result.is_none() {
                            last_result = Some(Err(err));
                        }
                    }
                }

                if remaining.len() == 0 {
                    break;
                } else {
                    select_all = futures::future::select_all(remaining.into_iter());
                }
            }

            last_result.unwrap_or(Ok(()))
        });

        tokio::spawn(async move {
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
                        todo!()
                    }
                    Some(msg) = message_receiver.recv() => match msg {
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
        });

        match handle.await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(err)) => Err(err),
            Err(err) => Err(err.into()),
        }
    }
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
