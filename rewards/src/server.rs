use crate::{Result, Trigger};
use tokio::sync::broadcast;

pub struct Server {
    trigger_receiver: broadcast::Receiver<Trigger>,
}

impl Server {
    pub async fn new(trigger_receiver: broadcast::Receiver<Trigger>) -> Result<Self> {
        let result = Self { trigger_receiver };
        Ok(result)
    }

    pub async fn run(&mut self, shutdown: triggered::Listener) -> Result {
        tracing::info!("starting rewards server");

        loop {
            if shutdown.is_triggered() {
                tracing::info!("stopping rewards server");
                return Ok(());
            }
            tokio::select! {
                _ = shutdown.clone() => (),
                _trigger = self.trigger_receiver.recv() => tracing::info!("chain trigger received"),
            }
        }
    }
}
