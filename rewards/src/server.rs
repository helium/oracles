use crate::{Result, Trigger};
use sqlx::{Pool, Postgres};
use tokio::sync::broadcast;

pub struct Server {
    trigger_receiver: broadcast::Receiver<Trigger>,
    pool: Pool<Postgres>,
}

impl Server {
    pub async fn new(
        pool: Pool<Postgres>,
        trigger_receiver: broadcast::Receiver<Trigger>,
    ) -> Result<Self> {
        let result = Self {
            pool,
            trigger_receiver,
        };
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
                trigger = self.trigger_receiver.recv() => {
                    if let Ok(trigger) = trigger {
                        if self.handle_trigger(trigger).await.is_err() {
                            tracing::error!("Failed to handle trigger!")
                        }
                    } else {
                        tracing::error!("Failed to recv trigger!")
                    }
                }
            }
        }
    }

    pub async fn handle_trigger(&mut self, trigger: Trigger) -> Result {
        // Store the trigger (ht + timestamp) in pg
        // Figure out heartbeat files corresponding to the incoming trigger - the last one we have
        // in pg
        // Read all the retrieved heartbeat files via FileMultiSource
        if trigger.insert_into(&self.pool).await.is_err() {
            tracing::error!("Error inserting trigger in DB!")
        }
        tracing::info!("chain trigger received {:#?}", trigger);
        Ok(())
    }
}
