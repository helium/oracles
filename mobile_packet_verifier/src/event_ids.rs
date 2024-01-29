use chrono::{DateTime, Duration, Utc};
use sqlx::{Pool, Postgres, Transaction};
use task_manager::ManagedTask;

use crate::settings::Settings;

pub async fn is_duplicate(
    txn: &mut Transaction<'_, Postgres>,
    event_id: String,
    received_timestamp: DateTime<Utc>,
) -> anyhow::Result<bool> {
    sqlx::query("INSERT INTO event_ids(event_id, received_timestamp) VALUES($1, $2) ON CONFLICT (event_id) DO NOTHING")
        .bind(event_id)
        .bind(received_timestamp)
        .execute(txn)
        .await
        .map(|result| result.rows_affected() == 0)
        .map_err(anyhow::Error::from)
}

pub struct EventIdPurger {
    conn: Pool<Postgres>,
    interval: Duration,
    max_age: Duration,
}

impl ManagedTask for EventIdPurger {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> futures::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(self.run(shutdown))
    }
}

impl EventIdPurger {
    pub fn from_settings(conn: Pool<Postgres>, settings: &Settings) -> Self {
        Self {
            conn,
            interval: settings.purger_interval(),
            max_age: settings.purger_max_age(),
        }
    }

    pub async fn run(self, mut shutdown: triggered::Listener) -> anyhow::Result<()> {
        let mut timer = tokio::time::interval(self.interval.to_std()?);

        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    return Ok(())
                }
                _ = timer.tick() => {
                    purge(&self.conn, self.max_age).await?;
                }
            }
        }
    }
}

async fn purge(conn: &Pool<Postgres>, max_age: Duration) -> anyhow::Result<()> {
    let timestamp = Utc::now() - max_age;

    sqlx::query("DELETE FROM event_ids where received_timestamp < $1")
        .bind(timestamp)
        .execute(conn)
        .await
        .map(|_| ())
        .map_err(anyhow::Error::from)
}
