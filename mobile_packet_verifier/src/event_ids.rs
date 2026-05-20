use chrono::{DateTime, Utc};
use sqlx::{Pool, Postgres, Transaction};
use std::time::Duration;
use task_manager::Periodic;

use crate::settings::Settings;

pub async fn is_duplicate(
    txn: &mut Transaction<'_, Postgres>,
    event_id: &str,
    received_timestamp: DateTime<Utc>,
) -> anyhow::Result<bool> {
    sqlx::query("INSERT INTO event_ids(event_id, received_timestamp) VALUES($1, $2) ON CONFLICT (event_id) DO NOTHING")
        .bind(event_id)
        .bind(received_timestamp)
        .execute(&mut **txn)
        .await
        .map(|result| result.rows_affected() == 0)
        .map_err(anyhow::Error::from)
}

pub struct EventIdPurger {
    conn: Pool<Postgres>,
    interval: Duration,
    max_age: Duration,
}

impl Periodic for EventIdPurger {
    type Error = anyhow::Error;

    fn interval(&self) -> Duration {
        self.interval
    }

    async fn tick(&mut self) -> anyhow::Result<()> {
        purge(&self.conn, self.max_age).await
    }
}

impl EventIdPurger {
    pub fn from_settings(conn: Pool<Postgres>, settings: &Settings) -> Self {
        Self {
            conn,
            interval: settings.purger_interval,
            max_age: settings.purger_max_age,
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
