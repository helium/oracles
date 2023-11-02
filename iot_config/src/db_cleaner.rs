use chrono::{DateTime, Duration, Utc};
use futures::TryFutureExt;
use sqlx::{Pool, Postgres, Transaction};
use task_manager::ManagedTask;

const SLEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(12 * 60 * 60);

pub struct DbCleaner {
    pool: Pool<Postgres>,
    deleted_entry_retention: Duration,
}

impl ManagedTask for DbCleaner {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> futures::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(
            tokio::spawn(self.run(shutdown))
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result }),
        )
    }
}

impl DbCleaner {
    pub fn new(pool: Pool<Postgres>, deleted_entry_retention: Duration) -> Self {
        Self {
            pool,
            deleted_entry_retention,
        }
    }

    async fn run(self, mut shutdown: triggered::Listener) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    tracing::warn!("db cleaner shutting down");
                    return Ok(());
                }
                _ = tokio::time::sleep(SLEEP_INTERVAL) => {
                    let mut tx = self.pool.begin().await?;
                    let timestamp = Utc::now() - self.deleted_entry_retention;

                    delete_skfs(&mut tx, timestamp).await?;
                    delete_devaddr_ranges(&mut tx, timestamp).await?;
                    delete_euis(&mut tx, timestamp).await?;
                    delete_routes(&mut tx, timestamp).await?;

                    tx.commit().await?;
                }
            }
        }
    }
}

async fn delete_routes(
    tx: &mut Transaction<'_, Postgres>,
    timestamp: DateTime<Utc>,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        delete from routes
        where deleted = true and updated_at < $1
    "#,
    )
    .bind(timestamp)
    .execute(tx)
    .await?;

    Ok(())
}

async fn delete_euis(
    tx: &mut Transaction<'_, Postgres>,
    timestamp: DateTime<Utc>,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        delete from route_eui_pairs 
        where deleted = true and updated_at < $1
    "#,
    )
    .bind(timestamp)
    .execute(tx)
    .await?;

    Ok(())
}

async fn delete_devaddr_ranges(
    tx: &mut Transaction<'_, Postgres>,
    timestamp: DateTime<Utc>,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        delete from route_devaddr_ranges 
        where deleted = true and updated_at < $1
    "#,
    )
    .bind(timestamp)
    .execute(tx)
    .await?;

    Ok(())
}

async fn delete_skfs(
    tx: &mut Transaction<'_, Postgres>,
    timestamp: DateTime<Utc>,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        delete from route_session_key_filters 
        where deleted = true and updated_at < $1
    "#,
    )
    .bind(timestamp)
    .execute(tx)
    .await?;

    Ok(())
}
