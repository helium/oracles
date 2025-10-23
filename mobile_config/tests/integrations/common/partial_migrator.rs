use chrono::Utc;
use sqlx::{
    migrate::{Migration, Migrator},
    PgPool,
};

pub struct PartialMigrator {
    pool: PgPool,
    versions: Vec<i64>,
    migrator: Migrator,
}

impl PartialMigrator {
    pub async fn new(pool: PgPool, versions: Vec<i64>) -> anyhow::Result<Self> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
            .fetch_one(&pool)
            .await
            .unwrap_or(0);

        if count > 0 {
            anyhow::bail!("PartialMigrator: database already has applied migrations. Did you forget `migrations = false`?");
        }

        Ok(Self {
            pool,
            versions,
            migrator: sqlx::migrate!(),
        })
    }

    pub async fn run_partial(&self) -> anyhow::Result<()> {
        {
            // Run tmp_migrator to create _sqlx_migrations table
            let tmp_dir = tempfile::tempdir()?;
            let tmp_migrator = Migrator::new(tmp_dir.path()).await?;
            tmp_migrator.run(&self.pool).await?;
        }

        // Mark skipped migrations as applied first
        for m in self.migrator.iter() {
            if self.versions.contains(&m.version) {
                tracing::info!("⏭️ Skipping migration {} {}", m.version, m.description);
                self.skip_migration(m).await?;
            }
        }

        // Now run the migrator normally
        self.migrator.run(&self.pool).await?;

        Ok(())
    }

    pub async fn run_skipped(&self) -> anyhow::Result<()> {
        tracing::info!("Re applying skipped migrations... {:?}", self.versions);

        // Delete skipped migrations first
        self.delete_skipped().await?;

        // Now run the migrator normally
        self.migrator.run(&self.pool).await?;

        Ok(())
    }

    async fn skip_migration(&self, migration: &Migration) -> anyhow::Result<()> {
        sqlx::query(
            r#"
                INSERT INTO _sqlx_migrations 
                    (version, description, installed_on, success, checksum, execution_time)
                VALUES ($1, $2, $3, TRUE, $4, 0)
                ON CONFLICT (version) DO NOTHING
            "#,
        )
        .bind(migration.version)
        .bind(format!("SKIPPED - {}", migration.description.clone()))
        .bind(Utc::now())
        .bind(migration.checksum.as_ref())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn delete_skipped(&self) -> anyhow::Result<()> {
        for version in &self.versions {
            sqlx::query(
                r#"
                DELETE FROM _sqlx_migrations
                WHERE version = $1
                "#,
            )
            .bind(version)
            .execute(&self.pool)
            .await?;
        }

        Ok(())
    }
}
