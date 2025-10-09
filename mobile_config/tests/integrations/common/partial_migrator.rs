use chrono::Utc;
use sqlx::{
    migrate::{Migration, Migrator},
    PgPool,
};
use std::path::Path;

pub struct PartialMigrator {
    pool: PgPool,
    versions: Vec<i64>,
    path: String,
}

impl PartialMigrator {
    pub async fn new(
        pool: PgPool,
        versions: Vec<i64>,
        path: Option<String>,
    ) -> anyhow::Result<Self> {
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM _sqlx_migrations")
            .fetch_one(&pool)
            .await
            .unwrap_or((0,));

        if count.0 > 0 {
            anyhow::bail!("PartialMigrator: database already has applied migrations. Did you forget `migrations = false`?");
        }

        Ok(Self {
            pool,
            versions,
            path: path.unwrap_or_else(|| "./migrations".to_string()),
        })
    }

    pub async fn run_partial(&self) -> anyhow::Result<()> {
        {
            // Run tmp_migrator to create _sqlx_migrations table
            let tmp_dir = tempfile::tempdir()?;
            let tmp_migrator = Migrator::new(tmp_dir.path()).await?;
            tmp_migrator.run(&self.pool).await?;
        }

        let migrator = Migrator::new(Path::new(&self.path)).await?;

        // Mark skipped migrations as applied first
        for m in migrator.iter() {
            if self.versions.contains(&m.version) {
                println!("⏭️ Skipping migration {} {}", m.version, m.description);
                self.skip_migration(m).await?;
            }
        }

        // Now run the migrator normally
        migrator.run(&self.pool).await?;

        Ok(())
    }

    pub async fn run_skipped(&self) -> anyhow::Result<()> {
        let migrator = Migrator::new(Path::new(&self.path)).await?;

        println!("Re applaying skipped migrations... {:?}", self.versions);

        // Delete skipped migrations first
        self.delete_skipped().await?;

        // Now run the migrator normally
        migrator.run(&self.pool).await?;

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
