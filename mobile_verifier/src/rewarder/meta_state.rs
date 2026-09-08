//! Where the rewarder keeps its own scheduling state.
//!
//! Two values, both owned by the rewarder rather than derived from any data
//! source: which epoch to reward next, and how long to keep skipping the
//! complete-data checks. Everything else the rewarder reads now comes from
//! Trino, so these are the last reason it needs a durable store of its own.
//!
//! They live in the Postgres `meta` table, which remains the source of truth —
//! every read comes from it, and the epoch advance is still written inside the
//! rewarder's end-of-epoch transaction. Configuring
//! [`Settings::rewarder_state_file`](crate::Settings) additionally mirrors both
//! values to a local JSON file, so a deployment can be running against the file
//! long before it stops running against Postgres. Nothing reads the mirror yet;
//! pointing the two accessors below at it is the remaining cutover step.

use std::path::{Path, PathBuf};

use anyhow::Context;
use chrono::{DateTime, TimeZone, Utc};
use db_store::meta;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, PgTransaction};

const NEXT_REWARD_EPOCH: &str = "next_reward_epoch";
const DISABLE_COMPLETE_DATA_CHECKS_UNTIL: &str = "disable_complete_data_checks_until";

/// The on-disk mirror of the two `meta` rows.
///
/// The `meta` table stores the disable-until value as a bare Unix timestamp;
/// the mirror stores it as an RFC 3339 datetime instead, so the file can be read
/// — and hand-edited — without converting epoch seconds in your head.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StateFile {
    next_reward_epoch: u64,
    /// Defaults to the Unix epoch — i.e. checks always run — matching the value
    /// migration 12 seeds into the `meta` table, so a hand-written file can
    /// leave it out.
    #[serde(default = "unix_epoch")]
    disable_complete_data_checks_until: DateTime<Utc>,
}

fn unix_epoch() -> DateTime<Utc> {
    DateTime::UNIX_EPOCH
}

#[derive(Debug, Clone)]
pub struct RewarderState {
    pool: PgPool,
    /// When set, every persisted change is also mirrored here.
    file: Option<PathBuf>,
}

impl RewarderState {
    pub fn new(pool: PgPool, file: Option<PathBuf>) -> Self {
        Self { pool, file }
    }

    pub fn from_settings(settings: &crate::Settings, pool: PgPool) -> Self {
        match settings.rewarder_state_file.as_ref() {
            Some(path) => tracing::info!(
                path = %path.display(),
                "mirroring rewarder state to file; postgres remains the source of truth"
            ),
            None => tracing::info!("rewarder state stored in the postgres `meta` table"),
        }
        Self::new(pool, settings.rewarder_state_file.clone())
    }

    pub async fn next_reward_epoch(&self) -> anyhow::Result<u64> {
        Ok(meta::fetch(&self.pool, NEXT_REWARD_EPOCH).await?)
    }

    pub async fn disable_complete_data_checks_until(&self) -> anyhow::Result<DateTime<Utc>> {
        let seconds: i64 = meta::fetch(&self.pool, DISABLE_COMPLETE_DATA_CHECKS_UNTIL).await?;

        Utc.timestamp_opt(seconds, 0).single().ok_or_else(|| {
            anyhow::anyhow!(
                "{DISABLE_COMPLETE_DATA_CHECKS_UNTIL} is not a valid timestamp: {seconds}"
            )
        })
    }

    /// Record the next epoch to reward, inside the caller's transaction so the
    /// write lands atomically with the rest of the end-of-epoch cleanup.
    ///
    /// The mirror is not updated here — see [`RewarderState::mirror_to_file`],
    /// which the caller runs once the transaction has committed.
    pub async fn save_next_reward_epoch(
        &self,
        transaction: &mut PgTransaction<'_>,
        value: u64,
    ) -> anyhow::Result<()> {
        Ok(meta::store(&mut **transaction, NEXT_REWARD_EPOCH, value).await?)
    }

    /// Copy the current `meta` values into the configured mirror file. A no-op
    /// when no file is configured.
    ///
    /// Deliberately reads back through Postgres rather than taking the values as
    /// arguments: the mirror then reflects what actually committed, and can't
    /// record an epoch advance that rolled back.
    pub async fn mirror_to_file(&self) -> anyhow::Result<()> {
        let Some(path) = self.file.as_ref() else {
            return Ok(());
        };

        let state = StateFile {
            next_reward_epoch: self.next_reward_epoch().await?,
            disable_complete_data_checks_until: self.disable_complete_data_checks_until().await?,
        };

        write(path, &state).await
    }
}

/// Write via a sibling temp file and rename, so a crash mid-write leaves the
/// previous contents intact rather than a truncated file.
async fn write(path: &Path, state: &StateFile) -> anyhow::Result<()> {
    let contents = serde_json::to_vec_pretty(state).context("serializing rewarder state")?;
    let tmp = path.with_extension("json.tmp");

    tokio::fs::write(&tmp, &contents)
        .await
        .with_context(|| format!("writing rewarder state file {}", tmp.display()))?;
    tokio::fs::rename(&tmp, path)
        .await
        .with_context(|| format!("replacing rewarder state file {}", path.display()))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use sqlx::PgPool;
    use tempfile::TempDir;

    async fn seed(pool: &PgPool, epoch: u64, disable_until: i64) -> anyhow::Result<()> {
        meta::store(pool, NEXT_REWARD_EPOCH, epoch).await?;
        meta::store(pool, DISABLE_COMPLETE_DATA_CHECKS_UNTIL, disable_until).await?;
        Ok(())
    }

    async fn read_mirror(dir: &TempDir) -> anyhow::Result<StateFile> {
        let contents = tokio::fs::read_to_string(dir.path().join("rewarder-state.json")).await?;
        Ok(serde_json::from_str(&contents)?)
    }

    fn state_with_mirror(pool: PgPool, dir: &TempDir) -> RewarderState {
        RewarderState::new(pool, Some(dir.path().join("rewarder-state.json")))
    }

    #[sqlx::test]
    async fn reads_come_from_postgres(pool: PgPool) -> anyhow::Result<()> {
        seed(&pool, 20321, 1_700_000_000).await?;
        let state = RewarderState::new(pool, None);

        assert_eq!(20321, state.next_reward_epoch().await?);
        assert_eq!(
            Utc.timestamp_opt(1_700_000_000, 0).single().unwrap(),
            state.disable_complete_data_checks_until().await?
        );

        Ok(())
    }

    #[sqlx::test]
    async fn mirror_captures_both_values(pool: PgPool) -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        seed(&pool, 20321, 1_700_000_000).await?;

        state_with_mirror(pool, &dir).mirror_to_file().await?;

        assert_eq!(
            StateFile {
                next_reward_epoch: 20321,
                disable_complete_data_checks_until: Utc
                    .timestamp_opt(1_700_000_000, 0)
                    .single()
                    .unwrap(),
            },
            read_mirror(&dir).await?
        );

        Ok(())
    }

    #[sqlx::test]
    async fn mirror_follows_an_epoch_advance(pool: PgPool) -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        seed(&pool, 5, 0).await?;
        let state = state_with_mirror(pool.clone(), &dir);

        state.mirror_to_file().await?;
        assert_eq!(5, read_mirror(&dir).await?.next_reward_epoch);

        let mut transaction = pool.begin().await?;
        state.save_next_reward_epoch(&mut transaction, 6).await?;
        transaction.commit().await?;
        state.mirror_to_file().await?;

        assert_eq!(6, read_mirror(&dir).await?.next_reward_epoch);

        Ok(())
    }

    /// The mirror reads back through Postgres, so an advance that never
    /// committed must not appear in the file.
    #[sqlx::test]
    async fn mirror_ignores_a_rolled_back_advance(pool: PgPool) -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        seed(&pool, 5, 0).await?;
        let state = state_with_mirror(pool.clone(), &dir);

        let mut transaction = pool.begin().await?;
        state.save_next_reward_epoch(&mut transaction, 6).await?;
        transaction.rollback().await?;
        state.mirror_to_file().await?;

        assert_eq!(5, read_mirror(&dir).await?.next_reward_epoch);

        Ok(())
    }

    #[sqlx::test]
    async fn mirroring_is_a_no_op_without_a_configured_file(pool: PgPool) -> anyhow::Result<()> {
        seed(&pool, 5, 0).await?;

        RewarderState::new(pool, None).mirror_to_file().await?;

        Ok(())
    }

    #[sqlx::test]
    async fn no_temp_file_is_left_behind(pool: PgPool) -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        seed(&pool, 5, 0).await?;

        state_with_mirror(pool, &dir).mirror_to_file().await?;

        let mut entries = tokio::fs::read_dir(dir.path()).await?;
        let mut names = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            names.push(entry.file_name().to_string_lossy().to_string());
        }
        assert_eq!(vec!["rewarder-state.json".to_string()], names);

        Ok(())
    }

    /// Migration 12 seeds the `meta` row to 0; an omitted field means the same,
    /// so a hand-written file can leave it out.
    #[test]
    fn disable_complete_data_checks_defaults_to_the_unix_epoch_when_absent() -> anyhow::Result<()> {
        let parsed: StateFile = serde_json::from_str(r#"{"next_reward_epoch": 1}"#)?;
        assert_eq!(
            DateTime::UNIX_EPOCH,
            parsed.disable_complete_data_checks_until
        );
        Ok(())
    }

    /// The whole point of the datetime: the file should be legible as written.
    #[sqlx::test]
    async fn mirror_writes_a_readable_datetime(pool: PgPool) -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        seed(&pool, 20321, 1_700_000_000).await?;

        state_with_mirror(pool, &dir).mirror_to_file().await?;

        let raw = tokio::fs::read_to_string(dir.path().join("rewarder-state.json")).await?;
        assert!(
            raw.contains("2023-11-14T22:13:20Z"),
            "expected an RFC 3339 timestamp, got: {raw}"
        );
        assert!(
            !raw.contains("1700000000"),
            "raw epoch seconds leaked: {raw}"
        );

        Ok(())
    }
}
