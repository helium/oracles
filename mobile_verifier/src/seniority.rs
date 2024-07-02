use chrono::{DateTime, Duration, Utc};
use file_store::file_sink::FileSinkClient;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use helium_proto::services::poc_mobile as proto;

use crate::heartbeats::{KeyType, ValidatedHeartbeat};

#[derive(Clone, Debug, PartialEq, sqlx::FromRow)]
pub struct Seniority {
    pub uuid: Uuid,
    pub seniority_ts: DateTime<Utc>,
    pub last_heartbeat: DateTime<Utc>,
    pub inserted_at: DateTime<Utc>,
    pub update_reason: i32,
}

impl Seniority {
    pub async fn fetch_latest(
        key: KeyType<'_>,
        exec: &mut Transaction<'_, Postgres>,
    ) -> Result<Option<Self>, sqlx::Error> {
        sqlx::query_as(
            "SELECT uuid, seniority_ts, last_heartbeat, inserted_at, update_reason FROM seniority WHERE radio_key = $1 ORDER BY last_heartbeat DESC LIMIT 1",
        )
        .bind(key)
        .fetch_optional(&mut *exec)
        .await
    }
}

pub struct SeniorityUpdate<'a> {
    key: KeyType<'a>,
    heartbeat_ts: DateTime<Utc>,
    uuid: Uuid,
    pub action: SeniorityUpdateAction,
}

#[derive(Debug, PartialEq)]
pub enum SeniorityUpdateAction {
    NoAction,
    Insert {
        new_seniority: DateTime<Utc>,
        update_reason: proto::SeniorityUpdateReason,
    },
    Update {
        curr_seniority: DateTime<Utc>,
    },
}

impl<'a> SeniorityUpdate<'a> {
    pub fn new(
        key: KeyType<'a>,
        heartbeat_ts: DateTime<Utc>,
        uuid: Uuid,
        action: SeniorityUpdateAction,
    ) -> Self {
        Self {
            key,
            heartbeat_ts,
            uuid,
            action,
        }
    }

    pub fn from(
        heartbeat: &'a ValidatedHeartbeat,
        action: SeniorityUpdateAction,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            key: heartbeat.heartbeat.key(),
            heartbeat_ts: heartbeat.heartbeat.timestamp,
            uuid: heartbeat
                .heartbeat
                .coverage_object
                .ok_or_else(|| anyhow::anyhow!("invalid heartbeat, no coverage object found"))?,
            action,
        })
    }

    pub fn determine_update_action(
        heartbeat: &'a ValidatedHeartbeat,
        coverage_claim_time: DateTime<Utc>,
        modeled_coverage_start: DateTime<Utc>,
        latest_seniority: Option<Seniority>,
    ) -> anyhow::Result<Self> {
        use proto::SeniorityUpdateReason::*;

        if let Some(prev_seniority) = latest_seniority {
            if heartbeat.heartbeat.coverage_object != Some(prev_seniority.uuid) {
                // TODO need to think about this
                if prev_seniority.update_reason == HeartbeatNotSeen as i32
                    && coverage_claim_time < prev_seniority.seniority_ts
                {
                    Self::from(heartbeat, SeniorityUpdateAction::NoAction)
                } else {
                    Self::from(
                        heartbeat,
                        SeniorityUpdateAction::Insert {
                            new_seniority: coverage_claim_time,
                            update_reason: NewCoverageClaimTime,
                        },
                    )
                }
            } else if heartbeat.heartbeat.timestamp - prev_seniority.last_heartbeat
                > Duration::days(3)
                && coverage_claim_time < heartbeat.heartbeat.timestamp
            {
                Self::from(
                    heartbeat,
                    SeniorityUpdateAction::Insert {
                        new_seniority: heartbeat.heartbeat.timestamp,
                        update_reason: HeartbeatNotSeen,
                    },
                )
            } else {
                Self::from(
                    heartbeat,
                    SeniorityUpdateAction::Update {
                        curr_seniority: prev_seniority.seniority_ts,
                    },
                )
            }
        } else if heartbeat.heartbeat.timestamp - modeled_coverage_start > Duration::days(3) {
            // This will become the default case 72 hours after we launch modeled coverage
            Self::from(
                heartbeat,
                SeniorityUpdateAction::Insert {
                    new_seniority: heartbeat.heartbeat.timestamp,
                    update_reason: HeartbeatNotSeen,
                },
            )
        } else {
            Self::from(
                heartbeat,
                SeniorityUpdateAction::Insert {
                    new_seniority: coverage_claim_time,
                    update_reason: NewCoverageClaimTime,
                },
            )
        }
    }
}

impl SeniorityUpdate<'_> {
    #[allow(deprecated)]
    pub async fn write(&self, seniorities: &FileSinkClient) -> anyhow::Result<()> {
        if let SeniorityUpdateAction::Insert {
            new_seniority,
            update_reason,
        } = self.action
        {
            seniorities
                .write(
                    proto::SeniorityUpdate {
                        key_type: Some(self.key.into()),
                        new_seniority_timestamp: new_seniority.timestamp() as u64,
                        reason: update_reason as i32,
                        new_seniority_timestamp_ms: new_seniority.timestamp_millis() as u64,
                    },
                    [],
                )
                .await?;
        }
        Ok(())
    }

    pub async fn execute(self, exec: &mut Transaction<'_, Postgres>) -> anyhow::Result<()> {
        match self.action {
            SeniorityUpdateAction::NoAction => (),
            SeniorityUpdateAction::Insert {
                new_seniority,
                update_reason,
            } => {
                sqlx::query(
                    r#"
                    INSERT INTO seniority
                      (radio_key, last_heartbeat, uuid, seniority_ts, inserted_at, update_reason, radio_type)
                    VALUES
                      ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (radio_key, radio_type, seniority_ts) DO UPDATE SET
                      uuid = EXCLUDED.uuid,
                      last_heartbeat = EXCLUDED.last_heartbeat,
                      update_reason = EXCLUDED.update_reason
                    "#,
                )
                .bind(self.key)
                .bind(self.heartbeat_ts)
                .bind(self.uuid)
                .bind(new_seniority)
                .bind(self.heartbeat_ts)
                .bind(update_reason as i32)
                .bind(self.key.hb_type())
                .execute(&mut *exec)
                .await?;
            }
            SeniorityUpdateAction::Update { curr_seniority } => {
                sqlx::query(
                    r#"
                    UPDATE seniority
                    SET last_heartbeat = $1
                    WHERE
                      radio_key = $2 AND
                      seniority_ts = $3
                    "#,
                )
                .bind(self.heartbeat_ts)
                .bind(self.key)
                .bind(curr_seniority)
                .execute(&mut *exec)
                .await?;
            }
        }
        Ok(())
    }
}
