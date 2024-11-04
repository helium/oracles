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
            "SELECT uuid, seniority_ts, last_heartbeat, inserted_at, update_reason FROM seniority WHERE radio_key = $1 ORDER BY last_heartbeat DESC, seniority_ts DESC LIMIT 1",
        )
        .bind(key)
        .fetch_optional(&mut *exec)
        .await
    }
}

#[derive(Debug)]
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

    pub fn from_heartbeat(
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
        latest_seniority: Option<Seniority>,
    ) -> anyhow::Result<Self> {
        use proto::SeniorityUpdateReason::*;

        const SENIORITY_UPDATE_SKIP_REASONS: [i32; 2] =
            [HeartbeatNotSeen as i32, ServiceProviderBan as i32];

        if let Some(prev_seniority) = latest_seniority {
            if heartbeat.heartbeat.coverage_object != Some(prev_seniority.uuid) {
                if SENIORITY_UPDATE_SKIP_REASONS.contains(&prev_seniority.update_reason)
                    && coverage_claim_time < prev_seniority.seniority_ts
                {
                    Self::from_heartbeat(heartbeat, SeniorityUpdateAction::NoAction)
                } else {
                    Self::from_heartbeat(
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
                Self::from_heartbeat(
                    heartbeat,
                    SeniorityUpdateAction::Insert {
                        new_seniority: heartbeat.heartbeat.timestamp,
                        update_reason: HeartbeatNotSeen,
                    },
                )
            } else {
                Self::from_heartbeat(
                    heartbeat,
                    SeniorityUpdateAction::Update {
                        curr_seniority: prev_seniority.seniority_ts,
                    },
                )
            }
        } else {
            Self::from_heartbeat(
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
    pub async fn write(
        &self,
        seniorities: &FileSinkClient<proto::SeniorityUpdate>,
    ) -> anyhow::Result<()> {
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
                .bind(Utc::now())
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    use chrono::Utc;
    use helium_crypto::PublicKeyBinary;
    use helium_proto::services::poc_mobile::{self, LocationSource};
    use proto::SeniorityUpdateReason;
    use rust_decimal::{prelude::FromPrimitive, Decimal};
    use uuid::Uuid;

    use crate::{
        cell_type::CellType,
        heartbeats::{Heartbeat, ValidatedHeartbeat},
    };

    impl ValidatedHeartbeat {
        fn generate() -> anyhow::Result<Self> {
            Ok(Self {
                heartbeat: Heartbeat {
                    hb_type: crate::heartbeats::HbType::Wifi,
                    hotspot_key: PublicKeyBinary::from_str("1trSuseaaeZSW8pqSsYKFkFYTfFVvy8DbPCcne6fYYfry6XqzdN1PwAsqinbGKW2ux9554Dw4ciw1uDTdKjBZfjYeuzCEpd95kmZMPGiHaT5ZwasdPgSzXCSYzqmGeQ97riiqEik9xKKhxU52tjCgLd7HNfpLGT9ceY71FCcKBM3fooUZCSiNNibsVvorBWdWjvetgsHLwjTGuwYMGQ2BpmA15r9t3EGNnrfKMv6E1VmoBcuyPYgi7bBLZYpW16Yua3aHd78Jz8QqBVz51S5xRTwDBmgK41e9tSVSqMcQbcZkXi5W7Jru8QEiUTHWyghHgSYpsvCfcQkVBKkP7fHpM4Jh1YTxY2MEvLaoTzxFLRtrM")?,
                    cbsd_id: None,
                    operation_mode: true,
                    lat: 0.0,
                    lon: 0.0,
                    coverage_object: Some(Uuid::new_v4()),
                    location_validation_timestamp: None,
                    location_source: LocationSource::Skyhook,
                    timestamp: Utc::now(),
                },
                cell_type: CellType::NovaGenericWifiOutdoor,
                location_trust_score_multiplier: Decimal::from_u32(0).unwrap(),
                distance_to_asserted: Some(0),
                coverage_meta: None,
                validity: poc_mobile::HeartbeatValidity::Valid,
            })
        }
    }

    impl Seniority {
        fn generate() -> Self {
            Self {
                uuid: Uuid::new_v4(),
                seniority_ts: Utc::now(),
                last_heartbeat: Utc::now(),
                inserted_at: Utc::now(),
                update_reason: SeniorityUpdateReason::NewCoverageClaimTime as i32,
            }
        }

        fn uuid(self, uuid: Uuid) -> Self {
            Self { uuid, ..self }
        }

        fn last_heartbeat(self, last_heartbeat: DateTime<Utc>) -> Self {
            Self {
                last_heartbeat,
                ..self
            }
        }

        fn update_reason(self, reason: SeniorityUpdateReason) -> Self {
            Self {
                update_reason: reason as i32,
                ..self
            }
        }
    }

    #[test]
    fn first_coverage_object() -> anyhow::Result<()> {
        let heartbeat = ValidatedHeartbeat::generate()?;
        let coverage_claim_time = Utc::now();

        let result =
            SeniorityUpdate::determine_update_action(&heartbeat, coverage_claim_time, None)?;

        let SeniorityUpdate {
            action:
                SeniorityUpdateAction::Insert {
                    new_seniority,
                    update_reason,
                },
            ..
        } = result
        else {
            panic!("should have return insert action");
        };

        assert_eq!(new_seniority, coverage_claim_time);
        assert_eq!(update_reason, SeniorityUpdateReason::NewCoverageClaimTime);

        Ok(())
    }

    #[test]
    fn new_heartbeat_same_coverage_object() -> anyhow::Result<()> {
        let heartbeat = ValidatedHeartbeat::generate()?;
        let seniority = Seniority::generate().uuid(heartbeat.heartbeat.coverage_object.unwrap());

        let coverage_claim_time = Utc::now();

        let result = SeniorityUpdate::determine_update_action(
            &heartbeat,
            coverage_claim_time,
            Some(seniority.clone()),
        )?;

        let SeniorityUpdate {
            action: SeniorityUpdateAction::Update { curr_seniority },
            ..
        } = result
        else {
            panic!("should have been update action")
        };

        assert_eq!(seniority.seniority_ts, curr_seniority);

        Ok(())
    }

    #[test]
    fn heartbeat_not_seen_for_72_hours() -> anyhow::Result<()> {
        let heartbeat = ValidatedHeartbeat::generate()?;
        let seniority = Seniority::generate()
            .uuid(heartbeat.heartbeat.coverage_object.unwrap())
            .last_heartbeat(heartbeat.heartbeat.timestamp - Duration::hours(73));

        let coverage_claim_time = heartbeat.heartbeat.timestamp - Duration::hours(1);

        let result = SeniorityUpdate::determine_update_action(
            &heartbeat,
            coverage_claim_time,
            Some(seniority.clone()),
        )?;

        let SeniorityUpdate {
            action:
                SeniorityUpdateAction::Insert {
                    new_seniority,
                    update_reason,
                },
            ..
        } = result
        else {
            panic!("should have been insert action")
        };

        assert_eq!(new_seniority, heartbeat.heartbeat.timestamp);
        assert_eq!(update_reason, SeniorityUpdateReason::HeartbeatNotSeen);

        Ok(())
    }

    #[test]
    fn new_coverage_object() -> anyhow::Result<()> {
        let heartbeat = ValidatedHeartbeat::generate()?;
        let seniority = Seniority::generate();

        let coverage_claim_time = Utc::now();

        let result = SeniorityUpdate::determine_update_action(
            &heartbeat,
            coverage_claim_time,
            Some(seniority.clone()),
        )?;

        let SeniorityUpdate {
            action:
                SeniorityUpdateAction::Insert {
                    new_seniority,
                    update_reason,
                },
            ..
        } = result
        else {
            panic!("should have been insert action")
        };

        assert_eq!(new_seniority, coverage_claim_time);
        assert_eq!(update_reason, SeniorityUpdateReason::NewCoverageClaimTime);

        Ok(())
    }

    #[test]
    fn new_coverage_object_previous_was_heartbeat_not_seen() -> anyhow::Result<()> {
        let heartbeat = ValidatedHeartbeat::generate()?;
        let seniority =
            Seniority::generate().update_reason(SeniorityUpdateReason::HeartbeatNotSeen);

        let coverage_claim_time = seniority.seniority_ts - Duration::seconds(1);

        let result = SeniorityUpdate::determine_update_action(
            &heartbeat,
            coverage_claim_time,
            Some(seniority.clone()),
        )?;

        assert_eq!(result.action, SeniorityUpdateAction::NoAction);

        Ok(())
    }

    #[test]
    fn new_coverage_object_previous_was_service_provider_ban() -> anyhow::Result<()> {
        let heartbeat = ValidatedHeartbeat::generate()?;
        let seniority =
            Seniority::generate().update_reason(SeniorityUpdateReason::ServiceProviderBan);

        let coverage_claim_time = seniority.seniority_ts - Duration::seconds(1);

        let result = SeniorityUpdate::determine_update_action(
            &heartbeat,
            coverage_claim_time,
            Some(seniority.clone()),
        )?;

        assert_eq!(result.action, SeniorityUpdateAction::NoAction);

        Ok(())
    }
}
