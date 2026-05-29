//! Consumer for `chain_rewardable_entities.entity_ownership_change_report`
//! files. Updates the `owner` and `owner_changed_at` columns on existing
//! `gateways` rows. Events for entities we've never seen via the hotspot
//! change stream are dropped — the daily reconciliation pass picks them
//! up from `asset_owners` in the metadata DB.

use crate::gateway::db::Gateway;
use chrono::{DateTime, Utc};
use file_store::{
    file_info_poller::FileInfoStream,
    traits::{MsgDecode, TimestampDecode, TimestampDecodeError},
};
use futures::StreamExt;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::chain_rewardable_entities::{
    EntityOwnerType, EntityOwnershipChangeReportV1,
};
use sqlx::{PgPool, Postgres};
use std::ops::ControlFlow;
use task_manager::ChannelConsumer;
use tokio::sync::mpsc::Receiver;

pub const PROCESS_NAME: &str = "entity_ownership_change";

#[derive(Debug, thiserror::Error)]
pub enum OwnershipChangeError {
    #[error("missing report")]
    MissingReport,
    #[error("missing change")]
    MissingChange,
    #[error("missing entity pub_key")]
    MissingEntityPubKey,
    #[error("missing owner")]
    MissingOwner,
    #[error("missing owner wallet")]
    MissingOwnerWallet,
    #[error("invalid timestamp: {0}")]
    Timestamp(#[from] TimestampDecodeError),
}

#[derive(Debug, Clone)]
pub struct OwnershipChange {
    pub entity_key: PublicKeyBinary,
    pub timestamp: DateTime<Utc>,
    /// Base58-encoded Solana wallet pubkey, matching the format stored in
    /// the existing `gateways.owner` column.
    pub owner: String,
    #[allow(dead_code)]
    pub owner_type: EntityOwnerType,
}

impl MsgDecode for OwnershipChange {
    type Msg = EntityOwnershipChangeReportV1;
}

impl TryFrom<EntityOwnershipChangeReportV1> for OwnershipChange {
    type Error = OwnershipChangeError;

    fn try_from(report: EntityOwnershipChangeReportV1) -> Result<Self, Self::Error> {
        let req = report.report.ok_or(OwnershipChangeError::MissingReport)?;
        let change = req.change.ok_or(OwnershipChangeError::MissingChange)?;

        let entity_key_bytes = change
            .entity_pub_key
            .ok_or(OwnershipChangeError::MissingEntityPubKey)?
            .value;

        let owner_info = change.owner.ok_or(OwnershipChangeError::MissingOwner)?;
        let owner_type = owner_info.r#type();
        let wallet_bytes = owner_info
            .wallet
            .ok_or(OwnershipChangeError::MissingOwnerWallet)?
            .value;

        Ok(Self {
            entity_key: PublicKeyBinary::from(entity_key_bytes),
            timestamp: change.timestamp_seconds.to_timestamp()?,
            owner: bs58::encode(wallet_bytes).into_string(),
            owner_type,
        })
    }
}

pub struct OwnershipChangeDaemon {
    pool: PgPool,
    receiver: Receiver<FileInfoStream<OwnershipChange>>,
}

impl OwnershipChangeDaemon {
    pub fn new(pool: PgPool, receiver: Receiver<FileInfoStream<OwnershipChange>>) -> Self {
        Self { pool, receiver }
    }

    async fn process_file(&self, file: FileInfoStream<OwnershipChange>) -> anyhow::Result<()> {
        tracing::info!(
            file = %file.file_info.key,
            "handling entity ownership change report"
        );
        let mut tx = self.pool.begin().await?;
        let mut stream = file.into_stream(&mut tx).await?;
        while let Some(change) = stream.next().await {
            if let Err(err) = self.apply_change(&mut tx, change).await {
                tracing::error!(?err, "failed to apply entity ownership change");
            }
        }
        tx.commit().await?;
        Ok(())
    }

    async fn apply_change(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        change: OwnershipChange,
    ) -> anyhow::Result<()> {
        // Only known mobile hotspots get updated. Unknown entities are either
        // IoT hotspots or hotspots whose mobile_hotspot_change_report we
        // haven't ingested yet; the daily tracker reconciles those.
        let Some(prev) = Gateway::get_by_address(&mut **tx, &change.entity_key).await? else {
            return Ok(());
        };

        if prev.owner.as_deref() == Some(change.owner.as_str()) {
            return Ok(());
        }

        let next = Gateway {
            inserted_at: Utc::now(),
            last_changed_at: change.timestamp,
            owner: Some(change.owner),
            owner_changed_at: Some(change.timestamp),
            // hash is recomputed inside insert; the value here is ignored.
            ..prev
        };

        next.insert(&mut **tx).await?;
        metrics::counter!("oracles_mobile_config_ownership_change_applied").increment(1);
        Ok(())
    }
}

impl ChannelConsumer for OwnershipChangeDaemon {
    type Item = FileInfoStream<OwnershipChange>;
    type Error = anyhow::Error;

    async fn recv(&mut self) -> Option<Self::Item> {
        self.receiver.recv().await
    }

    async fn handle(&mut self, file: Self::Item) -> anyhow::Result<()> {
        self.process_file(file).await
    }

    async fn on_receiver_closed(&mut self) -> Result<ControlFlow<()>, Self::Error> {
        Ok(ControlFlow::Break(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use helium_proto::services::chain_rewardable_entities::{
        EntityOwnerChangeV1, EntityOwnerInfo, EntityOwnershipChangeReqV1, HeliumPubKey,
        SolanaPubKey,
    };

    fn report(wallet_bytes: Vec<u8>, owner_type: EntityOwnerType) -> EntityOwnershipChangeReportV1 {
        EntityOwnershipChangeReportV1 {
            received_timestamp_ms: 0,
            report: Some(EntityOwnershipChangeReqV1 {
                change: Some(EntityOwnerChangeV1 {
                    block: 1,
                    timestamp_seconds: 1_700_000_000,
                    entity_pub_key: Some(HeliumPubKey {
                        value: vec![1, 2, 3],
                    }),
                    asset: Some(SolanaPubKey { value: vec![9] }),
                    owner: Some(EntityOwnerInfo {
                        wallet: Some(SolanaPubKey {
                            value: wallet_bytes,
                        }),
                        r#type: owner_type as i32,
                    }),
                }),
                signer: String::new(),
                signature: vec![],
            }),
        }
    }

    #[test]
    fn try_from_decodes_owner_as_base58() {
        let wallet = vec![0xde, 0xad, 0xbe, 0xef];
        let change =
            OwnershipChange::try_from(report(wallet.clone(), EntityOwnerType::DirectOwner))
                .unwrap();

        assert_eq!(change.owner, bs58::encode(wallet).into_string());
        assert_eq!(change.owner_type, EntityOwnerType::DirectOwner);
        assert_eq!(change.timestamp.timestamp(), 1_700_000_000);
    }

    #[test]
    fn try_from_preserves_welcome_pack_owner_type() {
        let change =
            OwnershipChange::try_from(report(vec![1], EntityOwnerType::WelcomePackOwner)).unwrap();
        assert_eq!(change.owner_type, EntityOwnerType::WelcomePackOwner);
    }

    #[test]
    fn try_from_rejects_missing_owner() {
        let r = EntityOwnershipChangeReportV1 {
            received_timestamp_ms: 0,
            report: Some(EntityOwnershipChangeReqV1 {
                change: Some(EntityOwnerChangeV1 {
                    block: 1,
                    timestamp_seconds: 1,
                    entity_pub_key: Some(HeliumPubKey { value: vec![1] }),
                    asset: None,
                    owner: None,
                }),
                signer: String::new(),
                signature: vec![],
            }),
        };
        assert!(matches!(
            OwnershipChange::try_from(r),
            Err(OwnershipChangeError::MissingOwner)
        ));
    }
}
