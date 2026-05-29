//! Consumer for `chain_rewardable_entities.mobile_hotspot_change_report`
//! files written by the ingest service to S3. Replaces the per-event
//! portion of [`crate::gateway::tracker::Tracker`] — the tracker still
//! runs daily as a reconciliation pass to backfill antenna/elevation
//! drift and pick up any missed events.

use crate::gateway::{
    db::{Gateway, GatewayType},
    metadata_db,
    service::info::DeviceType,
};
use chrono::{DateTime, Utc};
use file_store::{
    file_info_poller::FileInfoStream,
    traits::{MsgDecode, TimestampDecode, TimestampDecodeError},
};
use futures::StreamExt;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::chain_rewardable_entities::{
    MobileHotspotChangeReportV1, MobileHotspotDeviceType,
};
use sqlx::{PgPool, Pool, Postgres};
use task_manager::ChannelConsumer;
use tokio::sync::mpsc::Receiver;

pub const PROCESS_NAME: &str = "mobile_hotspot_change";

#[derive(Debug, thiserror::Error)]
pub enum MobileHotspotChangeError {
    #[error("missing report")]
    MissingReport,
    #[error("missing change")]
    MissingChange,
    #[error("missing pub_key")]
    MissingPubKey,
    #[error("missing metadata")]
    MissingMetadata,
    #[error("unsupported device type: {0}")]
    UnsupportedDeviceType(prost::UnknownEnumValue),
    #[error("invalid pub_key: {0}")]
    InvalidPubKey(#[from] helium_crypto::Error),
    #[error("invalid timestamp: {0}")]
    Timestamp(#[from] TimestampDecodeError),
}

#[derive(Debug, Clone)]
pub struct MobileHotspotChange {
    pub entity_key: PublicKeyBinary,
    pub timestamp: DateTime<Utc>,
    pub device_type: DeviceType,
    /// H3 cell parsed from `asserted_hex`. `None` if the message had an
    /// empty hex string or a value that couldn't be parsed.
    pub location: Option<u64>,
    pub azimuth: u32,
}

impl MsgDecode for MobileHotspotChange {
    type Msg = MobileHotspotChangeReportV1;
}

impl TryFrom<MobileHotspotChangeReportV1> for MobileHotspotChange {
    type Error = MobileHotspotChangeError;

    fn try_from(report: MobileHotspotChangeReportV1) -> Result<Self, Self::Error> {
        let req = report
            .report
            .ok_or(MobileHotspotChangeError::MissingReport)?;
        let change = req.change.ok_or(MobileHotspotChangeError::MissingChange)?;
        let metadata = change
            .metadata
            .ok_or(MobileHotspotChangeError::MissingMetadata)?;
        let pub_key_bytes = change
            .pub_key
            .ok_or(MobileHotspotChangeError::MissingPubKey)?
            .value;

        let device_type_proto = file_store_oracles::prost_enum(
            metadata.device_type,
            MobileHotspotChangeError::UnsupportedDeviceType,
        )?;

        let device_type = match device_type_proto {
            MobileHotspotDeviceType::Cbrs => DeviceType::Cbrs,
            MobileHotspotDeviceType::WifiIndoor => DeviceType::WifiIndoor,
            MobileHotspotDeviceType::WifiOutdoor => DeviceType::WifiOutdoor,
            MobileHotspotDeviceType::WifiDataOnly => DeviceType::WifiDataOnly,
            MobileHotspotDeviceType::Unknown => {
                return Err(MobileHotspotChangeError::UnsupportedDeviceType(
                    prost::UnknownEnumValue(0),
                ))
            }
        };

        let location = parse_h3(&metadata.asserted_hex);
        let timestamp = change.timestamp_seconds.to_timestamp()?;

        Ok(Self {
            entity_key: PublicKeyBinary::from(pub_key_bytes),
            timestamp,
            device_type,
            location,
            azimuth: metadata.azimuth,
        })
    }
}

fn parse_h3(asserted_hex: &str) -> Option<u64> {
    let trimmed = asserted_hex.trim_start_matches("0x");
    if trimmed.is_empty() {
        return None;
    }
    u64::from_str_radix(trimmed, 16).ok()
}

pub struct HotspotChangeDaemon {
    pool: PgPool,
    metadata: Pool<Postgres>,
    receiver: Receiver<FileInfoStream<MobileHotspotChange>>,
}

impl HotspotChangeDaemon {
    pub fn new(
        pool: PgPool,
        metadata: Pool<Postgres>,
        receiver: Receiver<FileInfoStream<MobileHotspotChange>>,
    ) -> Self {
        Self {
            pool,
            metadata,
            receiver,
        }
    }

    async fn process_file(&self, file: FileInfoStream<MobileHotspotChange>) -> anyhow::Result<()> {
        tracing::info!(
            file = %file.file_info.key,
            "handling mobile hotspot change report"
        );
        let mut tx = self.pool.begin().await?;
        let mut stream = file.into_stream(&mut tx).await?;
        while let Some(change) = stream.next().await {
            if let Err(err) = self.apply_change(&mut tx, change).await {
                tracing::error!(?err, "failed to apply mobile hotspot change");
            }
        }
        tx.commit().await?;
        Ok(())
    }

    async fn apply_change(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        change: MobileHotspotChange,
    ) -> anyhow::Result<()> {
        // CBRS is not stored in the gateways table — silently drop.
        let gateway_type = match GatewayType::try_from(change.device_type) {
            Ok(gt) => gt,
            Err(_) => return Ok(()),
        };

        let (antenna, elevation) =
            metadata_db::fetch_antenna_and_elevation(&self.metadata, &change.entity_key)
                .await
                .unwrap_or_else(|err| {
                    tracing::warn!(
                        ?err,
                        address = %change.entity_key,
                        "metadata DB deployment lookup failed; writing nulls"
                    );
                    (None, None)
                });

        let prev = Gateway::get_by_address(&mut **tx, &change.entity_key).await?;

        let location_changed = prev
            .as_ref()
            .map(|p| p.location != change.location)
            .unwrap_or(true);

        let prev_asserts = prev.as_ref().and_then(|p| p.location_asserts).unwrap_or(0);
        let location_asserts = if change.location.is_some() && location_changed {
            prev_asserts + 1
        } else {
            prev_asserts
        };

        let next_location_changed_at = if location_changed && change.location.is_some() {
            Some(change.timestamp)
        } else {
            prev.as_ref().and_then(|p| p.location_changed_at)
        };

        let next = Gateway {
            address: change.entity_key.clone(),
            gateway_type,
            created_at: prev
                .as_ref()
                .map(|p| p.created_at)
                .unwrap_or(change.timestamp),
            inserted_at: Utc::now(),
            last_changed_at: change.timestamp,
            hash: String::new(), // write-ignored; insert computes from row
            antenna,
            elevation,
            azimuth: Some(change.azimuth),
            location: change.location,
            location_changed_at: next_location_changed_at,
            location_asserts: Some(location_asserts),
            owner: prev.as_ref().and_then(|p| p.owner.clone()),
            owner_changed_at: prev.as_ref().and_then(|p| p.owner_changed_at),
        };

        let next_hash = next.compute_hash();
        let observable_changed = prev.as_ref().map(|p| p.hash != next_hash).unwrap_or(true);
        if !observable_changed {
            return Ok(());
        }

        next.insert(&mut **tx).await?;
        metrics::counter!("oracles_mobile_config_hotspot_change_applied").increment(1);
        Ok(())
    }
}

impl ChannelConsumer for HotspotChangeDaemon {
    type Item = FileInfoStream<MobileHotspotChange>;
    type Error = anyhow::Error;

    async fn recv(&mut self) -> Option<Self::Item> {
        self.receiver.recv().await
    }

    async fn handle(&mut self, file: Self::Item) -> anyhow::Result<()> {
        self.process_file(file).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use helium_proto::services::chain_rewardable_entities::{
        HeliumPubKey, MobileHotspotChangeReqV1, MobileHotspotChangeV1, MobileHotspotMetadata,
        SolanaPubKey,
    };

    fn report(
        metadata: MobileHotspotMetadata,
        timestamp_seconds: u64,
    ) -> MobileHotspotChangeReportV1 {
        MobileHotspotChangeReportV1 {
            received_timestamp_ms: 0,
            report: Some(MobileHotspotChangeReqV1 {
                change: Some(MobileHotspotChangeV1 {
                    block: 1,
                    timestamp_seconds,
                    pub_key: Some(HeliumPubKey {
                        value: vec![0, 1, 2, 3],
                    }),
                    asset: Some(SolanaPubKey { value: vec![9] }),
                    metadata: Some(metadata),
                }),
                signer: "test".into(),
                signature: vec![],
            }),
        }
    }

    #[test]
    fn parse_h3_handles_plain_hex() {
        assert_eq!(parse_h3("8528347ffffffff"), Some(0x8528347ffffffff));
    }

    #[test]
    fn parse_h3_handles_0x_prefix() {
        assert_eq!(parse_h3("0x8528347ffffffff"), Some(0x8528347ffffffff));
    }

    #[test]
    fn parse_h3_rejects_empty_string() {
        assert_eq!(parse_h3(""), None);
    }

    #[test]
    fn parse_h3_rejects_malformed() {
        assert_eq!(parse_h3("not-hex"), None);
    }

    #[test]
    fn try_from_succeeds_for_wifi_indoor() {
        let metadata = MobileHotspotMetadata {
            serial_number: "sn-1".into(),
            device_type: MobileHotspotDeviceType::WifiIndoor as i32,
            asserted_hex: "8528347ffffffff".into(),
            azimuth: 42,
        };

        let change = MobileHotspotChange::try_from(report(metadata, 1_700_000_000)).unwrap();

        assert_eq!(change.device_type, DeviceType::WifiIndoor);
        assert_eq!(change.location, Some(0x8528347ffffffff));
        assert_eq!(change.azimuth, 42);
        assert_eq!(change.timestamp.timestamp(), 1_700_000_000);
    }

    #[test]
    fn try_from_returns_none_location_for_empty_hex() {
        let metadata = MobileHotspotMetadata {
            serial_number: String::new(),
            device_type: MobileHotspotDeviceType::WifiDataOnly as i32,
            asserted_hex: String::new(),
            azimuth: 0,
        };

        let change = MobileHotspotChange::try_from(report(metadata, 1)).unwrap();
        assert_eq!(change.location, None);
    }

    #[test]
    fn try_from_rejects_unknown_device_type() {
        let metadata = MobileHotspotMetadata {
            serial_number: String::new(),
            device_type: MobileHotspotDeviceType::Unknown as i32,
            asserted_hex: String::new(),
            azimuth: 0,
        };

        assert!(matches!(
            MobileHotspotChange::try_from(report(metadata, 1)),
            Err(MobileHotspotChangeError::UnsupportedDeviceType(_))
        ));
    }

    #[test]
    fn try_from_rejects_missing_metadata() {
        let r = MobileHotspotChangeReportV1 {
            received_timestamp_ms: 0,
            report: Some(MobileHotspotChangeReqV1 {
                change: Some(MobileHotspotChangeV1 {
                    block: 1,
                    timestamp_seconds: 1,
                    pub_key: Some(HeliumPubKey { value: vec![0] }),
                    asset: None,
                    metadata: None,
                }),
                signer: String::new(),
                signature: vec![],
            }),
        };
        assert!(matches!(
            MobileHotspotChange::try_from(r),
            Err(MobileHotspotChangeError::MissingMetadata)
        ));
    }
}
