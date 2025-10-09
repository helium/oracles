use std::{sync::Arc, time::Instant};

use chrono::{DateTime, Utc};
use file_store::{
    file_info_poller::FileInfoStream,
    file_sink::FileSinkClient,
    file_source,
    file_upload::FileUpload,
    traits::{
        FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt, TimestampDecode,
        TimestampEncode,
    },
    FileType,
};
use futures::{StreamExt, TryStreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::{
    mobile_config::NetworkKeyRole,
    poc_mobile::{
        SubscriberMappingActivityIngestReportV1, SubscriberReportVerificationStatus,
        VerifiedSubscriberMappingActivityReportV1,
    },
};
use mobile_config::client::{
    authorization_client::AuthorizationVerifier, entity_client::EntityVerifier,
};
use sqlx::{Pool, Postgres};
use task_manager::{ManagedTask, TaskManager};
use tokio::sync::mpsc::Receiver;

use crate::Settings;

pub mod db;

pub struct SubscriberMappingActivityDaemon<AV, EV> {
    pool: Pool<Postgres>,
    authorization_verifier: Arc<AV>,
    entity_verifier: Arc<EV>,
    stream_receiver: Receiver<FileInfoStream<SubscriberMappingActivityIngestReportV1>>,
    verified_sink: FileSinkClient<VerifiedSubscriberMappingActivityReportV1>,
}

impl<AV, EV> SubscriberMappingActivityDaemon<AV, EV>
where
    AV: AuthorizationVerifier,
    EV: EntityVerifier,
{
    pub fn new(
        pool: Pool<Postgres>,
        authorization_verifier: AV,
        entity_verifier: EV,
        stream_receiver: Receiver<FileInfoStream<SubscriberMappingActivityIngestReportV1>>,
        verified_sink: FileSinkClient<VerifiedSubscriberMappingActivityReportV1>,
    ) -> Self {
        Self {
            pool,
            authorization_verifier: Arc::new(authorization_verifier),
            entity_verifier: Arc::new(entity_verifier),
            stream_receiver,
            verified_sink,
        }
    }

    pub async fn create_managed_task(
        pool: Pool<Postgres>,
        settings: &Settings,
        authorization_verifier: AV,
        entity_verifier: EV,
        file_store_client: file_store::Client,
        bucket: String,
        file_upload: FileUpload,
    ) -> anyhow::Result<impl ManagedTask> {
        let (stream_reciever, stream_server) = file_source::Continuous::prost_source()
            .state(pool.clone())
            .file_store(file_store_client, bucket)
            .lookback_start_after(settings.start_after)
            .prefix(FileType::SubscriberMappingActivityIngestReport.to_string())
            .create()
            .await?;

        let (verified_sink, verified_sink_server) =
            VerifiedSubscriberMappingActivityReportV1::file_sink(
                settings.store_base_path(),
                file_upload.clone(),
                FileSinkCommitStrategy::Manual,
                FileSinkRollTime::Default,
                env!("CARGO_PKG_NAME"),
            )
            .await?;

        let daemon = Self::new(
            pool,
            authorization_verifier,
            entity_verifier,
            stream_reciever,
            verified_sink,
        );

        Ok(TaskManager::builder()
            .add_task(stream_server)
            .add_task(verified_sink_server)
            .add_task(daemon)
            .build())
    }

    pub async fn run(mut self, mut shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("starting");

        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                Some(file) = self.stream_receiver.recv() => {
                    let start = Instant::now();
                    self.process_file(file).await?;
                    metrics::histogram!("subscriber_mapping_activity_processing_time")
                        .record(start.elapsed());
                }
            }
        }

        tracing::info!("stopping");
        Ok(())
    }

    async fn process_file(
        &self,
        file_info_stream: FileInfoStream<SubscriberMappingActivityIngestReportV1>,
    ) -> anyhow::Result<()> {
        let mut transaction = self.pool.begin().await?;
        let stream = file_info_stream.into_stream(&mut transaction).await?;

        let activity_stream = stream
            .map(|proto| {
                let activity = SubscriberMappingActivity::try_from(proto.clone())?;
                Ok((activity, proto))
            })
            .and_then(|(activity, proto)| {
                let av = self.authorization_verifier.clone();
                let ev = self.entity_verifier.clone();
                async move {
                    let status = verify_activity(av, ev, &activity).await?;
                    Ok((activity, proto, status))
                }
            })
            .and_then(|(activity, proto, status)| {
                let sink = self.verified_sink.clone();
                async move {
                    write_verified_report(sink, proto, status).await?;
                    Ok((activity, status))
                }
            })
            .try_filter_map(|(activity, status)| async move {
                Ok(matches!(status, SubscriberReportVerificationStatus::Valid).then_some(activity))
            });

        db::save(&mut transaction, activity_stream).await?;
        self.verified_sink.commit().await?;
        transaction.commit().await?;
        Ok(())
    }
}

async fn write_verified_report(
    sink: FileSinkClient<VerifiedSubscriberMappingActivityReportV1>,
    proto: SubscriberMappingActivityIngestReportV1,
    status: SubscriberReportVerificationStatus,
) -> anyhow::Result<()> {
    let verified_proto = VerifiedSubscriberMappingActivityReportV1 {
        report: Some(proto),
        status: status as i32,
        timestamp: Utc::now().encode_timestamp_millis(),
    };

    sink.write(verified_proto, &[("status", status.as_str_name())])
        .await?;

    Ok(())
}

async fn verify_activity<AV, EV>(
    authorization_verifier: impl AsRef<AV>,
    entity_verifier: impl AsRef<EV>,
    activity: &SubscriberMappingActivity,
) -> anyhow::Result<SubscriberReportVerificationStatus>
where
    AV: AuthorizationVerifier,
    EV: EntityVerifier,
{
    if !verify_known_carrier_key(authorization_verifier, &activity.carrier_pub_key).await? {
        return Ok(SubscriberReportVerificationStatus::InvalidCarrierKey);
    };
    if !verify_entity(&entity_verifier, &activity.subscriber_id).await? {
        return Ok(SubscriberReportVerificationStatus::InvalidSubscriberId);
    };
    if let Some(rek) = &activity.reward_override_entity_key {
        // use UTF8(key_serialization) as bytea
        if !verify_entity(entity_verifier, &rek.clone().into_bytes()).await? {
            return Ok(SubscriberReportVerificationStatus::InvalidRewardOverrideEntityKey);
        };
    }
    Ok(SubscriberReportVerificationStatus::Valid)
}

async fn verify_known_carrier_key<AV>(
    authorization_verifier: impl AsRef<AV>,
    public_key: &PublicKeyBinary,
) -> anyhow::Result<bool>
where
    AV: AuthorizationVerifier,
{
    authorization_verifier
        .as_ref()
        .verify_authorized_key(public_key, NetworkKeyRole::MobileCarrier)
        .await
        .map_err(anyhow::Error::from)
}

async fn verify_entity<EV>(
    entity_verifier: impl AsRef<EV>,
    entity_id: &[u8],
) -> anyhow::Result<bool>
where
    EV: EntityVerifier,
{
    entity_verifier
        .as_ref()
        .verify_rewardable_entity(entity_id)
        .await
        .map_err(anyhow::Error::from)
}

impl<AV, EV> ManagedTask for SubscriberMappingActivityDaemon<AV, EV>
where
    AV: AuthorizationVerifier,
    EV: EntityVerifier,
{
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> task_manager::TaskLocalBoxFuture {
        task_manager::spawn(self.run(shutdown))
    }
}

pub struct SubscriberMappingActivity {
    pub subscriber_id: Vec<u8>,
    pub discovery_reward_shares: u64,
    pub verification_reward_shares: u64,
    pub received_timestamp: DateTime<Utc>,
    pub carrier_pub_key: PublicKeyBinary,
    pub reward_override_entity_key: Option<String>,
}

impl TryFrom<SubscriberMappingActivityIngestReportV1> for SubscriberMappingActivity {
    type Error = anyhow::Error;

    fn try_from(value: SubscriberMappingActivityIngestReportV1) -> Result<Self, Self::Error> {
        let report = value
            .report
            .ok_or_else(|| anyhow::anyhow!("SubscriberMappingActivityReqV1 not found"))?;

        let reward_override_entity_key = if report.reward_override_entity_key.is_empty() {
            None
        } else {
            Some(report.reward_override_entity_key)
        };

        Ok(Self {
            subscriber_id: report.subscriber_id,
            discovery_reward_shares: report.discovery_reward_shares,
            verification_reward_shares: report.verification_reward_shares,
            received_timestamp: value.received_timestamp.to_timestamp_millis()?,
            carrier_pub_key: PublicKeyBinary::from(report.carrier_pub_key),
            reward_override_entity_key,
        })
    }
}

#[derive(Clone, Debug, sqlx::FromRow)]
pub struct SubscriberMappingShares {
    pub subscriber_id: Vec<u8>,
    #[sqlx(try_from = "i64")]
    pub discovery_reward_shares: u64,
    #[sqlx(try_from = "i64")]
    pub verification_reward_shares: u64,

    pub reward_override_entity_key: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::SubscriberMappingActivity;
    use helium_proto::services::poc_mobile::SubscriberMappingActivityIngestReportV1;

    #[test]
    fn try_from_subscriber_mapping_activity_check_entity_key() {
        // Make sure reward_override_entity_key empty string transforms to None
        let smair = SubscriberMappingActivityIngestReportV1 {
            received_timestamp: 1,
            report: Some({
                helium_proto::services::poc_mobile::SubscriberMappingActivityReqV1 {
                    subscriber_id: vec![10],
                    discovery_reward_shares: 2,
                    verification_reward_shares: 3,
                    timestamp: 4,
                    carrier_pub_key: vec![11],
                    signature: vec![12],
                    reward_override_entity_key: "".to_string(),
                }
            }),
        };
        let mut smair2 = smair.clone();
        smair2.report.as_mut().unwrap().reward_override_entity_key = "key".to_string();

        let res = SubscriberMappingActivity::try_from(smair).unwrap();
        assert!(res.reward_override_entity_key.is_none());

        let res = SubscriberMappingActivity::try_from(smair2).unwrap();
        assert_eq!(res.reward_override_entity_key, Some("key".to_string()));
    }
}
