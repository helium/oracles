use std::time::Instant;

use chrono::{DateTime, Utc};
use file_store::{
    file_info_poller::{FileInfoStream, LookbackBehavior},
    file_sink::FileSinkClient,
    file_source,
    file_upload::FileUpload,
    traits::{
        FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt, TimestampDecode,
        TimestampEncode,
    },
    FileStore, FileType,
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
    authorization_verifier: AV,
    entity_verifier: EV,
    stream_receiver: Receiver<FileInfoStream<SubscriberMappingActivityIngestReportV1>>,
    verified_sink: FileSinkClient<VerifiedSubscriberMappingActivityReportV1>,
}

impl<AV, EV> SubscriberMappingActivityDaemon<AV, EV>
where
    AV: AuthorizationVerifier,
    EV: EntityVerifier + Send + Sync + 'static,
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
            authorization_verifier,
            entity_verifier,
            stream_receiver,
            verified_sink,
        }
    }

    pub async fn create_managed_task(
        pool: Pool<Postgres>,
        settings: &Settings,
        authorization_verifier: AV,
        entity_verifier: EV,
        file_store: FileStore,
        file_upload: FileUpload,
    ) -> anyhow::Result<impl ManagedTask> {
        let (stream_reciever, stream_server) = file_source::Continuous::prost_source()
            .state(pool.clone())
            .store(file_store)
            .lookback(LookbackBehavior::StartAfter(settings.start_after))
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
            .map(SubscriberMappingActivity::try_from)
            .and_then(|sma| async move {
                let status = self.verify_activity(&sma).await?;
                Ok((sma, status))
            })
            .and_then(|(sma, status)| self.write_verified_report(sma, status))
            .try_filter_map(|(sma, status)| is_valid(sma, status));

        db::save(&mut transaction, activity_stream).await?;
        self.verified_sink.commit().await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn write_verified_report(
        &self,
        mut activity: SubscriberMappingActivity,
        status: SubscriberReportVerificationStatus,
    ) -> anyhow::Result<(
        SubscriberMappingActivity,
        SubscriberReportVerificationStatus,
    )> {
        let verified_proto = VerifiedSubscriberMappingActivityReportV1 {
            report: activity.take_original(),
            status: status as i32,
            timestamp: Utc::now().encode_timestamp_millis(),
        };

        self.verified_sink
            .write(verified_proto, &[("status", status.as_str_name())])
            .await?;

        Ok((activity, status))
    }

    async fn verify_activity(
        &self,
        activity: &SubscriberMappingActivity,
    ) -> anyhow::Result<SubscriberReportVerificationStatus> {
        if !self
            .verify_known_carrier_key(&activity.carrier_pub_key)
            .await?
        {
            return Ok(SubscriberReportVerificationStatus::InvalidCarrierKey);
        };
        if !self.verify_subscriber_id(&activity.subscriber_id).await? {
            return Ok(SubscriberReportVerificationStatus::InvalidSubscriberId);
        };
        Ok(SubscriberReportVerificationStatus::Valid)
    }

    async fn verify_known_carrier_key(&self, public_key: &PublicKeyBinary) -> anyhow::Result<bool> {
        self.authorization_verifier
            .verify_authorized_key(public_key, NetworkKeyRole::MobileCarrier)
            .await
            .map_err(anyhow::Error::from)
    }

    async fn verify_subscriber_id(&self, subscriber_id: &[u8]) -> anyhow::Result<bool> {
        self.entity_verifier
            .verify_rewardable_entity(subscriber_id)
            .await
            .map_err(anyhow::Error::from)
    }
}

async fn is_valid(
    activity: SubscriberMappingActivity,
    status: SubscriberReportVerificationStatus,
) -> anyhow::Result<Option<SubscriberMappingActivity>> {
    if status == SubscriberReportVerificationStatus::Valid {
        Ok(Some(activity))
    } else {
        Ok(None)
    }
}

impl<AV, EV> ManagedTask for SubscriberMappingActivityDaemon<AV, EV>
where
    AV: AuthorizationVerifier,
    EV: EntityVerifier + Send + Sync + 'static,
{
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> futures::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(self.run(shutdown))
        // let handle = tokio::spawn(async move { self.run(shutdown).await });
        // Box::pin(
        //     handle
        //         .map_err(anyhow::Error::from)
        //         .and_then(|result| async move { result }),
        // )
    }
}

pub struct SubscriberMappingActivity {
    subscriber_id: Vec<u8>,
    discovery_reward_shares: u64,
    verification_reward_shares: u64,
    received_timestamp: DateTime<Utc>,
    carrier_pub_key: PublicKeyBinary,
    original: Option<SubscriberMappingActivityIngestReportV1>,
}

impl SubscriberMappingActivity {
    fn take_original(&mut self) -> Option<SubscriberMappingActivityIngestReportV1> {
        self.original.take()
    }
}

impl TryFrom<SubscriberMappingActivityIngestReportV1> for SubscriberMappingActivity {
    type Error = anyhow::Error;

    fn try_from(value: SubscriberMappingActivityIngestReportV1) -> Result<Self, Self::Error> {
        let original = value.clone();
        let report = value
            .report
            .ok_or_else(|| anyhow::anyhow!("SubscriberMappingActivityReqV1 not found"))?;

        Ok(Self {
            subscriber_id: report.subscriber_id,
            discovery_reward_shares: report.discovery_reward_shares,
            verification_reward_shares: report.verification_reward_shares,
            received_timestamp: value.received_timestamp.to_timestamp_millis()?,
            carrier_pub_key: PublicKeyBinary::from(report.carrier_pub_key),
            original: Some(original),
        })
    }
}
