use std::{path::Path, time::Duration};

use crate::{
    file_sink::{FileSinkClient, DEFAULT_SINK_ROLL_SECS},
    file_upload::FileUpload,
    traits::msg_bytes::MsgBytes,
    FileSink, FileSinkBuilder, FileType, Result,
};
use helium_proto::{
    self as proto,
    services::{packet_verifier, poc_lora, poc_mobile},
    Message,
};

pub const DEFAULT_ROLL_TIME: Duration = Duration::from_secs(DEFAULT_SINK_ROLL_SECS);

#[derive(Copy, Clone, PartialEq, Eq)]
pub enum FileSinkCommitStrategy {
    /// Writer must manually call [`FileSinkClient::commit()`] for files to be uploaded.
    Manual,
    /// Files will be automatically uploaded when
    /// [`FileSinkBuilder::max_size()`] is exceeded, or [`DEFAULT_ROLL_TIME`] has elapsed.
    Automatic,
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub enum FileSinkRollTime {
    /// Default is 3 minutes
    Default,
    Duration(Duration),
}

#[async_trait::async_trait]
pub trait FileSinkWriteExt
where
    Self: Sized + MsgBytes + Send,
{
    const FILE_PREFIX: &'static str;
    const METRIC_SUFFIX: &'static str;

    async fn file_sink(
        target_path: &Path,
        file_upload: FileUpload,
        commit_strategy: FileSinkCommitStrategy,
        roll_time: FileSinkRollTime,
        metric_prefix: &str,
    ) -> Result<(FileSinkClient<Self>, FileSink<Self>)> {
        let builder = FileSinkBuilder::new(
            Self::FILE_PREFIX.to_string(),
            target_path,
            file_upload,
            format!("{}_{}", metric_prefix, Self::METRIC_SUFFIX),
        );

        let builder = match commit_strategy {
            FileSinkCommitStrategy::Manual => {
                builder.auto_commit(false).roll_time(DEFAULT_ROLL_TIME)
            }
            FileSinkCommitStrategy::Automatic => {
                builder.auto_commit(true).roll_time(DEFAULT_ROLL_TIME)
            }
        };

        let builder = match roll_time {
            FileSinkRollTime::Duration(duration) => builder.roll_time(duration),
            FileSinkRollTime::Default => builder.roll_time(DEFAULT_ROLL_TIME),
        };

        let file_sink = builder.create().await?;
        Ok(file_sink)
    }
}

macro_rules! impl_file_sink {
    ($msg_type:ty, $file_prefix:expr, $metric_suffix:expr) => {
        #[async_trait::async_trait]
        impl FileSinkWriteExt for $msg_type {
            const FILE_PREFIX: &'static str = $file_prefix;
            const METRIC_SUFFIX: &'static str = $metric_suffix;
        }

        impl MsgBytes for $msg_type {
            fn as_bytes(&self) -> bytes::Bytes {
                bytes::Bytes::from(self.encode_to_vec())
            }
        }
    };
}

impl_file_sink!(
    packet_verifier::InvalidPacket,
    FileType::InvalidPacket.to_str(),
    "invalid_packets"
);
impl_file_sink!(
    packet_verifier::ValidDataTransferSession,
    FileType::ValidDataTransferSession.to_str(),
    "valid_data_transfer_session"
);
impl_file_sink!(
    packet_verifier::ValidPacket,
    FileType::IotValidPacket.to_str(),
    "valid_packets"
);
impl_file_sink!(
    poc_lora::IotRewardShare,
    FileType::IotRewardShare.to_str(),
    "gateway_reward_shares"
);
impl_file_sink!(
    poc_lora::LoraBeaconIngestReportV1,
    FileType::IotBeaconIngestReport.to_str(),
    "beacon_report"
);
impl_file_sink!(
    poc_lora::LoraInvalidBeaconReportV1,
    FileType::IotInvalidBeaconReport.to_str(),
    "invalid_beacon"
);
impl_file_sink!(
    poc_lora::LoraInvalidWitnessReportV1,
    FileType::IotInvalidWitnessReport.to_str(),
    "invalid_witness_report"
);
impl_file_sink!(poc_lora::LoraPocV1, FileType::IotPoc.to_str(), "valid_poc");
impl_file_sink!(
    poc_lora::LoraWitnessIngestReportV1,
    FileType::IotWitnessIngestReport.to_str(),
    "witness_report"
);
impl_file_sink!(
    poc_lora::NonRewardablePacket,
    FileType::NonRewardablePacket.to_str(),
    "non_rewardable_packet"
);
impl_file_sink!(
    poc_mobile::CellHeartbeatIngestReportV1,
    FileType::CbrsHeartbeatIngestReport.to_str(),
    "heartbeat_report"
);
impl_file_sink!(
    poc_mobile::CoverageObjectIngestReportV1,
    FileType::CoverageObjectIngestReport.to_str(),
    "coverage_object_report"
);
impl_file_sink!(
    poc_mobile::CoverageObjectV1,
    FileType::CoverageObject.to_str(),
    "coverage_object"
);
impl_file_sink!(
    poc_mobile::DataTransferSessionIngestReportV1,
    FileType::DataTransferSessionIngestReport.to_str(),
    "mobile_data_transfer_session_report"
);
impl_file_sink!(
    poc_mobile::Heartbeat,
    FileType::ValidatedHeartbeat.to_str(),
    "heartbeat"
);
impl_file_sink!(
    poc_mobile::InvalidDataTransferIngestReportV1,
    FileType::InvalidDataTransferSessionIngestReport.to_str(),
    "invalid_data_transfer_session"
);
impl_file_sink!(
    poc_mobile::VerifiedDataTransferIngestReportV1,
    FileType::VerifiedDataTransferSession.to_str(),
    "verified_data_transfer_session"
);
impl_file_sink!(
    poc_mobile::InvalidatedRadioThresholdIngestReportV1,
    FileType::InvalidatedRadioThresholdIngestReport.to_str(),
    "invalidated_radio_threshold_ingest_report"
);
impl_file_sink!(
    poc_mobile::MobileRewardShare,
    FileType::MobileRewardShare.to_str(),
    "radio_reward_share"
);
impl_file_sink!(
    poc_mobile::OracleBoostingReportV1,
    FileType::OracleBoostingReport.to_str(),
    "oracle_boosting_report"
);
impl_file_sink!(
    poc_mobile::RadioThresholdIngestReportV1,
    FileType::RadioThresholdIngestReport.to_str(),
    "radio_threshold_ingest_report"
);
impl_file_sink!(
    poc_mobile::SeniorityUpdate,
    FileType::SeniorityUpdate.to_str(),
    "seniority_update"
);
impl_file_sink!(
    poc_mobile::ServiceProviderBoostedRewardsBannedRadioIngestReportV1,
    FileType::SPBoostedRewardsBannedRadioIngestReport.to_str(),
    "service_provider_boosted_rewards_banned_radio"
);
impl_file_sink!(
    poc_mobile::SpeedtestAvg,
    FileType::SpeedtestAvg.to_str(),
    "speedtest_average"
);
impl_file_sink!(
    poc_mobile::SpeedtestIngestReportV1,
    FileType::CellSpeedtestIngestReport.to_str(),
    "speedtest_report"
);
impl_file_sink!(
    poc_mobile::SubscriberLocationIngestReportV1,
    FileType::SubscriberLocationIngestReport.to_str(),
    "subscriber_location_report"
);
impl_file_sink!(
    poc_mobile::SubscriberVerifiedMappingEventIngestReportV1,
    FileType::SubscriberVerifiedMappingEventIngestReport.to_str(),
    "subscriber_verified_mapping_event_ingest_report"
);
impl_file_sink!(
    poc_mobile::VerifiedInvalidatedRadioThresholdIngestReportV1,
    FileType::VerifiedInvalidatedRadioThresholdIngestReport.to_str(),
    "verified_invalidated_radio_threshold"
);
impl_file_sink!(
    poc_mobile::VerifiedRadioThresholdIngestReportV1,
    FileType::VerifiedRadioThresholdIngestReport.to_str(),
    "verified_radio_threshold"
);
impl_file_sink!(
    poc_mobile::VerifiedServiceProviderBoostedRewardsBannedRadioIngestReportV1,
    FileType::VerifiedSPBoostedRewardsBannedRadioIngestReport.to_str(),
    "verified_sp_boosted_rewards_ban"
);
impl_file_sink!(
    poc_mobile::VerifiedSpeedtest,
    FileType::VerifiedSpeedtest.to_str(),
    "verified_speedtest"
);
impl_file_sink!(
    poc_mobile::VerifiedSubscriberLocationIngestReportV1,
    FileType::VerifiedSubscriberLocationIngestReport.to_str(),
    "verified_subscriber_location"
);
impl_file_sink!(
    poc_mobile::VerifiedSubscriberVerifiedMappingEventIngestReportV1,
    FileType::VerifiedSubscriberVerifiedMappingEventIngestReport.to_str(),
    "verified_subscriber_verified_mapping_event_ingest_report"
);
impl_file_sink!(
    poc_mobile::WifiHeartbeatIngestReportV1,
    FileType::WifiHeartbeatIngestReport.to_str(),
    "wifi_heartbeat_report"
);
impl_file_sink!(
    poc_mobile::HexUsageStatsIngestReportV1,
    FileType::HexUsageStatsIngestReport.to_str(),
    "hex_usage_counts_ingest_report"
);
impl_file_sink!(
    poc_mobile::RadioUsageStatsIngestReportV1,
    FileType::RadioUsageStatsIngestReport.to_str(),
    "hotspot_usage_counts_ingest_report"
);
impl_file_sink!(
    poc_mobile::UniqueConnectionsIngestReportV1,
    FileType::UniqueConnectionsReport.to_str(),
    "unique_connections_report"
);
impl_file_sink!(
    poc_mobile::VerifiedUniqueConnectionsIngestReportV1,
    FileType::VerifiedUniqueConnectionsReport.to_str(),
    "verified_unique_connections_report"
);
impl_file_sink!(
    poc_mobile::BanIngestReportV1,
    FileType::MobileBanReport.to_str(),
    "mobile_ban_report"
);
impl_file_sink!(
    poc_mobile::VerifiedBanIngestReportV1,
    FileType::VerifiedMobileBanReport.to_str(),
    "verified_mobile_ban_report"
);
impl_file_sink!(
    proto::BoostedHexUpdateV1,
    FileType::BoostedHexUpdate.to_str(),
    "boosted_hex_update"
);
impl_file_sink!(
    proto::EntropyReportV1,
    FileType::EntropyReport.to_str(),
    "report_submission"
);
impl_file_sink!(
    proto::PriceReportV1,
    FileType::PriceReport.to_str(),
    "report_submission"
);
impl_file_sink!(
    proto::RewardManifest,
    FileType::RewardManifest.to_str(),
    "reward_manifest"
);
impl_file_sink!(
    poc_mobile::SubscriberMappingActivityIngestReportV1,
    FileType::SubscriberMappingActivityIngestReport.to_str(),
    "subscriber_mapping_activity_ingest_report"
);
impl_file_sink!(
    poc_mobile::VerifiedSubscriberMappingActivityReportV1,
    FileType::VerifiedSubscriberMappingActivityReport.to_str(),
    "verified_subscriber_mapping_activity_report"
);
impl_file_sink!(
    poc_mobile::EnabledCarriersInfoReportV1,
    FileType::EnabledCarriersInfoReport.to_str(),
    "enabled_carriers_report"
);
