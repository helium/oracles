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

#[async_trait::async_trait]
pub trait FileSinkWriteExt
where
    Self: Sized + MsgBytes + Send,
{
    const FILE_TYPE: FileType;
    const METRIC_SUFFIX: &'static str;

    // The `auto_commit` option and `roll_time` option are incompatible with
    // each other. It doesn't make sense to roll a file every so often _and_
    // commit it every time something is written. If a roll_time is provided,
    // `auto_commit` is set to false.
    async fn file_sink(
        target_path: &Path,
        file_upload: FileUpload,
        roll_time: Option<Duration>,
        metric_prefix: &str,
    ) -> Result<(FileSinkClient<Self>, FileSink<Self>)> {
        let builder = FileSinkBuilder::new(
            Self::FILE_TYPE.to_string(),
            target_path,
            file_upload,
            format!("{}_{}", metric_prefix, Self::METRIC_SUFFIX),
        );

        let builder = if let Some(duration) = roll_time {
            builder.auto_commit(false).roll_time(duration)
        } else {
            builder.auto_commit(true)
        };

        let file_sink = builder.create().await?;
        Ok(file_sink)
    }
}

macro_rules! impl_file_sink {
    ($msg_type:ty, $file_type:expr, $metric_suffix:expr) => {
        #[async_trait::async_trait]
        impl FileSinkWriteExt for $msg_type {
            const FILE_TYPE: FileType = $file_type;
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
    FileType::InvalidPacket,
    "invalid_packets"
);
impl_file_sink!(
    packet_verifier::ValidDataTransferSession,
    FileType::ValidDataTransferSession,
    "valid_data_transfer_session"
);
impl_file_sink!(
    packet_verifier::ValidPacket,
    FileType::IotValidPacket,
    "valid_packets"
);
impl_file_sink!(
    poc_lora::IotRewardShare,
    FileType::IotRewardShare,
    "gateway_reward_shares"
);
impl_file_sink!(
    poc_lora::LoraBeaconIngestReportV1,
    FileType::IotBeaconIngestReport,
    "beacon_report"
);
impl_file_sink!(
    poc_lora::LoraInvalidBeaconReportV1,
    FileType::IotInvalidBeaconReport,
    "invalid_beacon"
);
impl_file_sink!(
    poc_lora::LoraInvalidWitnessReportV1,
    FileType::IotInvalidWitnessReport,
    "invalid_witness_report"
);
impl_file_sink!(poc_lora::LoraPocV1, FileType::IotPoc, "valid_poc");
impl_file_sink!(
    poc_lora::LoraWitnessIngestReportV1,
    FileType::IotWitnessIngestReport,
    "witness_report"
);
impl_file_sink!(
    poc_lora::NonRewardablePacket,
    FileType::NonRewardablePacket,
    "non_rewardable_packet"
);
impl_file_sink!(
    poc_mobile::CellHeartbeatIngestReportV1,
    FileType::CbrsHeartbeatIngestReport,
    "heartbeat_report"
);
impl_file_sink!(
    poc_mobile::CoverageObjectIngestReportV1,
    FileType::CoverageObjectIngestReport,
    "coverage_object_report"
);
impl_file_sink!(
    poc_mobile::CoverageObjectV1,
    FileType::CoverageObject,
    "coverage_object"
);
impl_file_sink!(
    poc_mobile::DataTransferSessionIngestReportV1,
    FileType::DataTransferSessionIngestReport,
    "mobile_data_transfer_session_report"
);
impl_file_sink!(
    poc_mobile::Heartbeat,
    FileType::ValidatedHeartbeat,
    "heartbeat"
);
impl_file_sink!(
    poc_mobile::InvalidDataTransferIngestReportV1,
    FileType::InvalidDataTransferSessionIngestReport,
    "invalid_data_transfer_session"
);
impl_file_sink!(
    poc_mobile::InvalidatedRadioThresholdIngestReportV1,
    FileType::InvalidatedRadioThresholdIngestReport,
    "invalidated_radio_threshold_ingest_report"
);
impl_file_sink!(
    poc_mobile::MobileRewardShare,
    FileType::MobileRewardShare,
    "radio_reward_share"
);
impl_file_sink!(
    poc_mobile::OracleBoostingReportV1,
    FileType::OracleBoostingReport,
    "oracle_boosting_report"
);
impl_file_sink!(
    poc_mobile::RadioThresholdIngestReportV1,
    FileType::RadioThresholdIngestReport,
    "radio_threshold_ingest_report"
);
impl_file_sink!(
    poc_mobile::SeniorityUpdate,
    FileType::SeniorityUpdate,
    "seniority_update"
);
impl_file_sink!(
    poc_mobile::ServiceProviderBoostedRewardsBannedRadioIngestReportV1,
    FileType::SPBoostedRewardsBannedRadioIngestReport,
    "service_provider_boosted_rewards_banned_radio"
);
impl_file_sink!(
    poc_mobile::SpeedtestAvg,
    FileType::SpeedtestAvg,
    "speedtest_average"
);
impl_file_sink!(
    poc_mobile::SpeedtestIngestReportV1,
    FileType::CellSpeedtestIngestReport,
    "speedtest_report"
);
impl_file_sink!(
    poc_mobile::SubscriberLocationIngestReportV1,
    FileType::SubscriberLocationIngestReport,
    "subscriber_location_report"
);
impl_file_sink!(
    poc_mobile::SubscriberVerifiedMappingEventIngestReportV1,
    FileType::SubscriberVerifiedMappingEventIngestReport,
    "subscriber_verified_mapping_event_ingest_report"
);
impl_file_sink!(
    poc_mobile::VerifiedInvalidatedRadioThresholdIngestReportV1,
    FileType::VerifiedInvalidatedRadioThresholdIngestReport,
    "verified_invalidated_radio_threshold"
);
impl_file_sink!(
    poc_mobile::VerifiedRadioThresholdIngestReportV1,
    FileType::VerifiedRadioThresholdIngestReport,
    "verified_radio_threshold"
);
impl_file_sink!(
    poc_mobile::VerifiedServiceProviderBoostedRewardsBannedRadioIngestReportV1,
    FileType::VerifiedSPBoostedRewardsBannedRadioIngestReport,
    "verified_sp_boosted_rewards_ban"
);
impl_file_sink!(
    poc_mobile::VerifiedSpeedtest,
    FileType::VerifiedSpeedtest,
    "verified_speedtest"
);
impl_file_sink!(
    poc_mobile::VerifiedSubscriberLocationIngestReportV1,
    FileType::VerifiedSubscriberLocationIngestReport,
    "verified_subscriber_location"
);
impl_file_sink!(
    poc_mobile::VerifiedSubscriberVerifiedMappingEventIngestReportV1,
    FileType::VerifiedSubscriberVerifiedMappingEventIngestReport,
    "verified_subscriber_verified_mapping_event_ingest_report"
);
impl_file_sink!(
    poc_mobile::WifiHeartbeatIngestReportV1,
    FileType::WifiHeartbeatIngestReport,
    "wifi_heartbeat_report"
);
impl_file_sink!(
    proto::BoostedHexUpdateV1,
    FileType::BoostedHexUpdate,
    "boosted_hex_update"
);
impl_file_sink!(
    proto::EntropyReportV1,
    FileType::EntropyReport,
    "report_submission"
);
impl_file_sink!(
    proto::PriceReportV1,
    FileType::PriceReport,
    "report_submission"
);
impl_file_sink!(
    proto::RewardManifest,
    FileType::RewardManifest,
    "reward_manifest"
);
