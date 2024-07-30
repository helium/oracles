use std::path::Path;

use crate::{
    file_sink::FileSinkClient, file_upload::FileUpload, FileSink, FileSinkBuilder, FileType, Result,
};
use helium_proto::{
    self as proto,
    services::{packet_verifier, poc_lora, poc_mobile},
    Message,
};

#[async_trait::async_trait]
pub trait FileSinkWriteExt
where
    Self: Sized + FileSinkBytes + Send,
{
    const FILE_TYPE: FileType;
    const METRIC_SUFFIX: &'static str;

    async fn file_sink(
        target_path: &Path,
        file_upload: FileUpload,
        metric_prefix: &'static str,
    ) -> Result<(FileSinkClient<Self>, FileSink<Self>)> {
        let builder = FileSinkBuilder::peynew(
            Self::FILE_TYPE.to_string(),
            target_path,
            file_upload,
            format!("{}_{}", metric_prefix, Self::METRIC_SUFFIX),
        )
        .auto_commit(false);

        let file_sink = builder.create().await?;
        Ok(file_sink)
    }

    async fn file_sink_opts(
        target_path: &Path,
        file_upload: FileUpload,
        metric_prefix: &'static str,
        opts_fn: impl FnOnce(FileSinkBuilder) -> FileSinkBuilder + Send,
    ) -> Result<(FileSinkClient<Self>, FileSink<Self>)> {
        let builder = FileSinkBuilder::peynew(
            Self::FILE_TYPE.to_string(),
            target_path,
            file_upload,
            format!("{}_{}", metric_prefix, Self::METRIC_SUFFIX),
        );
        let builder_opts = opts_fn(builder);

        let file_sink = builder_opts.create().await?;
        Ok(file_sink)
    }
}

pub trait FileSinkBytes {
    fn as_bytes(&self) -> bytes::Bytes;
}

// As prost::Message is implemented for basically all types implementing
// MsgBytes for anything that implements prost::Message makes it so you
// cannot use a FileSink for anything that is _not_ a protobuf. So we
// provide utility implementations for Vec<u8> an String, and require all
// protos to be implemented directly, following the pattern of verifying and
// signing messages.
impl FileSinkBytes for Vec<u8> {
    fn as_bytes(&self) -> bytes::Bytes {
        bytes::Bytes::from(self.clone())
    }
}

impl FileSinkBytes for String {
    fn as_bytes(&self) -> bytes::Bytes {
        bytes::Bytes::from(self.clone())
    }
}

impl FileSinkBytes for bytes::Bytes {
    fn as_bytes(&self) -> bytes::Bytes {
        self.clone()
    }
}

macro_rules! impl_msg_bytes {
    ($msg_type:ty, $file_type:expr, $metric_suffix:expr) => {
        #[async_trait::async_trait]
        impl FileSinkWriteExt for $msg_type {
            const FILE_TYPE: FileType = $file_type;
            const METRIC_SUFFIX: &'static str = $metric_suffix;
        }
        impl_msg_bytes!($msg_type);
    };

    ($msg_type:ty) => {
        impl FileSinkBytes for $msg_type {
            fn as_bytes(&self) -> bytes::Bytes {
                bytes::Bytes::from(self.encode_to_vec())
            }
        }
    };
}

impl_msg_bytes!(
    packet_verifier::InvalidPacket,
    FileType::InvalidPacket,
    "invalid_packets"
);
impl_msg_bytes!(
    packet_verifier::ValidDataTransferSession,
    FileType::ValidDataTransferSession,
    "valid_data_transfer_session"
);
impl_msg_bytes!(
    packet_verifier::ValidPacket,
    FileType::IotValidPacket,
    "valid_packets"
);
impl_msg_bytes!(
    poc_lora::IotRewardShare,
    FileType::IotRewardShare,
    "gateway_reward_shares"
);
impl_msg_bytes!(
    poc_lora::LoraBeaconIngestReportV1,
    FileType::IotBeaconIngestReport,
    "beacon_report"
);
impl_msg_bytes!(
    poc_lora::LoraInvalidBeaconReportV1,
    FileType::IotInvalidBeaconReport,
    "invalid_beacon"
);
impl_msg_bytes!(
    poc_lora::LoraInvalidWitnessReportV1,
    FileType::IotInvalidWitnessReport,
    "invalid_witness_report"
);
impl_msg_bytes!(poc_lora::LoraPocV1, FileType::IotPoc, "valid_poc");
impl_msg_bytes!(
    poc_lora::LoraWitnessIngestReportV1,
    FileType::IotWitnessIngestReport,
    "witness_report"
);
impl_msg_bytes!(
    poc_lora::NonRewardablePacket,
    FileType::NonRewardablePacket,
    "non_rewardable_packet"
);
impl_msg_bytes!(
    poc_mobile::CellHeartbeatIngestReportV1,
    FileType::CbrsHeartbeatIngestReport,
    "heartbeat_report"
);
impl_msg_bytes!(
    poc_mobile::CoverageObjectIngestReportV1,
    FileType::CoverageObjectIngestReport,
    "coverage_object_report"
);
impl_msg_bytes!(
    poc_mobile::CoverageObjectV1,
    FileType::CoverageObject,
    "coverage_object"
);
impl_msg_bytes!(
    poc_mobile::DataTransferSessionIngestReportV1,
    FileType::DataTransferSessionIngestReport,
    "mobile_data_transfer_session_report"
);
impl_msg_bytes!(
    poc_mobile::Heartbeat,
    FileType::ValidatedHeartbeat,
    "heartbeat"
);
impl_msg_bytes!(
    poc_mobile::InvalidDataTransferIngestReportV1,
    FileType::InvalidDataTransferSessionIngestReport,
    "invalid_data_transfer_session"
);
impl_msg_bytes!(
    poc_mobile::InvalidatedRadioThresholdIngestReportV1,
    FileType::InvalidatedRadioThresholdIngestReport,
    "invalidated_radio_threshold_ingest_report"
);
impl_msg_bytes!(
    poc_mobile::MobileRewardShare,
    FileType::MobileRewardShare,
    "radio_reward_share"
);
impl_msg_bytes!(
    poc_mobile::OracleBoostingReportV1,
    FileType::OracleBoostingReport,
    "oracle_boosting_report"
);
impl_msg_bytes!(
    poc_mobile::RadioThresholdIngestReportV1,
    FileType::RadioThresholdIngestReport,
    "radio_threshold_ingest_report"
);
impl_msg_bytes!(
    poc_mobile::SeniorityUpdate,
    FileType::SeniorityUpdate,
    "seniority_update"
);
impl_msg_bytes!(
    poc_mobile::ServiceProviderBoostedRewardsBannedRadioIngestReportV1,
    FileType::SPBoostedRewardsBannedRadioIngestReport,
    "service_provider_boosted_rewards_banned_radio"
);
impl_msg_bytes!(
    poc_mobile::SpeedtestAvg,
    FileType::SpeedtestAvg,
    "speedtest_average"
);
impl_msg_bytes!(
    poc_mobile::SpeedtestIngestReportV1,
    FileType::CellSpeedtestIngestReport,
    "speedtest_report"
);
impl_msg_bytes!(
    poc_mobile::SubscriberLocationIngestReportV1,
    FileType::SubscriberLocationIngestReport,
    "subscriber_location_report"
);
impl_msg_bytes!(
    poc_mobile::VerifiedInvalidatedRadioThresholdIngestReportV1,
    FileType::VerifiedInvalidatedRadioThresholdIngestReport,
    "verified_invalidated_radio_threshold"
);
impl_msg_bytes!(
    poc_mobile::VerifiedRadioThresholdIngestReportV1,
    FileType::VerifiedRadioThresholdIngestReport,
    "verified_radio_threshold"
);
impl_msg_bytes!(
    poc_mobile::VerifiedServiceProviderBoostedRewardsBannedRadioIngestReportV1,
    FileType::VerifiedSPBoostedRewardsBannedRadioIngestReport,
    "verified_sp_boosted_rewards_ban"
);
impl_msg_bytes!(
    poc_mobile::VerifiedSpeedtest,
    FileType::VerifiedSpeedtest,
    "verified_speedtest"
);
impl_msg_bytes!(
    poc_mobile::VerifiedSubscriberLocationIngestReportV1,
    FileType::VerifiedSubscriberLocationIngestReport,
    "verified_subscriber_location"
);
impl_msg_bytes!(
    poc_mobile::WifiHeartbeatIngestReportV1,
    FileType::WifiHeartbeatIngestReport,
    "wifi_heartbeat_report"
);
impl_msg_bytes!(
    proto::BoostedHexUpdateV1,
    FileType::BoostedHexUpdate,
    "boosted_hex_update"
);
impl_msg_bytes!(
    proto::EntropyReportV1,
    FileType::EntropyReport,
    "report_submission"
);
impl_msg_bytes!(proto::PriceReportV1);
impl_msg_bytes!(
    proto::RewardManifest,
    FileType::RewardManifest,
    "reward_manifest"
);
