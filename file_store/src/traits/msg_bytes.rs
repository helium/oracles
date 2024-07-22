use helium_proto::{
    self as proto,
    services::{packet_verifier, poc_lora, poc_mobile},
    Message,
};

pub trait MsgBytes {
    fn as_bytes(&self) -> bytes::Bytes;
}

// As prost::Message is implemented for basically all types implementing
// MsgBytes for anything that implements prost::Message makes it so you
// cannot use a FileSink for anything that is _not_ a protobuf. So we
// provide utility implementations for Vec<u8> an String, and require all
// protos to be implemented directly, following the pattern of verifying and
// signing messages.
impl MsgBytes for Vec<u8> {
    fn as_bytes(&self) -> bytes::Bytes {
        bytes::Bytes::from(self.clone())
    }
}

impl MsgBytes for String {
    fn as_bytes(&self) -> bytes::Bytes {
        bytes::Bytes::from(self.clone())
    }
}

impl MsgBytes for bytes::Bytes {
    fn as_bytes(&self) -> bytes::Bytes {
        self.clone()
    }
}

macro_rules! impl_msg_bytes {
    ($msg_type:ty) => {
        impl MsgBytes for $msg_type {
            fn as_bytes(&self) -> bytes::Bytes {
                bytes::Bytes::from(self.encode_to_vec())
            }
        }
    };
}

impl_msg_bytes!(packet_verifier::InvalidPacket);
impl_msg_bytes!(packet_verifier::ValidDataTransferSession);
impl_msg_bytes!(packet_verifier::ValidPacket);
impl_msg_bytes!(poc_lora::IotRewardShare);
impl_msg_bytes!(poc_lora::LoraBeaconIngestReportV1);
impl_msg_bytes!(poc_lora::LoraInvalidBeaconReportV1);
impl_msg_bytes!(poc_lora::LoraInvalidWitnessReportV1);
impl_msg_bytes!(poc_lora::LoraPocV1);
impl_msg_bytes!(poc_lora::LoraWitnessIngestReportV1);
impl_msg_bytes!(poc_lora::NonRewardablePacket);
impl_msg_bytes!(poc_mobile::CellHeartbeatIngestReportV1);
impl_msg_bytes!(poc_mobile::CoverageObjectIngestReportV1);
impl_msg_bytes!(poc_mobile::CoverageObjectV1);
impl_msg_bytes!(poc_mobile::DataTransferSessionIngestReportV1);
impl_msg_bytes!(poc_mobile::Heartbeat);
impl_msg_bytes!(poc_mobile::InvalidDataTransferIngestReportV1);
impl_msg_bytes!(poc_mobile::InvalidatedRadioThresholdIngestReportV1);
impl_msg_bytes!(poc_mobile::MobileRewardShare);
impl_msg_bytes!(poc_mobile::OracleBoostingReportV1);
impl_msg_bytes!(poc_mobile::RadioThresholdIngestReportV1);
impl_msg_bytes!(poc_mobile::SeniorityUpdate);
impl_msg_bytes!(poc_mobile::ServiceProviderBoostedRewardsBannedRadioIngestReportV1);
impl_msg_bytes!(poc_mobile::SpeedtestAvg);
impl_msg_bytes!(poc_mobile::SpeedtestIngestReportV1);
impl_msg_bytes!(poc_mobile::SubscriberLocationIngestReportV1);
impl_msg_bytes!(poc_mobile::VerifiedInvalidatedRadioThresholdIngestReportV1);
impl_msg_bytes!(poc_mobile::VerifiedRadioThresholdIngestReportV1);
impl_msg_bytes!(poc_mobile::VerifiedServiceProviderBoostedRewardsBannedRadioIngestReportV1);
impl_msg_bytes!(poc_mobile::VerifiedSpeedtest);
impl_msg_bytes!(poc_mobile::VerifiedSubscriberLocationIngestReportV1);
impl_msg_bytes!(poc_mobile::WifiHeartbeatIngestReportV1);
impl_msg_bytes!(proto::BoostedHexUpdateV1);
impl_msg_bytes!(proto::EntropyReportV1);
impl_msg_bytes!(proto::PriceReportV1);
impl_msg_bytes!(proto::RewardManifest);
