use crate::traits::MsgBytes;
use helium_proto::{
    self as proto,
    services::{chain_rewardable_entities, packet_verifier, poc_lora, poc_mobile},
    Message,
};

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
impl_msg_bytes!(poc_mobile::VerifiedDataTransferIngestReportV1);
impl_msg_bytes!(poc_mobile::InvalidatedRadioThresholdIngestReportV1);
impl_msg_bytes!(poc_mobile::MobileRewardShare);
impl_msg_bytes!(poc_mobile::OracleBoostingReportV1);
impl_msg_bytes!(poc_mobile::RadioThresholdIngestReportV1);
impl_msg_bytes!(poc_mobile::SeniorityUpdate);
impl_msg_bytes!(poc_mobile::ServiceProviderBoostedRewardsBannedRadioIngestReportV1);
impl_msg_bytes!(poc_mobile::SpeedtestAvg);
impl_msg_bytes!(poc_mobile::SpeedtestIngestReportV1);
impl_msg_bytes!(poc_mobile::SubscriberLocationIngestReportV1);
impl_msg_bytes!(poc_mobile::SubscriberVerifiedMappingEventIngestReportV1);
impl_msg_bytes!(poc_mobile::VerifiedInvalidatedRadioThresholdIngestReportV1);
impl_msg_bytes!(poc_mobile::VerifiedRadioThresholdIngestReportV1);
impl_msg_bytes!(poc_mobile::VerifiedServiceProviderBoostedRewardsBannedRadioIngestReportV1);
impl_msg_bytes!(poc_mobile::VerifiedSpeedtest);
impl_msg_bytes!(poc_mobile::VerifiedSubscriberLocationIngestReportV1);
impl_msg_bytes!(poc_mobile::VerifiedSubscriberVerifiedMappingEventIngestReportV1);
impl_msg_bytes!(poc_mobile::WifiHeartbeatIngestReportV1);
impl_msg_bytes!(poc_mobile::HexUsageStatsIngestReportV1);
impl_msg_bytes!(poc_mobile::RadioUsageStatsIngestReportV1);
impl_msg_bytes!(poc_mobile::UniqueConnectionsIngestReportV1);
impl_msg_bytes!(poc_mobile::VerifiedUniqueConnectionsIngestReportV1);
impl_msg_bytes!(poc_mobile::BanIngestReportV1);
impl_msg_bytes!(poc_mobile::VerifiedBanIngestReportV1);
impl_msg_bytes!(proto::BoostedHexUpdateV1);
impl_msg_bytes!(proto::EntropyReportV1);
impl_msg_bytes!(proto::PriceReportV1);
impl_msg_bytes!(proto::RewardManifest);
impl_msg_bytes!(poc_mobile::SubscriberMappingActivityIngestReportV1);
impl_msg_bytes!(poc_mobile::VerifiedSubscriberMappingActivityReportV1);
impl_msg_bytes!(chain_rewardable_entities::MobileHotspotChangeReportV1);
impl_msg_bytes!(chain_rewardable_entities::IotHotspotChangeReportV1);
impl_msg_bytes!(chain_rewardable_entities::EntityOwnershipChangeReportV1);
impl_msg_bytes!(chain_rewardable_entities::EntityRewardDestinationChangeReportV1);
impl_msg_bytes!(poc_mobile::EnabledCarriersInfoReportV1);
