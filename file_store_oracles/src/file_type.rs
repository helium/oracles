use file_store::Error;
use serde::Serialize;
use std::{fmt, io, str::FromStr};

pub const HEX_USAGE_STATS_INGEST_REPORT: &str = "hex_usage_stats_ingest_report";
pub const RADIO_USAGE_STATS_INGEST_REPORT: &str = "radio_usage_stats_ingest_report";
pub const HEX_USAGE_STATS_REQ: &str = "hex_usage_stats_req";
pub const RADIO_USAGE_STATS_REQ: &str = "radio_usage_stats_req";

pub const INVALIDATED_RADIO_THRESHOLD_REQ: &str = "invalidated_radio_threshold_req";
pub const INVALIDATED_RADIO_THRESHOLD_INGEST_REPORT: &str =
    "invalidated_radio_threshold_ingest_report";
pub const VERIFIED_INVALIDATED_RADIO_THRESHOLD_INGEST_REPORT: &str =
    "verified_invalidated_radio_threshold_report";
pub const RADIO_THRESHOLD_REQ: &str = "radio_threshold_req";
pub const RADIO_THRESHOLD_INGEST_REPORT: &str = "radio_threshold_ingest_report";
pub const VERIFIED_RADIO_THRESHOLD_INGEST_REPORT: &str = "verified_radio_threshold_report";
pub const SUBSCRIBER_LOCATION_REQ: &str = "subscriber_location_req";
pub const SUBSCRIBER_LOCATION_INGEST_REPORT: &str = "subscriber_location_report";
pub const VERIFIED_SUBSCRIBER_LOCATION_INGEST_REPORT: &str = "verified_subscriber_location_report";
pub const CBRS_HEARTBEAT: &str = "cbrs_heartbeat";
pub const WIFI_HEARTBEAT: &str = "wifi_heartbeat";
pub const CELL_SPEEDTEST: &str = "cell_speedtest";
pub const VERIFIED_SPEEDTEST: &str = "verified_speedtest";
pub const CELL_HEARTBEAT_INGEST_REPORT: &str = "heartbeat_report";
pub const WIFI_HEARTBEAT_INGEST_REPORT: &str = "wifi_heartbeat_report";
pub const CELL_SPEEDTEST_INGEST_REPORT: &str = "speedtest_report";
pub const ENTROPY: &str = "entropy";
pub const SUBNETWORK_REWARDS: &str = "subnetwork_rewards";
pub const ENTROPY_REPORT: &str = "entropy_report";
pub const IOT_BEACON_INGEST_REPORT: &str = "iot_beacon_ingest_report";
pub const IOT_WITNESS_INGEST_REPORT: &str = "iot_witness_ingest_report";
pub const IOT_POC: &str = "iot_poc";
pub const IOT_INVALID_BEACON_REPORT: &str = "iot_invalid_beacon";
pub const IOT_INVALID_WITNESS_REPORT: &str = "iot_invalid_witness";
pub const SPEEDTEST_AVG: &str = "speedtest_avg";
pub const VALIDATED_HEARTBEAT: &str = "validated_heartbeat";
pub const SIGNED_POC_RECEIPT_TXN: &str = "signed_poc_receipt_txn";
pub const RADIO_REWARD_SHARE: &str = "radio_reward_share";
pub const REWARD_MANIFEST: &str = "network_reward_manifest_v1";
pub const IOT_PACKET_REPORT: &str = "packetreport";
pub const IOT_VALID_PACKET: &str = "iot_valid_packet";
pub const INVALID_PACKET: &str = "invalid_packet";
pub const NON_REWARDABLE_PACKET: &str = "non_rewardable_packet";
pub const IOT_REWARD_SHARE: &str = "iot_network_reward_shares_v1";
pub const DATA_TRANSFER_SESSION_INGEST_REPORT: &str = "data_transfer_session_ingest_report";
pub const INVALID_DATA_TRANSFER_SESSION_INGEST_REPORT: &str =
    "invalid_data_transfer_session_ingest_report";
pub const VALID_DATA_TRANSFER_SESSION: &str = "valid_data_transfer_session";
pub const VERIFIED_DATA_TRANSFER_SESSION: &str = "verified_data_transfer_session";
pub const PRICE_REPORT: &str = "price_report";
pub const MOBILE_REWARD_SHARE: &str = "mobile_network_reward_shares_v1";
pub const MAPPER_MSG: &str = "mapper_msg";
pub const COVERAGE_OBJECT: &str = "coverage_object";
pub const COVERAGE_OBJECT_INGEST_REPORT: &str = "coverage_object_ingest_report";
pub const SENIORITY_UPDATE: &str = "seniority_update";
pub const BOOSTED_HEX_UPDATE: &str = "boosted_hex_update";
pub const ORACLE_BOOSTING_REPORT: &str = "oracle_boosting_report";
pub const URBANIZATION_DATA_SET: &str = "urbanization";
pub const FOOTFALL_DATA_SET: &str = "footfall";
pub const LANDTYPE_DATA_SET: &str = "landtype";
pub const SP_BOOSTED_REWARDS_BANNED_RADIO: &str = "service_provider_boosted_rewards_banned_radio";
pub const VERIFIED_SP_BOOSTED_REWARDS_BANNED_RADIO: &str =
    "verified_service_provider_boosted_rewards_banned_radio";
pub const SUBSCRIBER_VERIFIED_MAPPING_INGEST_REPORT: &str =
    "subscriber_verified_mapping_ingest_report";
pub const VERIFIED_SUBSCRIBER_VERIFIED_MAPPING_INGEST_REPORT: &str =
    "verified_subscriber_verified_mapping_ingest_report";
pub const PROMOTION_REWARD_INGEST_REPORT: &str = "promotion_reward_ingest_report";
pub const VERIFIED_PROMOTION_REWARD: &str = "verified_promotion_reward";
pub const SERVICE_PROVIDER_PROMOTION_FUND: &str = "service_provider_promotion_fund";
pub const UNIQUE_CONNECTIONS_REPORT: &str = "unique_connections_report";
pub const VERIFIED_UNIQUE_CONNECTIONS_REPORT: &str = "verified_unique_connections_report";
pub const SUBSCRIBER_MAPPING_ACTIVITY_INGEST_REPORT: &str =
    "subscriber_mapping_activity_ingest_report";
pub const VERIFIED_SUBSCRIBER_MAPPING_ACTIVITY_REPORT: &str =
    "verified_subscriber_mapping_activity_report";
pub const MOBILE_BAN_REPORT: &str = "mobile_ban_report";
pub const VERIFIED_MOBILE_BAN_REPORT: &str = "verified_mobile_ban_report";
pub const MOBILE_HOTSPOT_CHANGE_REPORT: &str = "mobile_hotspot_change_report";
pub const IOT_HOTSPOT_CHANGE_REPORT: &str = "iot_hotspot_change_report";
pub const ENTITY_OWNERSHIP_CHANGE_REPORT: &str = "entity_ownership_change_report";
pub const ENTITY_REWARD_DESTINATION_CHANGE_REPORT: &str = "entity_reward_destination_change_report";
pub const ENABLED_CARRIERS_REPORT: &str = "enabled_carriers_report";

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Copy, strum::EnumCount)]
#[serde(rename_all = "snake_case")]
pub enum FileType {
    CbrsHeartbeat = 0,
    CellSpeedtest = 1,
    Entropy = 2,
    SubnetworkRewards = 3,
    CbrsHeartbeatIngestReport,
    CellSpeedtestIngestReport,
    EntropyReport,
    IotBeaconIngestReport,
    IotWitnessIngestReport,
    IotPoc,
    IotInvalidBeaconReport,
    IotInvalidWitnessReport,
    SpeedtestAvg,
    ValidatedHeartbeat,
    SignedPocReceiptTxn,
    RadioRewardShare,
    RewardManifest,
    IotPacketReport,
    IotValidPacket,
    InvalidPacket,
    NonRewardablePacket,
    IotRewardShare,
    DataTransferSessionIngestReport,
    InvalidDataTransferSessionIngestReport,
    ValidDataTransferSession,
    VerifiedDataTransferSession,
    PriceReport,
    MobileRewardShare,
    SubscriberLocationReq,
    SubscriberLocationIngestReport,
    VerifiedSubscriberLocationIngestReport,
    MapperMsg,
    CoverageObject,
    CoverageObjectIngestReport,
    SeniorityUpdate,
    VerifiedSpeedtest,
    WifiHeartbeat,
    WifiHeartbeatIngestReport,
    BoostedHexUpdate,
    OracleBoostingReport,
    RadioThresholdReq,
    RadioThresholdIngestReport,
    VerifiedRadioThresholdIngestReport,
    UrbanizationDataSet,
    FootfallDataSet,
    LandtypeDataSet,
    InvalidatedRadioThresholdReq,
    InvalidatedRadioThresholdIngestReport,
    VerifiedInvalidatedRadioThresholdIngestReport,
    SPBoostedRewardsBannedRadioIngestReport,
    VerifiedSPBoostedRewardsBannedRadioIngestReport,
    SubscriberVerifiedMappingEventIngestReport,
    VerifiedSubscriberVerifiedMappingEventIngestReport,
    PromotionRewardIngestReport,
    VerifiedPromotionReward,
    ServiceProviderPromotionFund,
    HexUsageStatsIngestReport,
    RadioUsageStatsIngestReport,
    HexUsageStatsReq,
    RadioUsageStatsReq,
    UniqueConnectionsReport,
    VerifiedUniqueConnectionsReport,
    SubscriberMappingActivityIngestReport,
    VerifiedSubscriberMappingActivityReport,
    MobileBanReport,
    VerifiedMobileBanReport,
    MobileHotspotChangeReport,
    IotHotspotChangeReport,
    EntityOwnershipChangeReport,
    EntityRewardDestinationChangeReport,
    EnabledCarriersInfoReport,
}

impl fmt::Display for FileType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.to_str())
    }
}

impl FileType {
    pub const fn to_str(&self) -> &'static str {
        match self {
            Self::InvalidatedRadioThresholdReq => INVALIDATED_RADIO_THRESHOLD_REQ,
            Self::InvalidatedRadioThresholdIngestReport => {
                INVALIDATED_RADIO_THRESHOLD_INGEST_REPORT
            }
            Self::VerifiedInvalidatedRadioThresholdIngestReport => {
                VERIFIED_INVALIDATED_RADIO_THRESHOLD_INGEST_REPORT
            }
            Self::RadioThresholdReq => RADIO_THRESHOLD_REQ,
            Self::RadioThresholdIngestReport => RADIO_THRESHOLD_INGEST_REPORT,
            Self::VerifiedRadioThresholdIngestReport => VERIFIED_RADIO_THRESHOLD_INGEST_REPORT,
            Self::SubscriberLocationReq => SUBSCRIBER_LOCATION_REQ,
            Self::SubscriberLocationIngestReport => SUBSCRIBER_LOCATION_INGEST_REPORT,
            Self::VerifiedSubscriberLocationIngestReport => {
                VERIFIED_SUBSCRIBER_LOCATION_INGEST_REPORT
            }
            Self::CbrsHeartbeat => CBRS_HEARTBEAT,
            Self::WifiHeartbeat => WIFI_HEARTBEAT,
            Self::CellSpeedtest => CELL_SPEEDTEST,
            Self::VerifiedSpeedtest => VERIFIED_SPEEDTEST,
            Self::CbrsHeartbeatIngestReport => CELL_HEARTBEAT_INGEST_REPORT,
            Self::WifiHeartbeatIngestReport => WIFI_HEARTBEAT_INGEST_REPORT,
            Self::CellSpeedtestIngestReport => CELL_SPEEDTEST_INGEST_REPORT,
            Self::Entropy => ENTROPY,
            Self::SubnetworkRewards => SUBNETWORK_REWARDS,
            Self::EntropyReport => ENTROPY_REPORT,
            Self::IotBeaconIngestReport => IOT_BEACON_INGEST_REPORT,
            Self::IotWitnessIngestReport => IOT_WITNESS_INGEST_REPORT,
            Self::IotPoc => IOT_POC,
            Self::IotInvalidBeaconReport => IOT_INVALID_BEACON_REPORT,
            Self::IotInvalidWitnessReport => IOT_INVALID_WITNESS_REPORT,
            Self::SpeedtestAvg => SPEEDTEST_AVG,
            Self::ValidatedHeartbeat => VALIDATED_HEARTBEAT,
            Self::SignedPocReceiptTxn => SIGNED_POC_RECEIPT_TXN,
            Self::RadioRewardShare => RADIO_REWARD_SHARE,
            Self::RewardManifest => REWARD_MANIFEST,
            Self::IotPacketReport => IOT_PACKET_REPORT,
            Self::IotValidPacket => IOT_VALID_PACKET,
            Self::InvalidPacket => INVALID_PACKET,
            Self::NonRewardablePacket => NON_REWARDABLE_PACKET,
            Self::IotRewardShare => IOT_REWARD_SHARE,
            Self::DataTransferSessionIngestReport => DATA_TRANSFER_SESSION_INGEST_REPORT,
            Self::InvalidDataTransferSessionIngestReport => {
                INVALID_DATA_TRANSFER_SESSION_INGEST_REPORT
            }
            Self::ValidDataTransferSession => VALID_DATA_TRANSFER_SESSION,
            Self::VerifiedDataTransferSession => VERIFIED_DATA_TRANSFER_SESSION,
            Self::PriceReport => PRICE_REPORT,
            Self::MobileRewardShare => MOBILE_REWARD_SHARE,
            Self::MapperMsg => MAPPER_MSG,
            Self::CoverageObject => COVERAGE_OBJECT,
            Self::CoverageObjectIngestReport => COVERAGE_OBJECT_INGEST_REPORT,
            Self::SeniorityUpdate => SENIORITY_UPDATE,
            Self::BoostedHexUpdate => BOOSTED_HEX_UPDATE,
            Self::OracleBoostingReport => ORACLE_BOOSTING_REPORT,
            Self::UrbanizationDataSet => URBANIZATION_DATA_SET,
            Self::FootfallDataSet => FOOTFALL_DATA_SET,
            Self::LandtypeDataSet => LANDTYPE_DATA_SET,
            Self::SPBoostedRewardsBannedRadioIngestReport => SP_BOOSTED_REWARDS_BANNED_RADIO,
            Self::VerifiedSPBoostedRewardsBannedRadioIngestReport => {
                VERIFIED_SP_BOOSTED_REWARDS_BANNED_RADIO
            }
            Self::SubscriberVerifiedMappingEventIngestReport => {
                SUBSCRIBER_VERIFIED_MAPPING_INGEST_REPORT
            }
            Self::VerifiedSubscriberVerifiedMappingEventIngestReport => {
                VERIFIED_SUBSCRIBER_VERIFIED_MAPPING_INGEST_REPORT
            }
            Self::PromotionRewardIngestReport => PROMOTION_REWARD_INGEST_REPORT,
            Self::VerifiedPromotionReward => VERIFIED_PROMOTION_REWARD,
            Self::ServiceProviderPromotionFund => SERVICE_PROVIDER_PROMOTION_FUND,
            Self::HexUsageStatsIngestReport => HEX_USAGE_STATS_INGEST_REPORT,
            Self::RadioUsageStatsIngestReport => RADIO_USAGE_STATS_INGEST_REPORT,
            Self::HexUsageStatsReq => HEX_USAGE_STATS_REQ,
            Self::RadioUsageStatsReq => RADIO_USAGE_STATS_REQ,
            Self::UniqueConnectionsReport => UNIQUE_CONNECTIONS_REPORT,
            Self::VerifiedUniqueConnectionsReport => VERIFIED_UNIQUE_CONNECTIONS_REPORT,
            Self::SubscriberMappingActivityIngestReport => {
                SUBSCRIBER_MAPPING_ACTIVITY_INGEST_REPORT
            }
            Self::VerifiedSubscriberMappingActivityReport => {
                VERIFIED_SUBSCRIBER_MAPPING_ACTIVITY_REPORT
            }
            Self::MobileBanReport => MOBILE_BAN_REPORT,
            Self::VerifiedMobileBanReport => VERIFIED_MOBILE_BAN_REPORT,
            Self::MobileHotspotChangeReport => MOBILE_HOTSPOT_CHANGE_REPORT,
            Self::IotHotspotChangeReport => IOT_HOTSPOT_CHANGE_REPORT,
            Self::EntityOwnershipChangeReport => ENTITY_OWNERSHIP_CHANGE_REPORT,
            Self::EntityRewardDestinationChangeReport => ENTITY_REWARD_DESTINATION_CHANGE_REPORT,
            Self::EnabledCarriersInfoReport => ENABLED_CARRIERS_REPORT,
        }
    }
}

impl FromStr for FileType {
    type Err = Error;
    fn from_str(s: &str) -> file_store::Result<Self> {
        let result = match s {
            INVALIDATED_RADIO_THRESHOLD_REQ => Self::InvalidatedRadioThresholdReq,
            INVALIDATED_RADIO_THRESHOLD_INGEST_REPORT => {
                Self::InvalidatedRadioThresholdIngestReport
            }
            VERIFIED_INVALIDATED_RADIO_THRESHOLD_INGEST_REPORT => {
                Self::VerifiedInvalidatedRadioThresholdIngestReport
            }
            RADIO_THRESHOLD_REQ => Self::RadioThresholdReq,
            RADIO_THRESHOLD_INGEST_REPORT => Self::RadioThresholdIngestReport,
            VERIFIED_RADIO_THRESHOLD_INGEST_REPORT => Self::VerifiedRadioThresholdIngestReport,
            SUBSCRIBER_LOCATION_REQ => Self::SubscriberLocationReq,
            SUBSCRIBER_LOCATION_INGEST_REPORT => Self::SubscriberLocationIngestReport,
            VERIFIED_SUBSCRIBER_LOCATION_INGEST_REPORT => {
                Self::VerifiedSubscriberLocationIngestReport
            }
            CBRS_HEARTBEAT => Self::CbrsHeartbeat,
            WIFI_HEARTBEAT => Self::WifiHeartbeat,
            CELL_SPEEDTEST => Self::CellSpeedtest,
            VERIFIED_SPEEDTEST => Self::VerifiedSpeedtest,
            CELL_HEARTBEAT_INGEST_REPORT => Self::CbrsHeartbeatIngestReport,
            WIFI_HEARTBEAT_INGEST_REPORT => Self::WifiHeartbeatIngestReport,
            CELL_SPEEDTEST_INGEST_REPORT => Self::CellSpeedtestIngestReport,
            ENTROPY => Self::Entropy,
            SUBNETWORK_REWARDS => Self::SubnetworkRewards,
            ENTROPY_REPORT => Self::EntropyReport,
            IOT_BEACON_INGEST_REPORT => Self::IotBeaconIngestReport,
            IOT_WITNESS_INGEST_REPORT => Self::IotWitnessIngestReport,
            IOT_POC => Self::IotPoc,
            IOT_INVALID_BEACON_REPORT => Self::IotInvalidBeaconReport,
            IOT_INVALID_WITNESS_REPORT => Self::IotInvalidWitnessReport,
            SPEEDTEST_AVG => Self::SpeedtestAvg,
            VALIDATED_HEARTBEAT => Self::ValidatedHeartbeat,
            SIGNED_POC_RECEIPT_TXN => Self::SignedPocReceiptTxn,
            RADIO_REWARD_SHARE => Self::RadioRewardShare,
            REWARD_MANIFEST => Self::RewardManifest,
            IOT_PACKET_REPORT => Self::IotPacketReport,
            IOT_VALID_PACKET => Self::IotValidPacket,
            INVALID_PACKET => Self::InvalidPacket,
            NON_REWARDABLE_PACKET => Self::NonRewardablePacket,
            IOT_REWARD_SHARE => Self::IotRewardShare,
            DATA_TRANSFER_SESSION_INGEST_REPORT => Self::DataTransferSessionIngestReport,
            INVALID_DATA_TRANSFER_SESSION_INGEST_REPORT => {
                Self::InvalidDataTransferSessionIngestReport
            }
            VALID_DATA_TRANSFER_SESSION => Self::ValidDataTransferSession,
            PRICE_REPORT => Self::PriceReport,
            MOBILE_REWARD_SHARE => Self::MobileRewardShare,
            MAPPER_MSG => Self::MapperMsg,
            COVERAGE_OBJECT => Self::CoverageObject,
            COVERAGE_OBJECT_INGEST_REPORT => Self::CoverageObjectIngestReport,
            SENIORITY_UPDATE => Self::SeniorityUpdate,
            BOOSTED_HEX_UPDATE => Self::BoostedHexUpdate,
            ORACLE_BOOSTING_REPORT => Self::OracleBoostingReport,
            URBANIZATION_DATA_SET => Self::UrbanizationDataSet,
            FOOTFALL_DATA_SET => Self::FootfallDataSet,
            LANDTYPE_DATA_SET => Self::LandtypeDataSet,
            SP_BOOSTED_REWARDS_BANNED_RADIO => Self::SPBoostedRewardsBannedRadioIngestReport,
            VERIFIED_SP_BOOSTED_REWARDS_BANNED_RADIO => {
                Self::VerifiedSPBoostedRewardsBannedRadioIngestReport
            }
            SUBSCRIBER_VERIFIED_MAPPING_INGEST_REPORT => {
                Self::SubscriberVerifiedMappingEventIngestReport
            }
            VERIFIED_SUBSCRIBER_VERIFIED_MAPPING_INGEST_REPORT => {
                Self::VerifiedSubscriberVerifiedMappingEventIngestReport
            }
            PROMOTION_REWARD_INGEST_REPORT => Self::PromotionRewardIngestReport,
            VERIFIED_PROMOTION_REWARD => Self::VerifiedPromotionReward,
            SERVICE_PROVIDER_PROMOTION_FUND => Self::ServiceProviderPromotionFund,
            HEX_USAGE_STATS_INGEST_REPORT => Self::HexUsageStatsIngestReport,
            RADIO_USAGE_STATS_INGEST_REPORT => Self::RadioUsageStatsIngestReport,
            HEX_USAGE_STATS_REQ => Self::HexUsageStatsReq,
            RADIO_USAGE_STATS_REQ => Self::RadioUsageStatsReq,
            UNIQUE_CONNECTIONS_REPORT => Self::UniqueConnectionsReport,
            VERIFIED_UNIQUE_CONNECTIONS_REPORT => Self::VerifiedUniqueConnectionsReport,
            SUBSCRIBER_MAPPING_ACTIVITY_INGEST_REPORT => {
                Self::SubscriberMappingActivityIngestReport
            }
            VERIFIED_SUBSCRIBER_MAPPING_ACTIVITY_REPORT => {
                Self::VerifiedSubscriberMappingActivityReport
            }
            MOBILE_BAN_REPORT => Self::MobileBanReport,
            VERIFIED_MOBILE_BAN_REPORT => Self::VerifiedMobileBanReport,
            VERIFIED_DATA_TRANSFER_SESSION => Self::VerifiedDataTransferSession,
            MOBILE_HOTSPOT_CHANGE_REPORT => Self::MobileHotspotChangeReport,
            IOT_HOTSPOT_CHANGE_REPORT => Self::IotHotspotChangeReport,
            ENTITY_OWNERSHIP_CHANGE_REPORT => Self::EntityOwnershipChangeReport,
            ENTITY_REWARD_DESTINATION_CHANGE_REPORT => Self::EntityRewardDestinationChangeReport,
            _ => return Err(Error::from(io::Error::from(io::ErrorKind::InvalidInput))),
        };
        Ok(result)
    }
}
