// Generates a string-mapped enum and its parsing/formatting implementations.
//
// This macro defines:
// - an enum with the given variants,
// - a `to_str()` method returning the associated string literal,
// - a `Display` impl that prints the variant’s string form,
// - a `FromStr` impl that parses the string back into the enum,
// - and an error type used by `FromStr`.
//
// Each variant is written as `Name => "string"` inside the macro invocation.
// The error type name must be provided explicitly, since `macro_rules!`
// cannot construct identifiers from other identifiers.
//
// # Example
//
// ```rust
// make_string_mapped_enum! {
//     ParseFileTypeError,
//     enum FileType {
//         A => "a",
//         B => "b",
//     }
// }
//
// assert_eq!(FileType::A.to_str(), "a");
// assert_eq!("b".parse::<FileType>().unwrap(), FileType::B);
// assert!("x".parse::<FileType>().is_err());
// ```
//
// # Note
//
// This cannot be a rustdoc because macros cannot be used before they are
// defined, even in comments.
macro_rules! make_string_mapped_enum {
    (
        $err_name:ident,
        enum $enum_name:ident {
            $( $name:ident => $str:literal ),+ $(,)?
        }
    ) => {

        #[derive(Debug, PartialEq, Eq, Clone, serde::Serialize, Copy)]
        #[serde(rename_all = "snake_case")]
        pub enum $enum_name {
            $($name),*
        }

        impl std::fmt::Display for $enum_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.to_str())
            }
        }

        impl $enum_name {
            pub const fn to_str(&self) -> &'static str {
                match self {
                    $(Self::$name => $str,)*
                }
            }
        }

        #[derive(thiserror::Error, Debug)]
        #[error("invalid {} str: {0}", stringify!($enum_name))]
        pub struct $err_name(pub String);


        impl std::str::FromStr for $enum_name {
            type Err = $err_name;

            fn from_str(s: &str) -> Result<Self, $err_name> {
                let result = match s {
                    $($str => Self::$name,)*
                    other => return Err($err_name(other.to_owned()))
                };
                Ok(result)
            }
        }
    };
}

make_string_mapped_enum! {
    ParseFileTypeError,
    enum FileType {
        CbrsHeartbeat => "cbrs_heartbeat",
        CellSpeedtest => "cell_speedtest",
        Entropy => "entropy",
        SubnetworkRewards => "subnetwork_rewards",
        CbrsHeartbeatIngestReport => "heartbeat_report",
        CellSpeedtestIngestReport => "speedtest_report",
        EntropyReport => "entropy_report",
        IotBeaconIngestReport => "iot_beacon_ingest_report",
        IotWitnessIngestReport => "iot_witness_ingest_report",
        IotPoc => "iot_poc",
        IotInvalidBeaconReport => "iot_invalid_beacon",
        IotInvalidWitnessReport => "iot_invalid_witness",
        SpeedtestAvg => "speedtest_avg",
        ValidatedHeartbeat => "validated_heartbeat",
        SignedPocReceiptTxn => "signed_poc_receipt_txn",
        RadioRewardShare => "radio_reward_share",
        RewardManifest => "network_reward_manifest_v1",
        IotPacketReport => "packetreport",
        IotValidPacket => "iot_valid_packet",
        InvalidPacket => "invalid_packet",
        NonRewardablePacket => "non_rewardable_packet",
        IotRewardShare => "iot_network_reward_shares_v1",
        DataTransferSessionIngestReport => "data_transfer_session_ingest_report",
        InvalidDataTransferSessionIngestReport => "invalid_data_transfer_session_ingest_report",
        ValidDataTransferSession => "valid_data_transfer_session",
        VerifiedDataTransferSession => "verified_data_transfer_session",
        PriceReport => "price_report",
        MobileRewardShare => "mobile_network_reward_shares_v1",
        SubscriberLocationReq => "subscriber_location_req",
        SubscriberLocationIngestReport => "subscriber_location_report",
        VerifiedSubscriberLocationIngestReport => "verified_subscriber_location_report",
        MapperMsg => "mapper_msg",
        CoverageObject => "coverage_object",
        CoverageObjectIngestReport => "coverage_object_ingest_report",
        SeniorityUpdate => "seniority_update",
        VerifiedSpeedtest => "verified_speedtest",
        WifiHeartbeat => "wifi_heartbeat",
        WifiHeartbeatIngestReport => "wifi_heartbeat_report",
        BoostedHexUpdate => "boosted_hex_update",
        OracleBoostingReport => "oracle_boosting_report",
        RadioThresholdReq => "radio_threshold_req",
        RadioThresholdIngestReport => "radio_threshold_ingest_report",
        VerifiedRadioThresholdIngestReport => "verified_radio_threshold_report",
        UrbanizationDataSet => "urbanization",
        FootfallDataSet => "footfall",
        LandtypeDataSet => "landtype",
        InvalidatedRadioThresholdReq => "invalidated_radio_threshold_req",
        InvalidatedRadioThresholdIngestReport => "invalidated_radio_threshold_ingest_report",
        VerifiedInvalidatedRadioThresholdIngestReport => "verified_invalidated_radio_threshold_report",
        SPBoostedRewardsBannedRadioIngestReport => "service_provider_boosted_rewards_banned_radio",
        VerifiedSPBoostedRewardsBannedRadioIngestReport => "verified_service_provider_boosted_rewards_banned_radio",
        SubscriberVerifiedMappingEventIngestReport => "subscriber_verified_mapping_ingest_report",
        VerifiedSubscriberVerifiedMappingEventIngestReport => "verified_subscriber_verified_mapping_ingest_report",
        PromotionRewardIngestReport => "promotion_reward_ingest_report",
        VerifiedPromotionReward => "verified_promotion_reward",
        ServiceProviderPromotionFund => "service_provider_promotion_fund",
        HexUsageStatsIngestReport => "hex_usage_stats_ingest_report",
        RadioUsageStatsIngestReport => "radio_usage_stats_ingest_report",
        RadioUsageStatsIngestReportV2 => "radio_usage_stats_ingest_report_v2",
        HexUsageStatsReq => "hex_usage_stats_req",
        RadioUsageStatsReq => "radio_usage_stats_req",
        UniqueConnectionsReport => "unique_connections_report",
        VerifiedUniqueConnectionsReport => "verified_unique_connections_report",
        SubscriberMappingActivityIngestReport => "subscriber_mapping_activity_ingest_report",
        VerifiedSubscriberMappingActivityReport => "verified_subscriber_mapping_activity_report",
        MobileBanReport => "mobile_ban_report",
        VerifiedMobileBanReport => "verified_mobile_ban_report",
        MobileHotspotChangeReport => "mobile_hotspot_change_report",
        IotHotspotChangeReport => "iot_hotspot_change_report",
        EntityOwnershipChangeReport => "entity_ownership_change_report",
        EntityRewardDestinationChangeReport => "entity_reward_destination_change_report",
        EnabledCarriersInfoReport => "enabled_carriers_report",
    }
}
