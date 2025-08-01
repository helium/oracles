use crate::{Error, Result};
use helium_crypto::{PublicKey, Verify};
use helium_proto::services::{
    iot_config, mobile_config,
    poc_lora::{LoraBeaconReportReqV1, LoraStreamSessionInitV1, LoraWitnessReportReqV1},
    poc_mobile::{self, ServiceProviderBoostedRewardsBannedRadioReqV1},
    sub_dao,
};
use helium_proto::{
    services::poc_mobile::{
        CellHeartbeatReqV1, CoverageObjectReqV1, DataTransferSessionReqV1,
        InvalidatedRadioThresholdReportReqV1, RadioThresholdReportReqV1, SpeedtestReqV1,
        SubscriberLocationReqV1, WifiHeartbeatReqV1,
    },
    Message,
};

pub trait MsgVerify {
    fn verify(&self, verifier: &PublicKey) -> Result;
}

macro_rules! impl_msg_verify {
    ($msg_type:ty, $sig: ident) => {
        impl MsgVerify for $msg_type {
            fn verify(&self, verifier: &PublicKey) -> Result {
                let mut buf = vec![];
                let mut msg = self.clone();
                msg.$sig = vec![];
                msg.encode(&mut buf)?;
                verifier.verify(&buf, &self.$sig).map_err(Error::from)
            }
        }
    };
}
impl_msg_verify!(InvalidatedRadioThresholdReportReqV1, signature);
impl_msg_verify!(RadioThresholdReportReqV1, signature);
impl_msg_verify!(SubscriberLocationReqV1, signature);
impl_msg_verify!(CellHeartbeatReqV1, signature);
impl_msg_verify!(WifiHeartbeatReqV1, signature);
impl_msg_verify!(SpeedtestReqV1, signature);
impl_msg_verify!(LoraBeaconReportReqV1, signature);
impl_msg_verify!(LoraWitnessReportReqV1, signature);
impl_msg_verify!(LoraStreamSessionInitV1, signature);
impl_msg_verify!(DataTransferSessionReqV1, signature);
impl_msg_verify!(CoverageObjectReqV1, signature);
impl_msg_verify!(ServiceProviderBoostedRewardsBannedRadioReqV1, signature);
impl_msg_verify!(iot_config::OrgCreateHeliumReqV1, signature);
impl_msg_verify!(iot_config::OrgCreateRoamerReqV1, signature);
impl_msg_verify!(iot_config::OrgUpdateReqV1, signature);
impl_msg_verify!(iot_config::OrgDisableReqV1, signature);
impl_msg_verify!(iot_config::OrgEnableReqV1, signature);
impl_msg_verify!(iot_config::OrgDisableResV1, signature);
impl_msg_verify!(iot_config::OrgEnableResV1, signature);
impl_msg_verify!(iot_config::OrgResV1, signature);
impl_msg_verify!(iot_config::OrgListResV1, signature);
impl_msg_verify!(iot_config::RouteStreamReqV1, signature);
impl_msg_verify!(iot_config::RouteListReqV1, signature);
impl_msg_verify!(iot_config::RouteGetReqV1, signature);
impl_msg_verify!(iot_config::RouteCreateReqV1, signature);
impl_msg_verify!(iot_config::RouteUpdateReqV1, signature);
impl_msg_verify!(iot_config::RouteDeleteReqV1, signature);
impl_msg_verify!(iot_config::RouteGetEuisReqV1, signature);
impl_msg_verify!(iot_config::RouteUpdateEuisReqV1, signature);
impl_msg_verify!(iot_config::RouteGetDevaddrRangesReqV1, signature);
impl_msg_verify!(iot_config::RouteUpdateDevaddrRangesReqV1, signature);
impl_msg_verify!(iot_config::RouteSkfListReqV1, signature);
impl_msg_verify!(iot_config::RouteSkfGetReqV1, signature);
impl_msg_verify!(iot_config::RouteSkfUpdateReqV1, signature);
impl_msg_verify!(iot_config::GatewayLocationReqV1, signature);
impl_msg_verify!(iot_config::GatewayRegionParamsReqV1, signature);
impl_msg_verify!(iot_config::AdminAddKeyReqV1, signature);
impl_msg_verify!(iot_config::AdminLoadRegionReqV1, signature);
impl_msg_verify!(iot_config::AdminRemoveKeyReqV1, signature);
impl_msg_verify!(iot_config::GatewayInfoReqV1, signature);
impl_msg_verify!(iot_config::GatewayInfoStreamReqV1, signature);
impl_msg_verify!(iot_config::RegionParamsReqV1, signature);
impl_msg_verify!(iot_config::GatewayInfoResV1, signature);
impl_msg_verify!(iot_config::GatewayInfoStreamResV1, signature);
impl_msg_verify!(iot_config::RegionParamsResV1, signature);
impl_msg_verify!(mobile_config::AdminAddKeyReqV1, signature);
impl_msg_verify!(mobile_config::AdminRemoveKeyReqV1, signature);
impl_msg_verify!(mobile_config::AuthorizationVerifyReqV1, signature);
impl_msg_verify!(mobile_config::AuthorizationVerifyResV1, signature);
impl_msg_verify!(mobile_config::AuthorizationListReqV1, signature);
impl_msg_verify!(mobile_config::AuthorizationListResV1, signature);
impl_msg_verify!(mobile_config::EntityVerifyReqV1, signature);
impl_msg_verify!(mobile_config::EntityVerifyResV1, signature);
impl_msg_verify!(mobile_config::CarrierIncentivePromotionListReqV1, signature);
impl_msg_verify!(mobile_config::CarrierIncentivePromotionListResV1, signature);
impl_msg_verify!(mobile_config::CarrierKeyToEntityReqV1, signature);
impl_msg_verify!(mobile_config::CarrierKeyToEntityResV1, signature);
impl_msg_verify!(mobile_config::GatewayInfoReqV1, signature);
impl_msg_verify!(mobile_config::GatewayInfoStreamReqV1, signature);
impl_msg_verify!(mobile_config::GatewayInfoStreamReqV2, signature);
impl_msg_verify!(mobile_config::GatewayInfoStreamReqV3, signature);
impl_msg_verify!(mobile_config::GatewayInfoResV1, signature);
impl_msg_verify!(mobile_config::GatewayInfoResV2, signature);
impl_msg_verify!(mobile_config::GatewayInfoBatchReqV1, signature);
impl_msg_verify!(mobile_config::GatewayInfoStreamResV1, signature);
impl_msg_verify!(mobile_config::GatewayInfoStreamResV2, signature);
impl_msg_verify!(mobile_config::GatewayInfoStreamResV3, signature);
impl_msg_verify!(mobile_config::BoostedHexInfoStreamReqV1, signature);
impl_msg_verify!(mobile_config::BoostedHexModifiedInfoStreamReqV1, signature);
impl_msg_verify!(mobile_config::BoostedHexInfoStreamResV1, signature);
impl_msg_verify!(sub_dao::SubDaoEpochRewardInfoReqV1, signature);
impl_msg_verify!(sub_dao::SubDaoEpochRewardInfoResV1, signature);
impl_msg_verify!(poc_mobile::SubscriberVerifiedMappingEventReqV1, signature);
impl_msg_verify!(poc_mobile::HexUsageStatsReqV1, signature);
impl_msg_verify!(poc_mobile::RadioUsageStatsReqV1, signature);
impl_msg_verify!(poc_mobile::UniqueConnectionsReqV1, signature);
impl_msg_verify!(poc_mobile::SubscriberMappingActivityReqV1, signature);
impl_msg_verify!(poc_mobile::BanReqV1, signature);

#[cfg(test)]
mod test {
    use super::*;
    use base64::Engine;

    #[test]
    fn verify_heartbeat() {
        // Generated by FreedomFi
        const HEARTBEAT_MSG: &str = "CiEAucYd0JWglc+ffTbh+4s3wY6aWP4LTGxlbxyuMQJfviwSBmVub2RlYhjw0CYgq8mflwYp2Ls/3qtyREAxEHaKVYNMUsA4AUIBQUoZUDI3LVNDRTQyNTVXMjExMkNXNTAwMjUzNlJHMEUCIQDMXkTc49+zouvPTcf15ufutyQV04VoKW3ipqFkkIMxOgIgWAWJpo4MnNWzzzwMnE4OcY35XbsT34+K6ineoj50Szc=";
        let msg = CellHeartbeatReqV1::decode(
            base64::engine::general_purpose::STANDARD
                .decode(HEARTBEAT_MSG)
                .expect("base64 message")
                .as_ref(),
        )
        .expect("cell heartbeat");
        let public_key = PublicKey::from_bytes(&msg.pub_key).expect("public key");
        assert!(msg.verify(&public_key).is_ok());
    }

    #[test]
    fn verify_speedtest() {
        // Generated by FreedomFi
        const SPEEDTEST_MSG: &str = "CiEAPGoan3wJ+7zNiR3cIvcPpVSIxpvNUcpa5i0W46TNduMSEEhMLTIxNTMtMDAwMTI2OTQYtoOhlwYgsN4CKKDnAjAxOkcwRQIhAI8gko+CSzGkC4JxIY0+g1HwL4/kii6HEktOmoCasEV3AiBJgKrRUAJFEOS8fJo4/v8DUehl0IbH3dPZFY4CXEOuKA==";
        let msg = SpeedtestReqV1::decode(
            base64::engine::general_purpose::STANDARD
                .decode(SPEEDTEST_MSG)
                .expect("base64 message")
                .as_ref(),
        )
        .expect("cell speedtest");
        let public_key = PublicKey::from_bytes(&msg.pub_key).expect("public key");
        assert!(msg.verify(&public_key).is_ok());
    }
    // TODO: Add tests for iot beacon and witness reports
}
