use crate::{Error, Result};
use helium_crypto::{PublicKey, Verify};
use helium_proto::services::{
    iot_config::{
        AdminAddKeyReqV1, AdminLoadRegionReqV1, AdminRemoveKeyReqV1, GatewayLocationReqV1,
        GatewayRegionParamsReqV1, OrgCreateHeliumReqV1, OrgCreateRoamerReqV1, OrgDisableReqV1,
        OrgEnableReqV1, RouteCreateReqV1, RouteDeleteDevaddrRangesReqV1, RouteDeleteEuisReqV1,
        RouteDeleteReqV1, RouteGetDevaddrRangesReqV1, RouteGetEuisReqV1, RouteGetReqV1,
        RouteListReqV1, RouteStreamReqV1, RouteUpdateDevaddrRangesReqV1, RouteUpdateEuisReqV1,
        RouteUpdateReqV1,
    },
    poc_lora::{LoraBeaconReportReqV1, LoraWitnessReportReqV1},
};
use helium_proto::{
    services::poc_mobile::{CellHeartbeatReqV1, DataTransferSessionReqV1, SpeedtestReqV1},
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

impl_msg_verify!(CellHeartbeatReqV1, signature);
impl_msg_verify!(SpeedtestReqV1, signature);
impl_msg_verify!(LoraBeaconReportReqV1, signature);
impl_msg_verify!(LoraWitnessReportReqV1, signature);
impl_msg_verify!(DataTransferSessionReqV1, signature);
impl_msg_verify!(OrgCreateHeliumReqV1, signature);
impl_msg_verify!(OrgCreateRoamerReqV1, signature);
impl_msg_verify!(OrgDisableReqV1, signature);
impl_msg_verify!(OrgEnableReqV1, signature);
impl_msg_verify!(RouteStreamReqV1, signature);
impl_msg_verify!(RouteListReqV1, signature);
impl_msg_verify!(RouteGetReqV1, signature);
impl_msg_verify!(RouteCreateReqV1, signature);
impl_msg_verify!(RouteUpdateReqV1, signature);
impl_msg_verify!(RouteDeleteReqV1, signature);
impl_msg_verify!(RouteGetEuisReqV1, signature);
impl_msg_verify!(RouteDeleteEuisReqV1, signature);
impl_msg_verify!(RouteUpdateEuisReqV1, signature);
impl_msg_verify!(RouteGetDevaddrRangesReqV1, signature);
impl_msg_verify!(RouteUpdateDevaddrRangesReqV1, signature);
impl_msg_verify!(RouteDeleteDevaddrRangesReqV1, signature);
impl_msg_verify!(GatewayLocationReqV1, signature);
impl_msg_verify!(GatewayRegionParamsReqV1, signature);
impl_msg_verify!(AdminAddKeyReqV1, signature);
impl_msg_verify!(AdminLoadRegionReqV1, signature);
impl_msg_verify!(AdminRemoveKeyReqV1, signature);

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
