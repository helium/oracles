use crate::{Error, Result};
use helium_crypto::{PublicKey, Verify};
use helium_proto::services::poc_lora::{LoraBeaconReportReqV1, LoraWitnessReportReqV1};
use helium_proto::{
    services::poc_mobile::{CellHeartbeatReqV1, SpeedtestReqV1},
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn verify_heartbeat() {
        // Generated by FreedomFi
        const HEARTBEAT_MSG: &str = "CiEAucYd0JWglc+ffTbh+4s3wY6aWP4LTGxlbxyuMQJfviwSBmVub2RlYhjw0CYgq8mflwYp2Ls/3qtyREAxEHaKVYNMUsA4AUIBQUoZUDI3LVNDRTQyNTVXMjExMkNXNTAwMjUzNlJHMEUCIQDMXkTc49+zouvPTcf15ufutyQV04VoKW3ipqFkkIMxOgIgWAWJpo4MnNWzzzwMnE4OcY35XbsT34+K6ineoj50Szc=";
        let msg = CellHeartbeatReqV1::decode(
            base64::decode(HEARTBEAT_MSG)
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
            base64::decode(SPEEDTEST_MSG)
                .expect("base64 message")
                .as_ref(),
        )
        .expect("cell speedtest");
        let public_key = PublicKey::from_bytes(&msg.pub_key).expect("public key");
        assert!(msg.verify(&public_key).is_ok());
    }
    // TODO: Add tests for lora beacon and witness reports
}
