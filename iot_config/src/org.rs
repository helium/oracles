use helium_crypto::PublicKeyBinary;
use serde::Serialize;

pub mod proto {
    pub use helium_proto::services::iot_config::OrgV1;
}

#[derive(Debug, Clone, Serialize)]
pub struct Org {
    pub oui: u64,
    pub owner: PublicKeyBinary,
    pub payer: PublicKeyBinary,
    pub delegate_keys: Vec<PublicKeyBinary>,
    pub nonce: u64,
}

#[derive(Debug, Serialize)]
pub struct OrgList {
    orgs: Vec<Org>,
}

impl From<proto::OrgV1> for Org {
    fn from(org: proto::OrgV1) -> Self {
        Self {
            oui: org.oui,
            owner: org.owner.into(),
            payer: org.payer.into(),
            nonce: org.nonce,
            delegate_keys: org
                .delegate_keys
                .into_iter()
                .map(|key| key.into())
                .collect(),
        }
    }
}

impl From<Org> for proto::OrgV1 {
    fn from(org: Org) -> Self {
        Self {
            oui: org.oui,
            owner: org.owner.into(),
            payer: org.payer.into(),
            nonce: org.nonce,
            delegate_keys: org
                .delegate_keys
                .iter()
                .map(|key| key.as_ref().into())
                .collect(),
        }
    }
}
