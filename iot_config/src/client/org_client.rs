use super::{
    call_with_retry, iot_config, Arc, Channel, ClientError, Duration, Endpoint, Keypair, MsgVerify,
    PublicKey, Settings,
};
use async_trait::async_trait;
use chrono::Utc;
use file_store::traits::TimestampEncode;
use helium_proto::services::iot_config::{
    OrgDisableReqV1, OrgEnableReqV1, OrgGetReqV1, OrgListReqV1, OrgResV1, OrgV1,
};
use helium_proto_crypto::MsgSign;

#[async_trait]
pub trait Orgs: Send + Sync + 'static {
    async fn get(&mut self, oui: u64) -> Result<OrgResV1, ClientError>;
    async fn list(&mut self) -> Result<Vec<OrgV1>, ClientError>;
    async fn enable(&mut self, oui: u64) -> Result<(), ClientError>;
    async fn disable(&mut self, oui: u64) -> Result<(), ClientError>;
}

#[derive(Clone)]
pub struct OrgClient {
    client: iot_config::config_org_client::OrgClient<Channel>,
    signing_key: Arc<Keypair>,
    config_pubkey: PublicKey,
}

impl OrgClient {
    pub fn from_settings(settings: &Settings) -> Result<Self, Box<helium_crypto::Error>> {
        let channel = Endpoint::from(settings.url.clone())
            .connect_timeout(Duration::from_secs(settings.connect_timeout))
            .timeout(Duration::from_secs(settings.rpc_timeout))
            .connect_lazy();
        Ok(Self {
            client: iot_config::config_org_client::OrgClient::new(channel),
            signing_key: settings.signing_keypair()?,
            config_pubkey: settings.config_pubkey()?,
        })
    }
}

#[async_trait]
impl Orgs for OrgClient {
    async fn get(&mut self, oui: u64) -> Result<OrgResV1, ClientError> {
        tracing::debug!(%oui, "retrieving org");

        let req = OrgGetReqV1 { oui };
        let res = call_with_retry!(self.client.get(req))?.into_inner();
        res.verify(&self.config_pubkey)?;
        Ok(res)
    }

    async fn list(&mut self) -> Result<Vec<OrgV1>, ClientError> {
        tracing::debug!("retrieving org list");

        let res = call_with_retry!(self.client.list(OrgListReqV1 {}))?.into_inner();
        res.verify(&self.config_pubkey)?;
        Ok(res.orgs)
    }

    async fn enable(&mut self, oui: u64) -> Result<(), ClientError> {
        tracing::info!(%oui, "enabling org");

        let mut req = OrgEnableReqV1 {
            oui,
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        req.sign(&self.signing_key)?;
        let res = call_with_retry!(self.client.enable(req.clone()))?.into_inner();
        res.verify(&self.config_pubkey)?;
        Ok(())
    }

    async fn disable(&mut self, oui: u64) -> Result<(), ClientError> {
        tracing::info!(%oui, "disabling org");

        let mut req = OrgDisableReqV1 {
            oui,
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        req.sign(&self.signing_key)?;
        let res = call_with_retry!(self.client.disable(req.clone()))?.into_inner();
        res.verify(&self.config_pubkey)?;
        Ok(())
    }
}
