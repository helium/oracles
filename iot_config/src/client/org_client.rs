use super::{iot_config, Arc, Channel, ClientError, Duration, Endpoint, Keypair, Message, MsgVerify, PublicKey, Settings, Sign};
use chrono::Utc;
use file_store::traits::TimestampEncode;
use helium_proto::services::iot_config::{OrgEnableReqV1, OrgDisableReqV1, OrgResV1, OrgGetReqV1, OrgV1, OrgListReqV1};

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

    pub async fn get(&mut self, oui: u64) -> Result<OrgResV1, ClientError> {
        tracing::debug!(%oui, "retrieving org");

        let req = OrgGetReqV1 { oui };
        let res = self.client.get(req).await?.into_inner();
        res.verify(&self.config_pubkey)?;
        Ok(res)
    }

    pub async fn list(&mut self) -> Result<Vec<OrgV1>, ClientError> {
        tracing::debug!("retrieving org list");

        let res = self.client.list(OrgListReqV1 {}).await?.into_inner();
        res.verify(&self.config_pubkey)?;
        Ok(res.orgs)
    }

    pub async fn enable(&mut self, oui: u64) -> Result<(), ClientError> {
        tracing::info!(%oui, "enabling org");

        let mut req = OrgEnableReqV1 {
            oui,
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        req.signature = self.signing_key.sign(&req.encode_to_vec())?;
        let res = self.client.enable(req).await?.into_inner();
        res.verify(&self.config_pubkey)?;
        Ok(())
    }

    pub async fn disable(&mut self, oui: u64) -> Result<(), ClientError> {
        tracing::info!(%oui, "disabling org");

        let mut req = OrgDisableReqV1 {
            oui,
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        req.signature = self.signing_key.sign(&req.encode_to_vec())?;
        let res = self.client.disable(req).await?.into_inner();
        res.verify(&self.config_pubkey)?;
        Ok(())
    }
}
