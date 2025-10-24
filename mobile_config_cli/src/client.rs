use crate::{
    cmds::gateway::{GatewayInfo, GatewayInfoStream},
    current_timestamp, NetworkKeyRole, Result,
};

use base64::Engine;
use futures::{stream, StreamExt};
use helium_crypto::{Keypair, PublicKey, Sign, Verify};
use helium_proto::{
    services::mobile_config::{
        admin_client, authorization_client, carrier_service_client, entity_client, gateway_client,
        AdminAddKeyReqV1, AdminKeyResV1, AdminRemoveKeyReqV1, AuthorizationListReqV1,
        AuthorizationListResV1, AuthorizationVerifyReqV1, AuthorizationVerifyResV1,
        CarrierIncentivePromotionListReqV1, CarrierIncentivePromotionListResV1, EntityVerifyReqV1,
        EntityVerifyResV1, GatewayInfoBatchReqV1, GatewayInfoHistoricalReqV1, GatewayInfoReqV1,
        GatewayInfoResV2, GatewayInfoStreamResV2,
    },
    Message,
};
use mobile_config::KeyRole;
use std::{
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};
use tonic::transport::Channel;

pub struct AdminClient {
    client: admin_client::AdminClient<Channel>,
    server_pubkey: PublicKey,
}

pub struct AuthClient {
    client: authorization_client::AuthorizationClient<Channel>,
    server_pubkey: PublicKey,
}

pub struct CarrierClient {
    client: carrier_service_client::CarrierServiceClient<Channel>,
    server_pubkey: PublicKey,
}

pub struct EntityClient {
    client: entity_client::EntityClient<Channel>,
    server_pubkey: PublicKey,
}

pub struct GatewayClient {
    client: gateway_client::GatewayClient<Channel>,
    server_pubkey: PublicKey,
}

impl AdminClient {
    pub async fn new(host: &str, server_pubkey: &str) -> Result<Self> {
        Ok(Self {
            client: admin_client::AdminClient::connect(host.to_owned()).await?,
            server_pubkey: PublicKey::from_str(server_pubkey)?,
        })
    }

    pub async fn add_key(
        &mut self,
        pubkey: &PublicKey,
        key_role: KeyRole,
        keypair: &Keypair,
    ) -> Result {
        let mut request = AdminAddKeyReqV1 {
            pubkey: pubkey.into(),
            role: key_role.into(),
            signer: keypair.public_key().into(),
            signature: vec![],
            timestamp: current_timestamp()?,
        };
        request.signature = request.sign(keypair)?;
        self.client
            .add_key(request)
            .await?
            .into_inner()
            .verify(&self.server_pubkey)
    }

    pub async fn remove_key(
        &mut self,
        pubkey: &PublicKey,
        key_role: KeyRole,
        keypair: &Keypair,
    ) -> Result {
        let mut request = AdminRemoveKeyReqV1 {
            pubkey: pubkey.into(),
            role: key_role.into(),
            signer: keypair.public_key().into(),
            signature: vec![],
            timestamp: current_timestamp()?,
        };
        request.signature = request.sign(keypair)?;
        self.client
            .remove_key(request)
            .await?
            .into_inner()
            .verify(&self.server_pubkey)
    }
}

impl AuthClient {
    pub async fn new(host: &str, server_pubkey: &str) -> Result<Self> {
        Ok(Self {
            client: authorization_client::AuthorizationClient::connect(host.to_owned()).await?,
            server_pubkey: PublicKey::from_str(server_pubkey)?,
        })
    }

    pub async fn verify(
        &mut self,
        pubkey: &PublicKey,
        role: NetworkKeyRole,
        keypair: &Keypair,
    ) -> Result<bool> {
        let mut request = AuthorizationVerifyReqV1 {
            pubkey: pubkey.into(),
            role: role as i32,
            signer: keypair.public_key().into(),
            signature: vec![],
        };
        request.signature = request.sign(keypair)?;
        if let Ok(response) = self.client.verify(request).await {
            response
                .into_inner()
                .verify(&self.server_pubkey)
                .map_err(|_| anyhow::anyhow!("invalid response signature"))?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn list(
        &mut self,
        role: NetworkKeyRole,
        keypair: &Keypair,
    ) -> Result<Vec<PublicKey>> {
        let mut request = AuthorizationListReqV1 {
            role: role.into(),
            signer: keypair.public_key().into(),
            signature: vec![],
        };
        request.signature = request.sign(keypair)?;
        let response = self.client.list(request).await?.into_inner();
        response.verify(&self.server_pubkey)?;
        Ok(response
            .pubkeys
            .into_iter()
            .map(PublicKey::try_from)
            .collect::<Result<Vec<PublicKey>, _>>()?)
    }
}

impl CarrierClient {
    pub async fn new(host: &str, server_pubkey: &str) -> Result<Self> {
        Ok(Self {
            client: carrier_service_client::CarrierServiceClient::connect(host.to_owned()).await?,
            server_pubkey: PublicKey::from_str(server_pubkey)?,
        })
    }

    pub async fn list_incentive_promotions(
        &mut self,
        keypair: &Keypair,
    ) -> Result<CarrierIncentivePromotionListResV1> {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let mut request = CarrierIncentivePromotionListReqV1 {
            timestamp,
            signer: keypair.public_key().into(),
            signature: vec![],
        };
        request.signature = request.sign(keypair)?;
        match self.client.list_incentive_promotions(request).await {
            Ok(response) => {
                let res = response.into_inner();
                res.verify(&self.server_pubkey)?;
                Ok(res)
            }
            Err(error) => Err(error)?,
        }
    }
}

impl EntityClient {
    pub async fn new(host: &str, server_pubkey: &str) -> Result<Self> {
        Ok(Self {
            client: entity_client::EntityClient::connect(host.to_owned()).await?,
            server_pubkey: PublicKey::from_str(server_pubkey)?,
        })
    }

    pub async fn verify(&mut self, entity: &str, keypair: &Keypair) -> Result<bool> {
        let mut request = EntityVerifyReqV1 {
            entity_id: base64::engine::general_purpose::STANDARD.decode(entity)?,
            signer: keypair.public_key().into(),
            signature: vec![],
        };
        request.signature = request.sign(keypair)?;
        match self.client.verify(request).await {
            Ok(response) => {
                response.into_inner().verify(&self.server_pubkey)?;
                Ok(true)
            }
            Err(status) if status.code() == tonic::Code::NotFound => Ok(false),
            Err(error) => Err(error)?,
        }
    }
}

impl GatewayClient {
    pub async fn new(host: &str, server_pubkey: &str) -> Result<Self> {
        Ok(Self {
            client: gateway_client::GatewayClient::connect(host.to_owned()).await?,
            server_pubkey: PublicKey::from_str(server_pubkey)?,
        })
    }

    pub async fn info(&mut self, gateway: &PublicKey, keypair: &Keypair) -> Result<GatewayInfo> {
        let mut request = GatewayInfoReqV1 {
            address: gateway.into(),
            signer: keypair.public_key().into(),
            signature: vec![],
        };
        request.signature = request.sign(keypair)?;
        let response = self.client.info_v2(request).await?.into_inner();
        response.verify(&self.server_pubkey)?;
        let info = response
            .info
            .ok_or_else(|| anyhow::anyhow!("gateway not found"))?;
        GatewayInfo::try_from(info)
    }

    pub async fn info_batch(
        &mut self,
        gateways: &[PublicKey],
        batch_size: u32,
        keypair: &Keypair,
    ) -> Result<GatewayInfoStream> {
        let mut request = GatewayInfoBatchReqV1 {
            addresses: gateways.iter().map(|pubkey| pubkey.into()).collect(),
            batch_size,
            signer: keypair.public_key().into(),
            signature: vec![],
        };
        request.signature = request.sign(keypair)?;
        let config_pubkey = self.server_pubkey.clone();
        let stream = self
            .client
            .info_batch_v2(request)
            .await?
            .into_inner()
            .filter_map(|res| async move { res.ok() })
            .map(move |res| (res, config_pubkey.clone()))
            .filter_map(|(res, pubkey)| async move {
                match res.verify(&pubkey) {
                    Ok(()) => Some(res),
                    Err(err) => {
                        tracing::error!(?err, "Response verification failed");
                        None
                    }
                }
            })
            .flat_map(|res| stream::iter(res.gateways.into_iter()))
            .map(GatewayInfo::try_from)
            .filter_map(|gateway| async move { gateway.ok() })
            .boxed();

        Ok(stream)
    }

    pub async fn info_historical(
        &mut self,
        gateway: &PublicKey,
        query_time: u64,
        keypair: &Keypair,
    ) -> Result<GatewayInfo> {
        let mut request = GatewayInfoHistoricalReqV1 {
            address: gateway.into(),
            query_time,
            signer: keypair.public_key().into(),
            signature: vec![],
        };
        request.signature = request.sign(keypair)?;
        let response = self.client.info_historical(request).await?.into_inner();
        response.verify(&self.server_pubkey)?;
        let info = response
            .info
            .ok_or_else(|| anyhow::anyhow!("gateway not found"))?;
        GatewayInfo::try_from(info)
    }
}

pub trait MsgSign: Message + std::clone::Clone {
    fn sign(&self, keypair: &Keypair) -> Result<Vec<u8>>
    where
        Self: std::marker::Sized;
}

macro_rules! impl_sign {
    ($msg_type:ty, $( $sig: ident ),+ ) => {
        impl MsgSign for $msg_type {
            fn sign(&self, keypair: &Keypair) -> Result<Vec<u8>> {
                let mut msg = self.clone();
                $(msg.$sig = vec![];)+
                Ok(keypair.sign(&msg.encode_to_vec())?)
            }
        }
    }
}

impl_sign!(AdminAddKeyReqV1, signature);
impl_sign!(AdminRemoveKeyReqV1, signature);
impl_sign!(AuthorizationVerifyReqV1, signature);
impl_sign!(AuthorizationListReqV1, signature);
impl_sign!(EntityVerifyReqV1, signature);
impl_sign!(GatewayInfoReqV1, signature);
impl_sign!(GatewayInfoBatchReqV1, signature);
impl_sign!(GatewayInfoHistoricalReqV1, signature);
impl_sign!(CarrierIncentivePromotionListReqV1, signature);

pub trait MsgVerify: Message + std::clone::Clone {
    fn verify(&self, verifier: &PublicKey) -> Result
    where
        Self: std::marker::Sized;
}

macro_rules! impl_verify {
    ($msg_type:ty, $sig: ident) => {
        impl MsgVerify for $msg_type {
            fn verify(&self, verifier: &PublicKey) -> Result {
                let mut buf = vec![];
                let mut msg = self.clone();
                msg.$sig = vec![];
                msg.encode(&mut buf)?;
                verifier
                    .verify(&buf, &self.$sig)
                    .map_err(anyhow::Error::from)
            }
        }
    };
}

impl_verify!(AdminKeyResV1, signature);
impl_verify!(AuthorizationVerifyResV1, signature);
impl_verify!(AuthorizationListResV1, signature);
impl_verify!(EntityVerifyResV1, signature);
impl_verify!(GatewayInfoResV2, signature);
impl_verify!(GatewayInfoStreamResV2, signature);
impl_verify!(CarrierIncentivePromotionListResV1, signature);
