use crate::{
    admin::{self, AuthCache, CacheKeys, KeyType},
    region_map::{self, RegionMap, RegionMapReader},
    GrpcResult, Settings,
};
use anyhow::{anyhow, Result};
use chrono::Utc;
use file_store::traits::{MsgVerify, TimestampEncode};
use futures::future::TryFutureExt;
use helium_crypto::{Keypair, Network, PublicKey, PublicKeyBinary, Sign};
use helium_proto::{
    services::iot_config::{
        self, AdminAddKeyReqV1, AdminKeyResV1, AdminLoadRegionReqV1, AdminLoadRegionResV1,
        AdminRemoveKeyReqV1, RegionParamsReqV1, RegionParamsResV1,
    },
    Message, Region,
};
use sqlx::{Pool, Postgres};
use tokio::sync::watch;
use tonic::{Request, Response, Status};

pub struct AdminService {
    auth_cache: AuthCache,
    auth_updater: watch::Sender<CacheKeys>,
    pool: Pool<Postgres>,
    region_map: RegionMapReader,
    region_updater: watch::Sender<RegionMap>,
    required_network: Network,
    signing_key: Keypair,
}

impl AdminService {
    pub fn new(
        settings: &Settings,
        auth_cache: AuthCache,
        auth_updater: watch::Sender<CacheKeys>,
        pool: Pool<Postgres>,
        region_map: RegionMapReader,
        region_updater: watch::Sender<RegionMap>,
    ) -> Result<Self> {
        Ok(Self {
            auth_cache,
            auth_updater,
            pool,
            region_map,
            region_updater,
            required_network: settings.network,
            signing_key: settings.signing_keypair()?,
        })
    }

    fn verify_admin_request_signature<R>(
        &self,
        signer: &PublicKey,
        request: &R,
    ) -> Result<(), Status>
    where
        R: MsgVerify,
    {
        self.auth_cache
            .verify_signature_with_type(KeyType::Administrator, signer, request)
            .map_err(|_| Status::permission_denied("invalid admin signature"))?;
        Ok(())
    }

    fn verify_request_signature<R>(&self, signer: &PublicKey, request: &R) -> Result<(), Status>
    where
        R: MsgVerify,
    {
        self.auth_cache
            .verify_signature(signer, request)
            .map_err(|_| Status::permission_denied("invalid request signature"))?;
        Ok(())
    }

    fn verify_network(&self, public_key: PublicKey) -> Result<PublicKey, Status> {
        if self.required_network == public_key.network {
            Ok(public_key)
        } else {
            Err(Status::invalid_argument(format!(
                "invalid network: {}",
                public_key.network
            )))
        }
    }

    fn verify_public_key(&self, bytes: &[u8]) -> Result<PublicKey, Status> {
        PublicKey::try_from(bytes)
            .map_err(|_| Status::invalid_argument(format!("invalid public key: {bytes:?}")))
    }

    fn sign_response<R>(&self, response: &R) -> Result<Vec<u8>, Status>
    where
        R: Message,
    {
        self.signing_key
            .sign(&response.encode_to_vec())
            .map_err(|_| Status::internal("response signing error"))
    }
}

#[tonic::async_trait]
impl iot_config::Admin for AdminService {
    async fn add_key(&self, request: Request<AdminAddKeyReqV1>) -> GrpcResult<AdminKeyResV1> {
        let request = request.into_inner();

        let signer = self.verify_public_key(&request.signer)?;
        self.verify_admin_request_signature(&signer, &request)?;

        let key_type = request.key_type().into();
        let pubkey = self
            .verify_public_key(request.pubkey.as_ref())
            .and_then(|pubkey| self.verify_network(pubkey))
            .map_err(|_| Status::invalid_argument("invalid pubkey supplied"))?;

        admin::insert_key(request.pubkey.clone().into(), key_type, &self.pool)
            .and_then(|_| async move {
                if self.auth_updater.send_if_modified(|cache| {
                    if let std::collections::hash_map::Entry::Vacant(key) = cache.entry(pubkey) {
                        key.insert(key_type);
                        true
                    } else {
                        false
                    }
                }) {
                    Ok(())
                } else {
                    Err(anyhow!("key already registered"))
                }
            })
            .map_err(|err| {
                let pubkey: PublicKeyBinary = request.pubkey.into();
                tracing::error!(pubkey = pubkey.to_string(), "pubkey add failed");
                Status::internal(format!("error saving requested key: {pubkey}, {err:?}"))
            })
            .await?;

        let timestamp = Utc::now().encode_timestamp();
        let signer = self.signing_key.public_key().into();
        let mut resp = AdminKeyResV1 {
            timestamp,
            signer,
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp.encode_to_vec())?;

        Ok(Response::new(resp))
    }

    async fn remove_key(&self, request: Request<AdminRemoveKeyReqV1>) -> GrpcResult<AdminKeyResV1> {
        let request = request.into_inner();

        let signer = self.verify_public_key(&request.signer)?;
        self.verify_admin_request_signature(&signer, &request)?;

        admin::remove_key(request.pubkey.clone().into(), &self.pool)
            .and_then(|deleted| async move {
                match deleted {
                    Some((pubkey, _key_type)) => {
                        self.auth_updater.send_modify(|cache| {
                            cache.remove(&pubkey);
                        });
                        Ok(())
                    }
                    None => Ok(()),
                }
            })
            .map_err(|_| {
                let pubkey: PublicKeyBinary = request.pubkey.into();
                tracing::error!(pubkey = pubkey.to_string(), "pubkey remove failed");
                Status::internal(format!("error removing request key: {pubkey}"))
            })
            .await?;

        let timestamp = Utc::now().encode_timestamp();
        let signer = self.signing_key.public_key().into();
        let mut resp = AdminKeyResV1 {
            timestamp,
            signer,
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp.encode_to_vec())?;

        Ok(Response::new(resp))
    }

    async fn load_region(
        &self,
        request: Request<AdminLoadRegionReqV1>,
    ) -> GrpcResult<AdminLoadRegionResV1> {
        let request = request.into_inner();

        let signer = self.verify_public_key(&request.signer)?;
        self.verify_admin_request_signature(&signer, &request)?;

        let region = Region::from_i32(request.region).ok_or_else(|| {
            Status::invalid_argument(format!("invalid lora region {}", request.region))
        })?;

        if region == Region::Unknown {
            return Err(Status::invalid_argument(
                "unable to override the 'UKNOWN' region",
            ));
        }

        let params = match request.params {
            Some(params) => params,
            None => return Err(Status::invalid_argument("missing region")),
        };

        let idz = if !request.hex_indexes.is_empty() {
            Some(request.hex_indexes.as_ref())
        } else {
            None
        };

        region_map::update_region(region, &params.clone(), idz, &self.pool)
            .and_then(|updated_region| async move {
                self.region_updater.send_modify(|region_map| {
                    region_map.insert_params(region, params);
                });
                if let Some(region_tree) = updated_region {
                    tracing::debug!(region_cells = region_tree.len(), "new compacted region map");
                    self.region_updater
                        .send_modify(|region_map| region_map.replace_tree(region_tree));
                };
                Ok(())
            })
            .map_err(|err| {
                tracing::error!(
                    region = region.to_string(),
                    "failed to update region: {err:?}"
                );
                Status::internal("region update failed")
            })
            .await?;

        let timestamp = Utc::now().encode_timestamp();
        let signer = self.signing_key.public_key().into();
        let mut resp = AdminLoadRegionResV1 {
            timestamp,
            signer,
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp.encode_to_vec())?;

        Ok(Response::new(resp))
    }

    async fn region_params(
        &self,
        request: Request<RegionParamsReqV1>,
    ) -> GrpcResult<RegionParamsResV1> {
        let request = request.into_inner();

        let signer = self.verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request)?;

        let region = request.region();

        let params = self.region_map.get_params(&region);

        let timestamp = Utc::now().encode_timestamp();
        let signer = self.signing_key.public_key().into();
        let mut resp = RegionParamsResV1 {
            region: request.region,
            params,
            signer,
            signature: vec![],
            timestamp,
        };
        resp.signature = self.sign_response(&resp.encode_to_vec())?;
        tracing::debug!(region = region.to_string(), "returning region params");
        Ok(Response::new(resp))
    }
}
