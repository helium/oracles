use crate::{
    admin::{self, AuthCache, CacheKeys, KeyType},
    region_map::{self, RegionMap, RegionMapReader},
    telemetry, verify_public_key, GrpcResult, Settings,
};
use anyhow::{anyhow, Result};
use chrono::Utc;
use file_store::traits::TimestampEncode;
use futures::future::TryFutureExt;
use helium_crypto::{Keypair, PublicKey, PublicKeyBinary, Sign};
use helium_proto::{
    services::iot_config::{
        self, AdminAddKeyReqV1, AdminKeyResV1, AdminLoadRegionReqV1, AdminLoadRegionResV1,
        AdminRemoveKeyReqV1, RegionParamsReqV1, RegionParamsResV1,
    },
    traits::msg_verify::MsgVerify,
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

    fn sign_response(&self, response: &[u8]) -> Result<Vec<u8>, Status> {
        self.signing_key
            .sign(response)
            .map_err(|_| Status::internal("response signing error"))
    }
}

#[tonic::async_trait]
impl iot_config::Admin for AdminService {
    async fn add_key(&self, request: Request<AdminAddKeyReqV1>) -> GrpcResult<AdminKeyResV1> {
        let request = request.into_inner();
        telemetry::count_request("admin", "add-key");
        custom_tracing::record_b58("pub_key", &request.pubkey);
        custom_tracing::record_b58("signer", &request.signer);

        let signer = verify_public_key(&request.signer)?;
        self.verify_admin_request_signature(&signer, &request)?;

        let key_type = request.key_type().into();
        let pubkey = verify_public_key(request.pubkey.as_ref())
            .map_err(|_| Status::invalid_argument("invalid pubkey supplied"))?;

        admin::insert_key(request.pubkey.clone().into(), key_type, &self.pool)
            .and_then(|_| async move {
                if self.auth_updater.send_if_modified(|cache| {
                    if let std::collections::hash_map::Entry::Vacant(key) =
                        cache.entry(pubkey.clone())
                    {
                        key.insert(key_type);
                        true
                    } else {
                        false
                    }
                }) {
                    tracing::info!(%key_type, "key authorized");
                    Ok(())
                } else {
                    Err(anyhow!("key already registered"))
                }
            })
            .map_err(|err| {
                let pubkey: PublicKeyBinary = request.pubkey.into();
                tracing::error!("pubkey add failed");
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
        telemetry::count_request("admin", "remove-key");
        custom_tracing::record_b58("pub_key", &request.pubkey);
        custom_tracing::record_b58("signer", &request.signer);

        let signer = verify_public_key(&request.signer)?;
        self.verify_admin_request_signature(&signer, &request)?;

        admin::remove_key(request.pubkey.clone().into(), &self.pool)
            .and_then(|deleted| async move {
                match deleted {
                    Some((pubkey, key_type)) => {
                        self.auth_updater.send_modify(|cache| {
                            cache.remove(&pubkey);
                        });
                        tracing::info!(%key_type,"key de-authorized");
                        Ok(())
                    }
                    None => Ok(()),
                }
            })
            .map_err(|_| {
                let pubkey: PublicKeyBinary = request.pubkey.into();
                tracing::error!("pubkey remove failed");
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
        telemetry::count_request("admin", "load-region");
        custom_tracing::record_b58("signer", &request.signer);

        let signer = verify_public_key(&request.signer)?;
        self.verify_admin_request_signature(&signer, &request)?;

        let region = Region::try_from(request.region).map_err(|_| {
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
                    let region_tree_size = region_tree.len();
                    tracing::debug!(region_cells = region_tree_size, "new compacted region map");
                    telemetry::gauge_hexes(region_tree_size);
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
        telemetry::count_request("admin", "region-params");
        custom_tracing::record_b58("signer", &request.signer);

        let signer = verify_public_key(&request.signer)?;
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
