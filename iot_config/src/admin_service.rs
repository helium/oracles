use crate::{
    admin::{self, AuthCache, KeyType},
    region_map::{self, RegionMap},
    GrpcResult,
};
use anyhow::Result;
use file_store::traits::MsgVerify;
use futures::future::TryFutureExt;
use helium_crypto::{Network, PublicKey, PublicKeyBinary};
use helium_proto::services::iot_config::{
    self, AdminAddKeyReqV1, AdminKeyResV1, AdminLoadRegionReqV1, AdminLoadRegionResV1,
    AdminRemoveKeyReqV1,
};
use sqlx::{Pool, Postgres};
use tonic::{Request, Response, Status};

pub struct AdminService {
    auth_cache: AuthCache,
    pool: Pool<Postgres>,
    region_map: RegionMap,
    required_network: Network,
}

impl AdminService {
    pub fn new(
        auth_cache: AuthCache,
        pool: Pool<Postgres>,
        region_map: RegionMap,
        required_network: Network,
    ) -> Self {
        Self {
            auth_cache,
            pool,
            region_map,
            required_network,
        }
    }

    async fn verify_request_signature<R>(&self, request: &R) -> Result<(), Status>
    where
        R: MsgVerify,
    {
        self.auth_cache
            .verify_signature(KeyType::Administrator, request)
            .await
            .map_err(|_| Status::permission_denied("invalid admin signature"))?;
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
}

#[tonic::async_trait]
impl iot_config::Admin for AdminService {
    async fn add_key(&self, request: Request<AdminAddKeyReqV1>) -> GrpcResult<AdminKeyResV1> {
        let request = request.into_inner();

        self.verify_request_signature(&request).await?;

        let key_type = request.key_type().into();
        let pubkey = self
            .verify_public_key(request.pubkey.as_ref())
            .and_then(|pubkey| self.verify_network(pubkey))
            .map_err(|_| Status::invalid_argument("invalid pubkey supplied"))?;

        admin::insert_key(request.pubkey.clone().into(), key_type, &self.pool)
            .and_then(|_| async move {
                self.auth_cache.insert_key(key_type, pubkey).await;
                Ok(())
            })
            .map_err(|_| {
                let pubkey: PublicKeyBinary = request.pubkey.into();
                tracing::error!(pubkey = pubkey.to_string(), "pubkey add failed");
                Status::internal(format!("error saving requested key: {pubkey}"))
            })
            .await?;

        Ok(Response::new(AdminKeyResV1 {}))
    }

    async fn remove_key(&self, request: Request<AdminRemoveKeyReqV1>) -> GrpcResult<AdminKeyResV1> {
        let request = request.into_inner();

        self.verify_request_signature(&request).await?;

        admin::remove_key(request.pubkey.clone().into(), &self.pool)
            .and_then(|deleted| async move {
                match deleted {
                    Some((pubkey, key_type)) => {
                        self.auth_cache.remove_key(key_type, &pubkey).await;
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

        Ok(Response::new(AdminKeyResV1 {}))
    }

    async fn load_region(
        &self,
        request: Request<AdminLoadRegionReqV1>,
    ) -> GrpcResult<AdminLoadRegionResV1> {
        let request = request.into_inner();
        self.verify_request_signature(&request).await?;

        let region = Region::from_i32(request.region).ok_or(Status::invalid_argument(format!(
            "invalid lora region {}",
            request.region
        )))?;

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

        let updated_region = region_map::update_region(region, &params, idz, &self.pool)
            .await
            .map_err(|err| {
                tracing::error!(
                    region = region.to_string(),
                    "failed to update region: {err:?}"
                );
                Status::internal("region update failed")
            })?;

        self.region_map.insert_params(region, params).await;
        if let Some(region_tree) = updated_region {
            tracing::debug!(region_cells = region_tree.len(), "new compacted region map");
            self.region_map.replace_tree(region_tree).await;
        }

        Ok(Response::new(AdminLoadRegionResV1 {}))
    }
}
