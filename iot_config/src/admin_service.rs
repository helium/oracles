use crate::{
    region_map::{self, RegionMap},
    GrpcResult, Settings,
};
use anyhow::Result;
use file_store::traits::MsgVerify;
use helium_crypto::PublicKey;
use helium_proto::{
    services::iot_config::{
        self, AdminAddKeyReqV1, AdminKeyResV1, AdminLoadRegionReqV1, AdminLoadRegionResV1,
        AdminRemoveKeyReqV1,
    },
    Region,
};
use sqlx::{Pool, Postgres};
use tonic::{Request, Response, Status};

pub struct AdminService {
    admin_pubkey: PublicKey,
    pool: Pool<Postgres>,
    region_map: RegionMap,
}

impl AdminService {
    pub fn new(settings: &Settings, pool: Pool<Postgres>, region_map: RegionMap) -> Result<Self> {
        Ok(Self {
            admin_pubkey: settings.admin_pubkey()?,
            pool,
            region_map,
        })
    }

    fn verify_admin_signature<R>(&self, request: R) -> Result<R, Status>
    where
        R: MsgVerify,
    {
        request
            .verify(&self.admin_pubkey)
            .map_err(|_| Status::permission_denied("invalid admin signature"))?;
        Ok(request)
    }
}

#[tonic::async_trait]
impl iot_config::Admin for AdminService {
    async fn add_key(&self, _request: Request<AdminAddKeyReqV1>) -> GrpcResult<AdminKeyResV1> {
        unimplemented!();
    }

    async fn remove_key(
        &self,
        _request: Request<AdminRemoveKeyReqV1>,
    ) -> GrpcResult<AdminKeyResV1> {
        unimplemented!();
    }

    async fn load_region(
        &self,
        request: Request<AdminLoadRegionReqV1>,
    ) -> GrpcResult<AdminLoadRegionResV1> {
        let request = request.into_inner();
        let req = self.verify_admin_signature(request)?;

        let region = Region::from_i32(req.region)
            .ok_or_else(|| Status::invalid_argument("invalid region"))?;

        let params = match req.params {
            Some(params) => params,
            None => return Err(Status::invalid_argument("missing region")),
        };

        let idz = if !req.hex_indexes.is_empty() {
            Some(req.hex_indexes.as_ref())
        } else {
            None
        };

        let updated_region = region_map::update_region(region, &params, idz, &self.pool)
            .await
            .map_err(|_| Status::internal("region update failed"))?;

        self.region_map.insert_params(region, params).await;
        if let Some(region_tree) = updated_region {
            tracing::debug!("New compacted region map with {} cells", region_tree.len());
            self.region_map.replace_tree(region_tree).await;
        }

        Ok(Response::new(AdminLoadRegionResV1 {}))
    }
}
