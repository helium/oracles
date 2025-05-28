use std::sync::Arc;

use anyhow::Result;
use chrono::Utc;
use file_store::traits::{MsgVerify, TimestampEncode};
use helium_crypto::{Keypair, PublicKey, Sign};
use helium_proto::{
    services::iot_config::{
        self, route_stream_res_v1, ActionV1, OrgCreateHeliumReqV1, OrgCreateRoamerReqV1,
        OrgDisableReqV1, OrgDisableResV1, OrgEnableReqV1, OrgEnableResV1, OrgGetReqV1, OrgGetReqV2,
        OrgListReqV1, OrgListReqV2, OrgListResV1, OrgListResV2, OrgResV1, OrgResV2, OrgUpdateReqV1,
        OrgV2, RouteStreamResV1,
    },
    Message,
};
use sqlx::{Pool, Postgres};
use tokio::sync::broadcast;
use tonic::{Request, Response, Status};

use crate::{
    admin::AuthCache, broadcast_update, org, route::list_routes, telemetry, verify_public_key,
    GrpcResult,
};

pub struct OrgService {
    auth_cache: AuthCache,
    pool: Pool<Postgres>,
    route_update_tx: broadcast::Sender<RouteStreamResV1>,
    signing_key: Arc<Keypair>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum UpdateAuthorizer {
    Admin,
    Org,
}

impl OrgService {
    pub fn new(
        signing_key: Arc<Keypair>,
        auth_cache: AuthCache,
        pool: Pool<Postgres>,
        route_update_tx: broadcast::Sender<RouteStreamResV1>,
    ) -> Result<Self> {
        Ok(Self {
            auth_cache,
            pool,
            route_update_tx,
            signing_key,
        })
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

    async fn stream_org_routes_enable_disable(&self, oui: u64) -> Result<(), Status> {
        let routes = list_routes(oui, &self.pool).await.map_err(|err| {
            tracing::error!(org = oui, reason = ?err, "failed to list org routes for streaming update");
            Status::internal(format!("error retrieving routes for updated org: {}", oui))
        })?;
        let timestamp = Utc::now().encode_timestamp();
        let signer: Vec<u8> = self.signing_key.public_key().into();
        for route in routes {
            let route_id = route.id.clone();
            let mut update = RouteStreamResV1 {
                action: ActionV1::Add.into(),
                data: Some(route_stream_res_v1::Data::Route(route.into())),
                timestamp,
                signer: signer.clone(),
                signature: vec![],
            };
            update.signature = self.sign_response(&update.encode_to_vec())?;
            if broadcast_update(update, self.route_update_tx.clone())
                .await
                .is_err()
            {
                tracing::info!(
                    route_id,
                    "all subscribers disconnected; org routes update incomplete"
                );
                break;
            };
            tracing::debug!(route_id, "route updated");
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl iot_config::Org for OrgService {
    async fn list(&self, _request: Request<OrgListReqV1>) -> GrpcResult<OrgListResV1> {
        telemetry::count_request("org", "list_deprecated_call");
        tracing::warn!(
            "Deprecated API endpoint 'org.list' was called. This endpoint is no longer supported."
        );

        Err(Status::failed_precondition(
            "This API endpoint (org.list) has been deprecated and is no longer supported. \
            Please use org.list_v2 instead. Refer to API documentation for migration details.",
        ))
    }

    async fn list_v2(&self, _request: Request<OrgListReqV2>) -> GrpcResult<OrgListResV2> {
        telemetry::count_request("org", "list_v2");

        let proto_orgs: Vec<OrgV2> = org::list(&self.pool)
            .await
            .map_err(|_| Status::internal("org list failed"))?
            .into_iter()
            .map(|org| org.into())
            .collect();

        let mut resp = OrgListResV2 {
            orgs: proto_orgs,
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp.encode_to_vec())?;

        Ok(Response::new(resp))
    }

    async fn get(&self, _request: Request<OrgGetReqV1>) -> GrpcResult<OrgResV1> {
        telemetry::count_request("org", "get_deprecated_call");
        tracing::warn!(
            "Deprecated API endpoint 'org.get' was called. This endpoint is no longer supported."
        );

        Err(Status::failed_precondition(
            "This API endpoint (org.get) has been deprecated and is no longer supported. \
            Please use org.get_v2 instead. Refer to API documentation for migration details.",
        ))
    }

    async fn get_v2(&self, request: Request<OrgGetReqV2>) -> GrpcResult<OrgResV2> {
        let request = request.into_inner();
        telemetry::count_request("org", "get");
        custom_tracing::record("oui", request.oui);

        let org = org::get(request.oui, &self.pool)
            .await
            .map_err(|err| {
                tracing::error!(reason = ?err, "get org request failed");
                Status::internal("org get failed")
            })?
            .ok_or_else(|| Status::not_found(format!("oui: {}", request.oui)))?;

        let net_id = org::get_org_netid(org.oui, &self.pool)
            .await
            .map_err(|err| {
                tracing::error!(oui = org.oui, reason = ?err, "get org net id failed");
                Status::not_found("invalid org; no net id found")
            })?;

        let devaddr_constraints = org
            .constraints
            .as_ref()
            .map_or_else(Vec::new, |constraints| {
                constraints
                    .iter()
                    .map(|constraint| constraint.clone().into())
                    .collect()
            });

        let mut resp = OrgResV2 {
            org: Some(org.into()),
            net_id: net_id.into(),
            devaddr_constraints,
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };

        resp.signature = self.sign_response(&resp.encode_to_vec())?;
        Ok(Response::new(resp))
    }

    async fn create_helium(&self, _request: Request<OrgCreateHeliumReqV1>) -> GrpcResult<OrgResV1> {
        telemetry::count_request("org", "create_helium");
        tracing::warn!(
            "Deprecated API endpoint 'org.create_helium' was called. This endpoint is no longer supported."
        );

        Err(Status::failed_precondition(
            "This API endpoint (org.create_helium) has been deprecated and is no longer supported.",
        ))
    }

    async fn create_roamer(&self, _request: Request<OrgCreateRoamerReqV1>) -> GrpcResult<OrgResV1> {
        telemetry::count_request("org", "create_roamer");
        tracing::warn!(
            "Deprecated API endpoint 'org.create_roamer' was called. This endpoint is no longer supported."
        );

        Err(Status::failed_precondition(
            "This API endpoint (org.create_roamer) has been deprecated and is no longer supported.",
        ))
    }

    async fn update(&self, _request: Request<OrgUpdateReqV1>) -> GrpcResult<OrgResV1> {
        telemetry::count_request("org", "update");
        tracing::warn!(
            "Deprecated API endpoint 'org.update' was called. This endpoint is no longer supported."
        );

        Err(Status::failed_precondition(
            "This API endpoint (org.update) has been deprecated and is no longer supported.",
        ))
    }

    async fn disable(&self, request: Request<OrgDisableReqV1>) -> GrpcResult<OrgDisableResV1> {
        let request = request.into_inner();
        telemetry::count_request("org", "disable");
        custom_tracing::record("oui", request.oui);
        custom_tracing::record_b58("signer", &request.signer);

        let signer = verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request)?;

        if !org::is_locked(request.oui, &self.pool)
            .await
            .map_err(|_| Status::internal("error retrieving current status"))?
        {
            org::toggle_locked(request.oui, &self.pool)
                .await
                .map_err(|err| {
                    tracing::error!(
                        org = request.oui,
                        reason = ?err,
                        "failed to disable org with reason"
                    );
                    Status::internal(format!("org disable failed for: {}", request.oui))
                })?;
            tracing::info!(oui = request.oui, "org locked");

            self.stream_org_routes_enable_disable(request.oui).await?
        }

        let mut resp = OrgDisableResV1 {
            oui: request.oui,
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp.encode_to_vec())?;

        Ok(Response::new(resp))
    }

    async fn enable(&self, request: Request<OrgEnableReqV1>) -> GrpcResult<OrgEnableResV1> {
        let request = request.into_inner();
        telemetry::count_request("org", "enable");
        custom_tracing::record("oui", request.oui);
        custom_tracing::record_b58("signer", &request.signer);

        let signer = verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request)?;

        if org::is_locked(request.oui, &self.pool)
            .await
            .map_err(|_| Status::internal("error retrieving current status"))?
        {
            org::toggle_locked(request.oui, &self.pool)
                .await
                .map_err(|err| {
                    tracing::error!(
                        org = request.oui,
                        reason = ?err,
                        "failed to enable org with reason"
                    );
                    Status::internal(format!("org enable failed for: {}", request.oui))
                })?;
            tracing::info!(oui = request.oui, "org unlocked");

            self.stream_org_routes_enable_disable(request.oui).await?
        }

        let mut resp = OrgEnableResV1 {
            oui: request.oui,
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp.encode_to_vec())?;

        Ok(Response::new(resp))
    }
}
