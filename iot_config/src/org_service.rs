use crate::{
    admin::{AuthCache, KeyType},
    lora_field, org,
    route::list_routes,
    GrpcResult, HELIUM_NET_ID,
};
use anyhow::Result;
use file_store::traits::MsgVerify;
use helium_crypto::{Network, PublicKey};
use helium_proto::services::iot_config::{
    self, route_stream_res_v1, ActionV1, OrgCreateHeliumReqV1, OrgCreateRoamerReqV1,
    OrgDisableReqV1, OrgDisableResV1, OrgEnableReqV1, OrgEnableResV1, OrgGetReqV1, OrgListReqV1,
    OrgListResV1, OrgResV1, OrgV1, RouteStreamResV1,
};
use sqlx::{Pool, Postgres};
use tokio::sync::broadcast::Sender;
use tonic::{Request, Response, Status};

pub struct OrgService {
    auth_cache: AuthCache,
    pool: Pool<Postgres>,
    required_network: Network,
    route_update_tx: Sender<RouteStreamResV1>,
}

impl OrgService {
    pub fn new(
        auth_cache: AuthCache,
        pool: Pool<Postgres>,
        required_network: Network,
        route_update_tx: Sender<RouteStreamResV1>,
    ) -> Self {
        Self {
            auth_cache,
            pool,
            required_network,
            route_update_tx,
        }
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
}

#[tonic::async_trait]
impl iot_config::Org for OrgService {
    async fn list(&self, _request: Request<OrgListReqV1>) -> GrpcResult<OrgListResV1> {
        let proto_orgs: Vec<OrgV1> = org::list(&self.pool)
            .await
            .map_err(|_| Status::internal("org list failed"))?
            .into_iter()
            .map(|org| org.into())
            .collect();

        Ok(Response::new(OrgListResV1 { orgs: proto_orgs }))
    }

    async fn get(&self, request: Request<OrgGetReqV1>) -> GrpcResult<OrgResV1> {
        let request = request.into_inner();

        let org = org::get_with_constraints(request.oui, &self.pool)
            .await
            .map_err(|err| {
                tracing::error!("get org {} request failed {err:?}", request.oui);
                Status::internal("org get failed")
            })?;
        let net_id = org
            .constraints
            .first()
            .ok_or("not found")
            .and_then(|constraint| {
                constraint
                    .start_addr
                    .to_net_id()
                    .map_err(|_| "invalid net id")
            })
            .map_err(|err| Status::internal(format!("net id error: {err}")))?;

        Ok(Response::new(OrgResV1 {
            org: Some(org.org.into()),
            net_id: net_id.into(),
            devaddr_constraints: org
                .constraints
                .into_iter()
                .map(|constraint| constraint.into())
                .collect(),
        }))
    }

    async fn create_helium(&self, request: Request<OrgCreateHeliumReqV1>) -> GrpcResult<OrgResV1> {
        let request = request.into_inner();

        self.verify_request_signature(&request).await?;

        let mut verify_keys: Vec<&[u8]> = vec![request.owner.as_ref(), request.payer.as_ref()];
        let mut verify_delegates: Vec<&[u8]> = request
            .delegate_keys
            .iter()
            .map(|key| key.as_slice())
            .collect();
        verify_keys.append(&mut verify_delegates);
        _ = verify_keys
            .iter()
            .map(|key| {
                self.verify_public_key(key)
                    .and_then(|pub_key| self.verify_network(pub_key))
                    .map_err(|err| {
                        tracing::error!("failed pubkey validation: {err}");
                        Status::invalid_argument(format!("failed pubkey validation: {err}"))
                    })
            })
            .collect::<Result<Vec<PublicKey>, Status>>()?;

        tracing::debug!("create helium org request: {request:?}");

        let requested_addrs = request.devaddrs;
        let devaddr_constraint = org::next_helium_devaddr(&self.pool)
            .await
            .map_err(|err| {
                tracing::error!("failed to retrieve next helium network devaddr {err:?}");
                Status::failed_precondition("helium address unavailable")
            })?
            .to_range(requested_addrs);

        let org = org::create_org(
            request.owner.into(),
            request.payer.into(),
            request
                .delegate_keys
                .into_iter()
                .map(|key| key.into())
                .collect(),
            &self.pool,
        )
        .await
        .map_err(|err| {
            tracing::error!("org save failed: {err:?}");
            Status::internal("org save failed")
        })?;

        org::insert_constraints(org.oui, HELIUM_NET_ID, &devaddr_constraint, &self.pool)
            .await
            .map_err(|err| {
                tracing::error!("org constraints save failed: {err:?}");
                Status::internal("org constraints save failed")
            })?;

        Ok(Response::new(OrgResV1 {
            org: Some(org.into()),
            net_id: HELIUM_NET_ID.into(),
            devaddr_constraints: vec![devaddr_constraint.into()],
        }))
    }

    async fn create_roamer(&self, request: Request<OrgCreateRoamerReqV1>) -> GrpcResult<OrgResV1> {
        let request = request.into_inner();

        self.verify_request_signature(&request).await?;

        let mut verify_keys: Vec<&[u8]> = vec![request.owner.as_ref(), request.payer.as_ref()];
        let mut verify_delegates: Vec<&[u8]> = request
            .delegate_keys
            .iter()
            .map(|key| key.as_slice())
            .collect();
        verify_keys.append(&mut verify_delegates);
        _ = verify_keys
            .iter()
            .map(|key| {
                self.verify_public_key(key)
                    .and_then(|pub_key| self.verify_network(pub_key))
                    .map_err(|err| {
                        Status::invalid_argument(format!("failed pubkey validation: {err}"))
                    })
            })
            .collect::<Result<Vec<PublicKey>, Status>>()?;

        tracing::debug!("create roamer org request: {request:?}");

        let net_id = lora_field::net_id(request.net_id);
        let devaddr_range = net_id
            .full_range()
            .map_err(|_| Status::invalid_argument("invalid net_id"))?;

        let org = org::create_org(
            request.owner.into(),
            request.payer.into(),
            request
                .delegate_keys
                .into_iter()
                .map(|key| key.into())
                .collect(),
            &self.pool,
        )
        .await
        .map_err(|err| {
            tracing::error!("failed to create org {err:?}");
            Status::internal("org save failed")
        })?;

        org::insert_constraints(org.oui, net_id, &devaddr_range, &self.pool)
            .await
            .map_err(|err| {
                tracing::error!("failed to save org constraints {err:?}");
                Status::internal("org constraints save failed")
            })?;

        Ok(Response::new(OrgResV1 {
            org: Some(org.into()),
            net_id: net_id.into(),
            devaddr_constraints: vec![devaddr_range.into()],
        }))
    }

    async fn disable(&self, request: Request<OrgDisableReqV1>) -> GrpcResult<OrgDisableResV1> {
        let request = request.into_inner();

        self.verify_request_signature(&request).await?;

        if !org::is_locked(request.oui, &self.pool)
            .await
            .map_err(|_| Status::internal("error retrieving current status"))?
        {
            org::toggle_locked(request.oui, &self.pool)
                .await
                .map_err(|err| {
                    tracing::error!(
                        org = request.oui,
                        "failed to disable org with reason {err:?}"
                    );
                    Status::internal(format!("org disable failed for: {}", request.oui))
                })?;

            let org_routes = list_routes(request.oui, &self.pool).await.map_err(|err| {
                tracing::error!(
                    org = request.oui,
                    "failed to list org routes for streaming disable update {err:?}"
                );
                Status::internal(format!(
                    "error retrieving routes for disabled org: {}",
                    request.oui
                ))
            })?;

            for route in org_routes {
                let route_id = route.id.clone();
                if self
                    .route_update_tx
                    .send(RouteStreamResV1 {
                        action: ActionV1::Add.into(),
                        data: Some(route_stream_res_v1::Data::Route(route.into())),
                    })
                    .is_err()
                {
                    tracing::info!(
                        route_id = route_id,
                        "all subscribers disconnected; route disable incomplete"
                    );
                    break;
                };
                tracing::debug!(route_id = route_id, "route disabled");
            }
        }

        Ok(Response::new(OrgDisableResV1 { oui: request.oui }))
    }

    async fn enable(&self, request: Request<OrgEnableReqV1>) -> GrpcResult<OrgEnableResV1> {
        let request = request.into_inner();

        self.verify_request_signature(&request).await?;

        if org::is_locked(request.oui, &self.pool)
            .await
            .map_err(|_| Status::internal("error retrieving current status"))?
        {
            org::toggle_locked(request.oui, &self.pool)
                .await
                .map_err(|err| {
                    tracing::error!(
                        org = request.oui,
                        "failed to enable org with reason {err:?}"
                    );
                    Status::internal(format!("org enable failed for: {}", request.oui))
                })?;

            let org_routes = list_routes(request.oui, &self.pool).await.map_err(|err| {
                tracing::error!(
                    org = request.oui,
                    "failed to list routes for streaming enable update {err:?}"
                );
                Status::internal(format!(
                    "error retrieving routes for enabled org: {}",
                    request.oui
                ))
            })?;

            for route in org_routes {
                let route_id = route.id.clone();
                if self
                    .route_update_tx
                    .send(RouteStreamResV1 {
                        action: ActionV1::Add.into(),
                        data: Some(route_stream_res_v1::Data::Route(route.into())),
                    })
                    .is_err()
                {
                    tracing::info!(
                        route_id = route_id,
                        "all subscribers disconnected; route enable incomplete"
                    );
                    break;
                };
                tracing::debug!(route_id = route_id, "route enabled");
            }
        }

        Ok(Response::new(OrgEnableResV1 { oui: request.oui }))
    }
}
