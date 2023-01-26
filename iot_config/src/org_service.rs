use crate::{
    lora_field,
    org::{self, OrgStatus},
    route::list_routes,
    GrpcResult, Settings, HELIUM_NET_ID,
};
use anyhow::Result;
use file_store::traits::MsgVerify;
use helium_crypto::{Network, PublicKey};
use helium_proto::services::iot_config::{
    self, ActionV1, OrgCreateHeliumReqV1, OrgCreateRoamerReqV1, OrgDisableReqV1, OrgDisableResV1,
    OrgEnableReqV1, OrgEnableResV1, OrgGetReqV1, OrgListReqV1, OrgListResV1, OrgResV1, OrgV1,
    RouteStreamResV1,
};
use sqlx::{Pool, Postgres};
use tokio::sync::broadcast::Sender;
use tonic::{Request, Response, Status};

pub struct OrgService {
    admin_pubkey: PublicKey,
    pool: Pool<Postgres>,
    required_network: Network,
    route_update_tx: Sender<RouteStreamResV1>,
}

impl OrgService {
    pub async fn new(
        settings: &Settings,
        route_update_tx: Sender<RouteStreamResV1>,
    ) -> Result<Self> {
        Ok(Self {
            admin_pubkey: settings.admin_pubkey()?,
            pool: settings.database.connect(10).await?,
            required_network: settings.network,
            route_update_tx,
        })
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
            .map_err(|_| Status::internal("org get failed"))?;
        let net_id = org
            .constraints
            .start_addr
            .to_net_id()
            .map_err(|_| Status::internal("net id error"))?;

        Ok(Response::new(OrgResV1 {
            org: Some(org.org.into()),
            net_id: net_id.into(),
            devaddr_constraints: vec![org.constraints.into()],
        }))
    }

    async fn create_helium(&self, request: Request<OrgCreateHeliumReqV1>) -> GrpcResult<OrgResV1> {
        let request = request.into_inner();

        let req = self.verify_admin_signature(request)?;

        let verify_keys: Vec<&[u8]> = vec![req.owner.as_ref(), req.payer.as_ref()];

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

        let requested_addrs = req.devaddrs;
        let devaddr_constraint = org::next_helium_devaddr(&self.pool)
            .await
            .map_err(|_| Status::failed_precondition("helium address unavailable"))?
            .to_range(requested_addrs);

        let org = org::create_org(req.owner.into(), req.payer.into(), vec![], &self.pool)
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

        let req = self.verify_admin_signature(request)?;
        let verify_keys: Vec<&[u8]> = vec![req.owner.as_ref(), req.payer.as_ref()];
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

        let net_id = lora_field::net_id(req.net_id);
        let devaddr_range = net_id
            .full_range()
            .map_err(|_| Status::invalid_argument("invalid net_id"))?;

        let org = org::create_org(req.owner.into(), req.payer.into(), vec![], &self.pool)
            .await
            .map_err(|_| Status::internal("org save failed"))?;

        org::insert_constraints(org.oui, net_id, &devaddr_range, &self.pool)
            .await
            .map_err(|_| Status::internal("org constraints save failed"))?;

        Ok(Response::new(OrgResV1 {
            org: Some(org.into()),
            net_id: net_id.into(),
            devaddr_constraints: vec![devaddr_range.into()],
        }))
    }

    async fn disable(&self, request: Request<OrgDisableReqV1>) -> GrpcResult<OrgDisableResV1> {
        let request = request.into_inner();

        let req = self.verify_admin_signature(request)?;

        org::toggle_status(req.oui, OrgStatus::Disabled, &self.pool)
            .await
            .map_err(|_| Status::internal(format!("org disable failed for: {}", req.oui)))?;

        let org_routes = list_routes(req.oui, &self.pool).await.map_err(|_| {
            Status::internal(format!(
                "error retrieving routes for disabled org: {}",
                req.oui
            ))
        })?;

        for route in org_routes {
            let route_id = route.id.clone();
            self.route_update_tx
                .send(RouteStreamResV1 {
                    action: ActionV1::Delete.into(),
                    route: Some(route.into()),
                })
                .map_err(|_| {
                    Status::internal(format!(
                        "failed updating routers with deleted route: {route_id}"
                    ))
                })?;
            tracing::debug!("updated packet routers with removed route: {route_id}");
        }

        Ok(Response::new(OrgDisableResV1 { oui: req.oui }))
    }

    async fn enable(&self, request: Request<OrgEnableReqV1>) -> GrpcResult<OrgEnableResV1> {
        let request = request.into_inner();

        let req = self.verify_admin_signature(request)?;

        org::toggle_status(req.oui, OrgStatus::Enabled, &self.pool)
            .await
            .map_err(|_| Status::internal(format!("org enable failed for: {}", req.oui)))?;

        let org_routes = list_routes(req.oui, &self.pool).await.map_err(|_| {
            Status::internal(format!(
                "error retrieving routes for enabled org: {}",
                req.oui
            ))
        })?;

        for route in org_routes {
            let route_id = route.id.clone();
            self.route_update_tx
                .send(RouteStreamResV1 {
                    action: ActionV1::Create.into(),
                    route: Some(route.into()),
                })
                .map_err(|_| {
                    Status::internal(format!(
                        "failed updating routers with created route: {route_id}"
                    ))
                })?;
            tracing::debug!("updated packet routers with recreated route: {route_id}");
        }

        Ok(Response::new(OrgEnableResV1 { oui: req.oui }))
    }
}
