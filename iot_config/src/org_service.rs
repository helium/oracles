use crate::{
    admin::{AuthCache, KeyType},
    lora_field, org,
    route::list_routes,
    GrpcResult, Settings, HELIUM_NET_ID,
};
use anyhow::Result;
use chrono::Utc;
use file_store::traits::{MsgVerify, TimestampEncode};
use helium_crypto::{Keypair, Network, PublicKey, Sign};
use helium_proto::{
    services::iot_config::{
        self, route_stream_res_v1, ActionV1, OrgCreateHeliumReqV1, OrgCreateRoamerReqV1,
        OrgDisableReqV1, OrgDisableResV1, OrgEnableReqV1, OrgEnableResV1, OrgGetReqV1,
        OrgListReqV1, OrgListResV1, OrgResV1, OrgV1, RouteStreamResV1,
    },
    Message,
};
use sqlx::{Pool, Postgres};
use tokio::sync::broadcast::Sender;
use tonic::{Request, Response, Status};

pub struct OrgService {
    auth_cache: AuthCache,
    pool: Pool<Postgres>,
    required_network: Network,
    route_update_tx: Sender<RouteStreamResV1>,
    signing_key: Keypair,
}

impl OrgService {
    pub fn new(
        settings: &Settings,
        auth_cache: AuthCache,
        pool: Pool<Postgres>,
        route_update_tx: Sender<RouteStreamResV1>,
    ) -> Result<Self> {
        Ok(Self {
            auth_cache,
            pool,
            required_network: settings.network,
            route_update_tx,
            signing_key: settings.signing_keypair()?,
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
impl iot_config::Org for OrgService {
    async fn list(&self, _request: Request<OrgListReqV1>) -> GrpcResult<OrgListResV1> {
        let proto_orgs: Vec<OrgV1> = org::list(&self.pool)
            .await
            .map_err(|_| Status::internal("org list failed"))?
            .into_iter()
            .map(|org| org.into())
            .collect();

        let mut resp = OrgListResV1 {
            orgs: proto_orgs,
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp)?;

        Ok(Response::new(resp))
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

        let mut resp = OrgResV1 {
            org: Some(org.org.into()),
            net_id: net_id.into(),
            devaddr_constraints: org
                .constraints
                .into_iter()
                .map(|constraint| constraint.into())
                .collect(),
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp)?;

        Ok(Response::new(resp))
    }

    async fn create_helium(&self, request: Request<OrgCreateHeliumReqV1>) -> GrpcResult<OrgResV1> {
        let request = request.into_inner();

        let signer = self.verify_public_key(&request.signer)?;
        self.verify_admin_request_signature(&signer, &request)?;

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

        let mut resp = OrgResV1 {
            org: Some(org.into()),
            net_id: HELIUM_NET_ID.into(),
            devaddr_constraints: vec![devaddr_constraint.into()],
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp)?;

        Ok(Response::new(resp))
    }

    async fn create_roamer(&self, request: Request<OrgCreateRoamerReqV1>) -> GrpcResult<OrgResV1> {
        let request = request.into_inner();

        let signer = self.verify_public_key(&request.signer)?;
        self.verify_admin_request_signature(&signer, &request)?;

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

        let mut resp = OrgResV1 {
            org: Some(org.into()),
            net_id: net_id.into(),
            devaddr_constraints: vec![devaddr_range.into()],
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp)?;

        Ok(Response::new(resp))
    }

    async fn disable(&self, request: Request<OrgDisableReqV1>) -> GrpcResult<OrgDisableResV1> {
        let request = request.into_inner();

        let signer = self.verify_public_key(&request.signer)?;
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

            let timestamp = Utc::now().encode_timestamp();
            let signer: Vec<u8> = self.signing_key.public_key().into();
            for route in org_routes {
                let route_id = route.id.clone();
                let mut update = RouteStreamResV1 {
                    action: ActionV1::Add.into(),
                    data: Some(route_stream_res_v1::Data::Route(route.into())),
                    timestamp,
                    signer: signer.clone(),
                    signature: vec![],
                };
                update.signature = self.sign_response(&update)?;
                if self.route_update_tx.send(update).is_err() {
                    tracing::info!(
                        route_id = route_id,
                        "all subscribers disconnected; route disable incomplete"
                    );
                    break;
                };
                tracing::debug!(route_id = route_id, "route disabled");
            }
        }

        let mut resp = OrgDisableResV1 {
            oui: request.oui,
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp)?;

        Ok(Response::new(resp))
    }

    async fn enable(&self, request: Request<OrgEnableReqV1>) -> GrpcResult<OrgEnableResV1> {
        let request = request.into_inner();

        let signer = self.verify_public_key(&request.signer)?;
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

            let timestamp = Utc::now().encode_timestamp();
            let signer: Vec<u8> = self.signing_key.public_key().into();
            for route in org_routes {
                let route_id = route.id.clone();
                let mut update = RouteStreamResV1 {
                    action: ActionV1::Add.into(),
                    data: Some(route_stream_res_v1::Data::Route(route.into())),
                    timestamp,
                    signer: signer.clone(),
                    signature: vec![],
                };
                update.signature = self.sign_response(&update)?;
                if self.route_update_tx.send(update).is_err() {
                    tracing::info!(
                        route_id = route_id,
                        "all subscribers disconnected; route enable incomplete"
                    );
                    break;
                };
                tracing::debug!(route_id = route_id, "route enabled");
            }
        }

        let mut resp = OrgEnableResV1 {
            oui: request.oui,
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp)?;

        Ok(Response::new(resp))
    }
}
