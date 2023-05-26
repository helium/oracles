use crate::{
    admin::{AuthCache, KeyType},
    lora_field, org,
    route::list_routes,
    telemetry, verify_public_key, GrpcResult, Settings, HELIUM_NET_ID,
};
use anyhow::Result;
use chrono::Utc;
use file_store::traits::{MsgVerify, TimestampEncode};
use helium_crypto::{Keypair, PublicKey, Sign};
use helium_proto::{
    services::iot_config::{
        self, route_stream_res_v1, ActionV1, OrgCreateHeliumReqV1, OrgCreateRoamerReqV1,
        OrgDisableReqV1, OrgDisableResV1, OrgEnableReqV1, OrgEnableResV1, OrgGetReqV1,
        OrgListReqV1, OrgListResV1, OrgResV1, OrgV1, RouteStreamResV1,
    },
    Message,
};
use sqlx::{Pool, Postgres};
use tokio::sync::{broadcast, watch};
use tonic::{Request, Response, Status};

pub struct OrgService {
    auth_cache: AuthCache,
    pool: Pool<Postgres>,
    route_update_tx: broadcast::Sender<RouteStreamResV1>,
    signing_key: Keypair,
    delegate_updater: watch::Sender<org::DelegateCache>,
}

impl OrgService {
    pub fn new(
        settings: &Settings,
        auth_cache: AuthCache,
        pool: Pool<Postgres>,
        route_update_tx: broadcast::Sender<RouteStreamResV1>,
        delegate_updater: watch::Sender<org::DelegateCache>,
    ) -> Result<Self> {
        Ok(Self {
            auth_cache,
            pool,
            route_update_tx,
            signing_key: settings.signing_keypair()?,
            delegate_updater,
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
impl iot_config::Org for OrgService {
    async fn list(&self, _request: Request<OrgListReqV1>) -> GrpcResult<OrgListResV1> {
        telemetry::count_request("org", "list");

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
        resp.signature = self.sign_response(&resp.encode_to_vec())?;

        Ok(Response::new(resp))
    }

    async fn get(&self, request: Request<OrgGetReqV1>) -> GrpcResult<OrgResV1> {
        let request = request.into_inner();
        telemetry::count_request("org", "get");

        let org = org::get(request.oui, &self.pool).await.map_err(|err| {
            tracing::error!("get org {} request failed {err:?}", request.oui);
            Status::internal("org get failed")
        })?;
        let net_id = if let Some(ref constraints) = org.constraints {
            constraints
                .first()
                .map(|constraint| constraint.start_addr.to_net_id())
                .ok_or(Status::not_found("no org devaddr constraints"))?
                .map_err(|_| Status::invalid_argument("invalid net id"))?
        } else {
            return Err(Status::not_found(
                "invalid org; no valid devaddr constraints",
            ));
        };

        let devaddr_constraints = org
            .constraints
            .as_ref()
            .map_or_else(Vec::new, |constraints| {
                constraints
                    .iter()
                    .map(|constraint| constraint.clone().into())
                    .collect()
            });

        let mut resp = OrgResV1 {
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

    async fn create_helium(&self, request: Request<OrgCreateHeliumReqV1>) -> GrpcResult<OrgResV1> {
        let request = request.into_inner();
        telemetry::count_request("org", "create-helium");

        let signer = verify_public_key(&request.signer)?;
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
                verify_public_key(key).map_err(|err| {
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
            HELIUM_NET_ID,
            &devaddr_constraint,
            &self.pool,
        )
        .await
        .map_err(|err| {
            tracing::error!("org save failed: {err}");
            Status::internal("org save failed: {err}")
        })?;

        org.delegate_keys.as_ref().map(|keys| {
            self.delegate_updater.send_if_modified(|cache| {
                keys.iter().fold(
                    false,
                    |acc, key| {
                        if cache.insert(key.clone()) {
                            true
                        } else {
                            acc
                        }
                    },
                )
            })
        });

        let mut resp = OrgResV1 {
            org: Some(org.into()),
            net_id: HELIUM_NET_ID.into(),
            devaddr_constraints: vec![devaddr_constraint.into()],
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp.encode_to_vec())?;

        Ok(Response::new(resp))
    }

    async fn create_roamer(&self, request: Request<OrgCreateRoamerReqV1>) -> GrpcResult<OrgResV1> {
        let request = request.into_inner();
        telemetry::count_request("org", "create-roamer");

        let signer = verify_public_key(&request.signer)?;
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
                verify_public_key(key).map_err(|err| {
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
            net_id,
            &devaddr_range,
            &self.pool,
        )
        .await
        .map_err(|err| {
            tracing::error!("failed to create org {err}");
            Status::internal("org save failed: {err}")
        })?;

        org.delegate_keys.as_ref().map(|keys| {
            self.delegate_updater.send_if_modified(|cache| {
                keys.iter().fold(
                    false,
                    |acc, key| {
                        if cache.insert(key.clone()) {
                            true
                        } else {
                            acc
                        }
                    },
                )
            })
        });

        let mut resp = OrgResV1 {
            org: Some(org.into()),
            net_id: net_id.into(),
            devaddr_constraints: vec![devaddr_range.into()],
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp.encode_to_vec())?;

        Ok(Response::new(resp))
    }

    async fn disable(&self, request: Request<OrgDisableReqV1>) -> GrpcResult<OrgDisableResV1> {
        let request = request.into_inner();
        telemetry::count_request("org", "disable");

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
                update.signature = self.sign_response(&update.encode_to_vec())?;
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
        resp.signature = self.sign_response(&resp.encode_to_vec())?;

        Ok(Response::new(resp))
    }

    async fn enable(&self, request: Request<OrgEnableReqV1>) -> GrpcResult<OrgEnableResV1> {
        let request = request.into_inner();
        telemetry::count_request("org", "enable");

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
                update.signature = self.sign_response(&update.encode_to_vec())?;
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
        resp.signature = self.sign_response(&resp.encode_to_vec())?;

        Ok(Response::new(resp))
    }
}
