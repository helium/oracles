use std::sync::Arc;

use crate::{
    admin::{AuthCache, KeyType},
    broadcast_update, helium_netids, lora_field, org,
    route::list_routes,
    telemetry, verify_public_key, GrpcResult,
};
use anyhow::Result;
use chrono::Utc;
use file_store::traits::TimestampEncode;
use helium_crypto::{Keypair, PublicKey, Sign};
use helium_proto::{
    services::iot_config::{
        self, route_stream_res_v1, ActionV1, DevaddrConstraintV1, OrgCreateHeliumReqV1,
        OrgCreateRoamerReqV1, OrgDisableReqV1, OrgDisableResV1, OrgEnableReqV1, OrgEnableResV1,
        OrgGetReqV1, OrgListReqV1, OrgListResV1, OrgResV1, OrgUpdateReqV1, OrgV1, RouteStreamResV1,
    },
    traits::msg_verify::MsgVerify,
    Message,
};
use sqlx::{Pool, Postgres};
use tokio::sync::{broadcast, watch};
use tonic::{Request, Response, Status};

pub struct OrgService {
    auth_cache: AuthCache,
    pool: Pool<Postgres>,
    route_update_tx: broadcast::Sender<RouteStreamResV1>,
    signing_key: Arc<Keypair>,
    delegate_updater: watch::Sender<org::DelegateCache>,
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
        delegate_updater: watch::Sender<org::DelegateCache>,
    ) -> Result<Self> {
        Ok(Self {
            auth_cache,
            pool,
            route_update_tx,
            signing_key,
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

    async fn verify_update_request_signature(
        &self,
        signer: &PublicKey,
        request: &OrgUpdateReqV1,
    ) -> Result<UpdateAuthorizer, Status> {
        if self
            .auth_cache
            .verify_signature_with_type(KeyType::Administrator, signer, request)
            .is_ok()
        {
            tracing::debug!(signer = signer.to_string(), "request authorized by admin");
            return Ok(UpdateAuthorizer::Admin);
        }

        let org_owner = org::get(request.oui, &self.pool)
            .await
            .transpose()
            .ok_or_else(|| Status::not_found(format!("oui: {}", request.oui)))?
            .map(|org| org.owner)
            .map_err(|_| Status::internal("auth verification error"))?;
        if org_owner == signer.clone().into() && request.verify(signer).is_ok() {
            tracing::debug!(
                signer = signer.to_string(),
                "request authorized by delegate"
            );
            return Ok(UpdateAuthorizer::Org);
        }

        Err(Status::permission_denied("unauthorized request signature"))
    }

    fn sign_response(&self, response: &[u8]) -> Result<Vec<u8>, Status> {
        self.signing_key
            .sign(response)
            .map_err(|_| Status::internal("response signing error"))
    }

    async fn stream_org_routes_enable_disable(&self, oui: u64) -> Result<(), Status> {
        let routes = list_routes(oui, &self.pool).await.map_err(|err| {
            tracing::error!(org = oui, reason = ?err, "failed to list org routes for streaming update");
            Status::internal(format!("error retrieving routes for updated org: {oui}"))
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
                Status::not_found("invalid org; no valid devaddr constraints")
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
        custom_tracing::record_b58("pub_key", &request.owner);
        custom_tracing::record_b58("signer", &request.signer);

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
                    tracing::error!(reason = ?err, "failed pubkey validation");
                    Status::invalid_argument(format!("failed pubkey validation: {err:?}"))
                })
            })
            .collect::<Result<Vec<PublicKey>, Status>>()?;

        tracing::info!(?request, "create helium org");

        let net_id = request.net_id();
        let requested_addrs = if request.devaddrs >= 8 && request.devaddrs % 2 == 0 {
            request.devaddrs
        } else {
            return Err(Status::invalid_argument(format!(
                "{} devaddrs requested; minimum 8, even number required",
                request.devaddrs
            )));
        };

        let mut txn = self
            .pool
            .begin()
            .await
            .map_err(|_| Status::internal("error saving org record"))?;
        let devaddr_constraints = helium_netids::checkout_devaddr_constraints(&mut txn, requested_addrs, net_id.into())
            .await
            .map_err(|err| {
                tracing::error!(?net_id, count = %requested_addrs, reason = ?err, "failed to retrieve available helium devaddrs");
                Status::failed_precondition("helium addresses unavailable")
            })?;
        tracing::info!(constraints = ?devaddr_constraints, "devaddr constraints issued");
        let helium_netid_field = helium_netids::HeliumNetId::from(net_id).id();

        let org = org::create_org(
            request.owner.into(),
            request.payer.into(),
            request
                .delegate_keys
                .into_iter()
                .map(|key| key.into())
                .collect(),
            helium_netid_field,
            &devaddr_constraints,
            &mut *txn,
        )
        .await
        .map_err(|err| {
            tracing::error!(reason = ?err, "org save failed");
            Status::internal(format!("org save failed: {err:?}"))
        })?;

        txn.commit()
            .await
            .map_err(|_| Status::internal("error saving org record"))?;

        org.delegate_keys.as_ref().map(|keys| {
            self.delegate_updater.send_if_modified(|cache| {
                keys.iter().fold(false, |acc, key| {
                    if cache.insert(key.clone()) {
                        tracing::info!(%key, "delegate key authorized");
                        true
                    } else {
                        acc
                    }
                })
            })
        });

        let devaddr_constraints = org
            .constraints
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(DevaddrConstraintV1::from)
            .collect();
        let mut resp = OrgResV1 {
            org: Some(org.into()),
            net_id: helium_netid_field.into(),
            devaddr_constraints,
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
        custom_tracing::record_b58("pub_key", &request.owner);
        custom_tracing::record_b58("signer", &request.signer);

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
                    Status::invalid_argument(format!("failed pubkey validation: {err:?}"))
                })
            })
            .collect::<Result<Vec<PublicKey>, Status>>()?;

        tracing::info!(?request, "create roamer org");

        let net_id = lora_field::net_id(request.net_id);
        let devaddr_range = net_id
            .full_range()
            .map_err(|_| Status::invalid_argument("invalid net_id"))?;
        tracing::info!(constraints = ?devaddr_range, "roaming devaddr range");

        let org = org::create_org(
            request.owner.into(),
            request.payer.into(),
            request
                .delegate_keys
                .into_iter()
                .map(|key| key.into())
                .collect(),
            net_id,
            &[devaddr_range],
            &self.pool,
        )
        .await
        .map_err(|err| {
            tracing::error!(reason = ?err, "failed to create org");
            Status::internal(format!("org save failed: {err:?}"))
        })?;

        org.delegate_keys.as_ref().map(|keys| {
            self.delegate_updater.send_if_modified(|cache| {
                keys.iter().fold(false, |acc, key| {
                    if cache.insert(key.clone()) {
                        tracing::info!(?key, "delegate key authorized");
                        true
                    } else {
                        acc
                    }
                })
            })
        });

        let devaddr_constraints = org
            .constraints
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(DevaddrConstraintV1::from)
            .collect();
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

    async fn update(&self, request: Request<OrgUpdateReqV1>) -> GrpcResult<OrgResV1> {
        let request = request.into_inner();
        telemetry::count_request("org", "update");
        custom_tracing::record("oui", request.oui);
        custom_tracing::record_b58("signer", &request.signer);

        let signer = verify_public_key(&request.signer)?;
        let authorizer = self
            .verify_update_request_signature(&signer, &request)
            .await?;

        let org = org::update_org(
            request.oui,
            authorizer,
            request.updates,
            &self.pool,
            &self.delegate_updater,
        )
        .await
        .map_err(|err| {
            tracing::error!(reason = ?err, "org update failed");
            Status::internal(format!("org update failed: {err:?}"))
        })?;

        let net_id = org::get_org_netid(org.oui, &self.pool)
            .await
            .map_err(|err| {
                tracing::error!(oui = org.oui, reason = ?err, "get org net id failed");
                Status::not_found("invalid org; no valid devaddr constraints")
            })?;

        let devaddr_constraints = org
            .constraints
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(DevaddrConstraintV1::from)
            .collect();
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
