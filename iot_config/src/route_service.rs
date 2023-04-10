use crate::{
    admin::{AuthCache, KeyType},
    lora_field::{DevAddrConstraint, DevAddrRange, EuiPair},
    org::{self, DbOrgError},
    route::{self, Route, RouteStorageError},
    update_channel, GrpcResult, GrpcStreamRequest, GrpcStreamResult, Settings,
};
use anyhow::{anyhow, Result};
use chrono::Utc;
use file_store::traits::{MsgVerify, TimestampEncode};
use futures::{
    future::TryFutureExt,
    stream::{StreamExt, TryStreamExt},
};
use helium_crypto::{Keypair, PublicKey, Sign};
use helium_proto::{
    services::iot_config::{
        self, route_stream_res_v1, ActionV1, DevaddrRangeV1, EuiPairV1, RouteCreateReqV1,
        RouteDeleteReqV1, RouteDevaddrRangesResV1, RouteEuisResV1, RouteGetDevaddrRangesReqV1,
        RouteGetEuisReqV1, RouteGetReqV1, RouteListReqV1, RouteListResV1, RouteResV1,
        RouteStreamReqV1, RouteStreamResV1, RouteUpdateDevaddrRangesReqV1, RouteUpdateEuisReqV1,
        RouteUpdateReqV1, RouteV1,
    },
    Message,
};
use sqlx::{Pool, Postgres};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tonic::{Request, Response, Status};

const UPDATE_BATCH_LIMIT: usize = 5_000;

pub struct RouteService {
    auth_cache: AuthCache,
    pool: Pool<Postgres>,
    update_channel: broadcast::Sender<RouteStreamResV1>,
    shutdown: triggered::Listener,
    signing_key: Arc<Keypair>,
}

#[derive(Clone, Debug)]
enum OrgId<'a> {
    Oui(u64),
    RouteId(&'a str),
}

impl RouteService {
    pub fn new(
        settings: &Settings,
        auth_cache: AuthCache,
        pool: Pool<Postgres>,
        shutdown: triggered::Listener,
    ) -> Result<Self> {
        Ok(Self {
            auth_cache,
            pool,
            update_channel: update_channel(),
            shutdown,
            signing_key: Arc::new(settings.signing_keypair()?),
        })
    }

    fn subscribe_to_routes(&self) -> broadcast::Receiver<RouteStreamResV1> {
        self.update_channel.subscribe()
    }

    pub fn clone_update_channel(&self) -> broadcast::Sender<RouteStreamResV1> {
        self.update_channel.clone()
    }

    async fn verify_request_signature<'a, R>(
        &self,
        signer: &PublicKey,
        request: &R,
        id: OrgId<'a>,
    ) -> Result<(), Status>
    where
        R: MsgVerify,
    {
        if self
            .auth_cache
            .verify_signature_with_type(KeyType::Administrator, signer, request)
            .is_ok()
        {
            tracing::debug!(signer = signer.to_string(), "request authorized by admin");
            return Ok(());
        }

        let org_keys = match id {
            OrgId::Oui(oui) => org::get_org_pubkeys(oui, &self.pool).await,
            OrgId::RouteId(route_id) => org::get_org_pubkeys_by_route(route_id, &self.pool).await,
        }
        .map_err(|_| Status::internal("auth verification error"))?;

        if org_keys.as_slice().contains(signer) && request.verify(signer).is_ok() {
            tracing::debug!(
                signer = signer.to_string(),
                "request authorized by delegate"
            );
            return Ok(());
        }

        Err(Status::permission_denied("unauthorized request signature"))
    }

    fn verify_stream_request_signature<R>(
        &self,
        signer: &PublicKey,
        request: &R,
    ) -> Result<(), Status>
    where
        R: MsgVerify,
    {
        if self.auth_cache.verify_signature(signer, request).is_ok() {
            tracing::debug!(signer = signer.to_string(), "request authorized");
            Ok(())
        } else {
            Err(Status::permission_denied("unauthorized request signature"))
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

    async fn update_validator(
        &self,
        route_id: &str,
        check_constraints: bool,
    ) -> Result<DevAddrEuiValidator, DbOrgError> {
        let admin_keys = self.auth_cache.get_keys_by_type(KeyType::Administrator);

        DevAddrEuiValidator::new(route_id, admin_keys, &self.pool, check_constraints).await
    }
}

#[tonic::async_trait]
impl iot_config::Route for RouteService {
    async fn list(&self, request: Request<RouteListReqV1>) -> GrpcResult<RouteListResV1> {
        let request = request.into_inner();

        let signer = self.verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request, OrgId::Oui(request.oui))
            .await?;

        tracing::debug!(org = request.oui, "list routes");

        let proto_routes: Vec<RouteV1> = route::list_routes(request.oui, &self.pool)
            .await
            .map_err(|_| Status::internal("route list failed"))?
            .into_iter()
            .map(|route| route.into())
            .collect();

        let mut resp = RouteListResV1 {
            routes: proto_routes,
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp)?;

        Ok(Response::new(resp))
    }

    async fn get(&self, request: Request<RouteGetReqV1>) -> GrpcResult<RouteResV1> {
        let request = request.into_inner();

        let signer = self.verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request, OrgId::RouteId(&request.id))
            .await?;

        tracing::debug!(route_id = request.id, "get route");

        let route = route::get_route(&request.id, &self.pool)
            .await
            .map_err(|err| {
                tracing::warn!("fetch route failed: {err:?}");
                Status::internal("fetch route failed")
            })?;

        let mut resp = RouteResV1 {
            route: Some(route.into()),
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp)?;

        Ok(Response::new(resp))
    }

    async fn create(&self, request: Request<RouteCreateReqV1>) -> GrpcResult<RouteResV1> {
        let request = request.into_inner();

        let signer = self.verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request, OrgId::Oui(request.oui))
            .await?;

        let route: Route = request
            .route
            .ok_or("missing route")
            .map_err(Status::invalid_argument)?
            .into();
        tracing::debug!(org = request.oui, "route create {route:?}");

        if route.oui != request.oui {
            tracing::warn!(
                route_org = route.oui,
                requestor_org = request.oui,
                "route org does not match requestor",
            );
            return Err(Status::invalid_argument(
                "request oui does not match route oui",
            ));
        }

        let new_route: Route = route::create_route(
            route,
            &self.pool,
            &self.signing_key,
            self.clone_update_channel(),
        )
        .await
        .map_err(|err| {
            tracing::error!("route create failed {err:?}");
            Status::internal("route create failed")
        })?;

        let mut resp = RouteResV1 {
            route: Some(new_route.into()),
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp)?;

        Ok(Response::new(resp))
    }

    async fn update(&self, request: Request<RouteUpdateReqV1>) -> GrpcResult<RouteResV1> {
        let request = request.into_inner();

        let route: Route = request
            .clone()
            .route
            .ok_or("missing route")
            .map_err(Status::invalid_argument)?
            .into();
        tracing::debug!(
            org = route.oui,
            route_id = route.id,
            "route update {route:?}"
        );

        let signer = self.verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request, OrgId::Oui(route.oui))
            .await?;

        let updated_route = route::update_route(
            route,
            &self.pool,
            &self.signing_key,
            self.clone_update_channel(),
        )
        .await
        .map_err(|err| {
            tracing::error!("route update failed {err:?}");
            Status::internal("update route failed")
        })?;

        let mut resp = RouteResV1 {
            route: Some(updated_route.into()),
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp)?;

        Ok(Response::new(resp))
    }

    async fn delete(&self, request: Request<RouteDeleteReqV1>) -> GrpcResult<RouteResV1> {
        let request = request.into_inner();

        let signer = self.verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request, OrgId::RouteId(&request.id))
            .await?;

        tracing::debug!(route_id = request.id, "route delete");

        let route = route::get_route(&request.id, &self.pool)
            .await
            .map_err(|_| Status::internal("fetch route failed"))?;

        route::delete_route(
            &request.id,
            &self.pool,
            &self.signing_key,
            self.clone_update_channel(),
        )
        .await
        .map_err(|err| {
            tracing::error!("route delete failed {err:?}");
            Status::internal("delete route failed")
        })?;

        let mut resp = RouteResV1 {
            route: Some(route.into()),
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp)?;

        Ok(Response::new(resp))
    }

    type streamStream = GrpcStreamResult<RouteStreamResV1>;
    async fn stream(&self, request: Request<RouteStreamReqV1>) -> GrpcResult<Self::streamStream> {
        let request = request.into_inner();

        let signer = self.verify_public_key(&request.signer)?;
        self.verify_stream_request_signature(&signer, &request)?;

        tracing::info!("client subscribed to route stream");
        let pool = self.pool.clone();
        let shutdown_listener = self.shutdown.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(20);
        let signing_key = self.signing_key.clone();

        let mut route_updates = self.subscribe_to_routes();

        tokio::spawn(async move {
            if stream_existing_routes(&pool, &signing_key, tx.clone())
                .and_then(|_| stream_existing_euis(&pool, &signing_key, tx.clone()))
                .and_then(|_| stream_existing_devaddrs(&pool, &signing_key, tx.clone()))
                .await
                .is_err()
            {
                return;
            }

            tracing::info!("existing routes sent; streaming updates as available");
            loop {
                let shutdown = shutdown_listener.clone();

                tokio::select! {
                    _ = shutdown => return,
                    msg = route_updates.recv() => if let Ok(update) = msg {
                        if tx.send(Ok(update)).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });

        Ok(Response::new(GrpcStreamResult::new(rx)))
    }

    type get_euisStream = GrpcStreamResult<EuiPairV1>;
    async fn get_euis(
        &self,
        request: Request<RouteGetEuisReqV1>,
    ) -> GrpcResult<Self::get_euisStream> {
        let request = request.into_inner();

        let signer = self.verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request, OrgId::RouteId(&request.route_id))
            .await?;

        let pool = self.pool.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(20);

        tracing::debug!(route_id = request.route_id, "listing eui pairs");

        tokio::spawn(async move {
            let mut eui_stream = match route::list_euis_for_route(&request.route_id, &pool) {
                Ok(euis) => euis,
                Err(RouteStorageError::UuidParse(err)) => {
                    _ = tx
                        .send(Err(Status::invalid_argument(format!("{}", err))))
                        .await;
                    return;
                }
                Err(_) => {
                    _ = tx
                        .send(Err(Status::internal(format!(
                            "failed retrieving eui pairs for route {}",
                            &request.route_id
                        ))))
                        .await;
                    return;
                }
            };

            while let Some(eui) = eui_stream.next().await {
                let message = match eui {
                    Ok(eui) => Ok(eui.into()),
                    Err(bad_eui) => Err(Status::internal(format!("invalid eui: {:?}", bad_eui))),
                };
                if tx.send(message).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(GrpcStreamResult::new(rx)))
    }

    async fn update_euis(
        &self,
        request: GrpcStreamRequest<RouteUpdateEuisReqV1>,
    ) -> GrpcResult<RouteEuisResV1> {
        let mut request = request.into_inner();

        let mut to_add: Vec<EuiPair> = vec![];
        let mut to_remove: Vec<EuiPair> = vec![];
        let mut pending_updates: usize = 0;

        let mut validator: DevAddrEuiValidator =
            if let Ok(Some(first_update)) = request.message().await {
                if let Some(eui_pair) = &first_update.eui_pair {
                    let mut validator = self
                        .update_validator(&eui_pair.route_id, false)
                        .await
                        .map_err(|_| Status::internal("unable to verify updates"))?;
                    validator.validate_update(&first_update)?;
                    match first_update.action() {
                        ActionV1::Add => to_add.push(eui_pair.into()),
                        ActionV1::Remove => to_remove.push(eui_pair.into()),
                    };
                    pending_updates += 1;
                    validator
                } else {
                    return Err(Status::invalid_argument("no valid route_id for update"));
                }
            } else {
                return Err(Status::invalid_argument("no eui pair provided"));
            };

        while let Ok(Some(update)) = request.message().await {
            validator.validate_update(&update)?;
            match (update.action(), update.eui_pair) {
                (ActionV1::Add, Some(eui_pair)) => to_add.push(eui_pair.into()),
                (ActionV1::Remove, Some(eui_pair)) => to_remove.push(eui_pair.into()),
                _ => return Err(Status::invalid_argument("no eui pair provided")),
            };
            pending_updates += 1;
            if pending_updates >= UPDATE_BATCH_LIMIT {
                tracing::debug!(
                    adding = to_add.len(),
                    removing = to_remove.len(),
                    "updating eui pairs",
                );
                route::update_euis(
                    &to_add,
                    &to_remove,
                    &self.pool,
                    self.signing_key.clone(),
                    self.update_channel.clone(),
                )
                .await
                .map_err(|err| {
                    tracing::error!("eui pair update failed: {err:?}");
                    Status::internal("eui pair update failed")
                })?;
                to_add = vec![];
                to_remove = vec![];
                pending_updates = 0;
            }
        }

        if pending_updates > 0 {
            tracing::debug!(
                adding = to_add.len(),
                removing = to_remove.len(),
                "updating euis",
            );

            route::update_euis(
                &to_add,
                &to_remove,
                &self.pool,
                self.signing_key.clone(),
                self.clone_update_channel(),
            )
            .await
            .map_err(|err| {
                tracing::error!("eui update failed: {err:?}");
                Status::internal("eui update failed")
            })?;
        }
        let mut resp = RouteEuisResV1 {
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp)?;

        Ok(Response::new(resp))
    }

    type get_devaddr_rangesStream = GrpcStreamResult<DevaddrRangeV1>;
    async fn get_devaddr_ranges(
        &self,
        request: Request<RouteGetDevaddrRangesReqV1>,
    ) -> GrpcResult<Self::get_devaddr_rangesStream> {
        let request = request.into_inner();

        let signer = self.verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request, OrgId::RouteId(&request.route_id))
            .await?;

        let (tx, rx) = tokio::sync::mpsc::channel(20);
        let pool = self.pool.clone();

        tracing::debug!(route_id = request.route_id, "listing devaddr ranges");

        tokio::spawn(async move {
            let mut devaddrs = match route::list_devaddr_ranges_for_route(&request.route_id, &pool)
            {
                Ok(devaddrs) => devaddrs,
                Err(RouteStorageError::UuidParse(err)) => {
                    _ = tx
                        .send(Err(Status::invalid_argument(format!("{}", err))))
                        .await;
                    return;
                }
                Err(_) => {
                    _ = tx
                        .send(Err(Status::internal("failed retrieving devaddr ranges")))
                        .await;
                    return;
                }
            };

            while let Some(devaddr) = devaddrs.next().await {
                let message = match devaddr {
                    Ok(devaddr) => Ok(devaddr.into()),
                    Err(bad_devaddr) => Err(Status::internal(format!(
                        "invalid devaddr: {:?}",
                        bad_devaddr
                    ))),
                };
                if tx.send(message).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(GrpcStreamResult::new(rx)))
    }

    async fn update_devaddr_ranges(
        &self,
        request: GrpcStreamRequest<RouteUpdateDevaddrRangesReqV1>,
    ) -> GrpcResult<RouteDevaddrRangesResV1> {
        let mut request = request.into_inner();

        let mut to_add: Vec<DevAddrRange> = vec![];
        let mut to_remove: Vec<DevAddrRange> = vec![];
        let mut pending_updates: usize = 0;

        let mut validator: DevAddrEuiValidator =
            if let Ok(Some(first_update)) = request.message().await {
                if let Some(devaddr) = &first_update.devaddr_range {
                    let mut validator = self
                        .update_validator(&devaddr.route_id, true)
                        .await
                        .map_err(|_| Status::internal("unable to verify updates"))?;
                    validator.validate_update(&first_update)?;
                    match first_update.action() {
                        ActionV1::Add => to_add.push(devaddr.into()),
                        ActionV1::Remove => to_remove.push(devaddr.into()),
                    };
                    pending_updates += 1;
                    validator
                } else {
                    return Err(Status::invalid_argument("no valid route_id for update"));
                }
            } else {
                return Err(Status::invalid_argument("no devaddr range provided"));
            };

        while let Ok(Some(update)) = request.message().await {
            validator.validate_update(&update)?;
            match (update.action(), update.devaddr_range) {
                (ActionV1::Add, Some(devaddr)) => to_add.push(devaddr.into()),
                (ActionV1::Remove, Some(devaddr)) => to_remove.push(devaddr.into()),
                _ => return Err(Status::invalid_argument("no devaddr range provided")),
            };
            pending_updates += 1;
            if pending_updates >= UPDATE_BATCH_LIMIT {
                tracing::debug!(
                    adding = to_add.len(),
                    removing = to_remove.len(),
                    "updating devaddr ranges"
                );
                route::update_devaddr_ranges(
                    &to_add,
                    &to_remove,
                    &self.pool,
                    self.signing_key.clone(),
                    self.update_channel.clone(),
                )
                .await
                .map_err(|err| {
                    tracing::error!("devaddr range update failed: {err:?}");
                    Status::internal("devaddr range update failed")
                })?;
                to_add = vec![];
                to_remove = vec![];
                pending_updates = 0;
            }
        }

        if pending_updates > 0 {
            tracing::debug!(
                adding = to_add.len(),
                removing = to_remove.len(),
                "updating devaddr ranges"
            );

            route::update_devaddr_ranges(
                &to_add,
                &to_remove,
                &self.pool,
                self.signing_key.clone(),
                self.update_channel.clone(),
            )
            .await
            .map_err(|err| {
                tracing::error!("devaddr range update failed: {err:?}");
                Status::internal("devaddr range update failed")
            })?;
        }
        let mut resp = RouteDevaddrRangesResV1 {
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp)?;

        Ok(Response::new(resp))
    }
}

struct DevAddrEuiValidator {
    route_ids: Vec<String>,
    constraints: Option<Vec<DevAddrConstraint>>,
    signing_keys: Vec<PublicKey>,
}

#[derive(thiserror::Error, Debug)]
enum DevAddrEuiValidationError {
    #[error("devaddr range outside of constraint bounds {0}")]
    RangeOutOfBounds(String),
    #[error("no route for update {0}")]
    NoRouteId(String),
    #[error("unauthorized signature {0}")]
    UnauthorizedSignature(String),
    #[error("invalid update {0}")]
    InvalidUpdate(String),
}

impl DevAddrEuiValidator {
    async fn new(
        route_id: &str,
        mut admin_keys: Vec<PublicKey>,
        db: impl sqlx::PgExecutor<'_> + Copy,
        check_constraints: bool,
    ) -> Result<Self, DbOrgError> {
        let constraints = if check_constraints {
            Some(org::get_constraints_by_route(route_id, db).await?)
        } else {
            None
        };

        let mut org_keys = org::get_org_pubkeys_by_route(route_id, db).await?;
        org_keys.append(&mut admin_keys);

        Ok(Self {
            route_ids: org::get_route_ids_by_route(route_id, db).await?,
            constraints,
            signing_keys: org_keys,
        })
    }

    fn validate_update<'a, R>(&'a mut self, request: &'a R) -> Result<(), Status>
    where
        R: MsgVerify + ValidateRouteComponent<'a> + std::fmt::Debug,
    {
        validate_owned_route(request, &self.route_ids)
            .and_then(|update| validate_range_bounds(update, self.constraints.as_ref()))
            .and_then(|update| validate_signature(update, &mut self.signing_keys))
            .map_err(|err| Status::invalid_argument(format!("{err:?}")))?;
        Ok(())
    }
}

trait ValidateRouteComponent<'a> {
    type Error;
    fn route_id(&'a self) -> Result<&'a String, Self::Error>;
    fn range(&self) -> Result<Option<DevAddrRange>, Self::Error>;
}

impl<'a> ValidateRouteComponent<'a> for RouteUpdateDevaddrRangesReqV1 {
    type Error = &'a str;

    fn route_id(&'a self) -> Result<&'a String, Self::Error> {
        if let Some(ref devaddr) = self.devaddr_range {
            Ok(&devaddr.route_id)
        } else {
            Err("missing devaddr range update")
        }
    }
    fn range(&self) -> Result<Option<DevAddrRange>, Self::Error> {
        if let Some(ref devaddr) = self.devaddr_range {
            Ok(Some(devaddr.into()))
        } else {
            Err("missing devaddr range update")
        }
    }
}

impl<'a> ValidateRouteComponent<'a> for RouteUpdateEuisReqV1 {
    type Error = &'a str;

    fn route_id(&'a self) -> Result<&'a String, Self::Error> {
        if let Some(ref eui_pair) = self.eui_pair {
            Ok(&eui_pair.route_id)
        } else {
            Err("missing eui pair update")
        }
    }
    fn range(&self) -> Result<Option<DevAddrRange>, Self::Error> {
        Ok(None)
    }
}

fn validate_owned_route<'a, T>(
    update: &'a T,
    route_ids: &'a [String],
) -> Result<&'a T, DevAddrEuiValidationError>
where
    T: ValidateRouteComponent<'a> + std::fmt::Debug,
{
    let update_id = update
        .route_id()
        .map_err(|_| DevAddrEuiValidationError::InvalidUpdate(format!("{update:?}")))?;
    if !route_ids.contains(update_id) {
        return Err(DevAddrEuiValidationError::NoRouteId(format!("{update:?}")));
    }
    Ok(update)
}

fn validate_range_bounds<'a, T>(
    update: &'a T,
    constraints: Option<&Vec<DevAddrConstraint>>,
) -> Result<&'a T, DevAddrEuiValidationError>
where
    T: ValidateRouteComponent<'a> + std::fmt::Debug,
{
    if let Some(constraints) = constraints {
        match update.range() {
            Ok(Some(range)) => {
                for constraint in constraints {
                    if constraint.contains_range(&range) {
                        return Ok(update);
                    }
                }
                Err(DevAddrEuiValidationError::RangeOutOfBounds(format!(
                    "{update:?}"
                )))
            }
            Ok(None) => Ok(update),
            _ => Err(DevAddrEuiValidationError::InvalidUpdate(format!(
                "{update:?}"
            ))),
        }
    } else {
        Ok(update)
    }
}

fn validate_signature<'a, R>(
    request: &'a R,
    signing_keys: &mut [PublicKey],
) -> Result<&'a R, DevAddrEuiValidationError>
where
    R: MsgVerify + ValidateRouteComponent<'a> + std::fmt::Debug,
{
    for (idx, pubkey) in signing_keys.iter().enumerate() {
        if request.verify(pubkey).is_ok() {
            signing_keys.swap(idx, 0);
            return Ok(request);
        }
    }
    Err(DevAddrEuiValidationError::UnauthorizedSignature(format!(
        "{request:?}"
    )))
}

async fn stream_existing_routes(
    pool: &Pool<Postgres>,
    signing_key: &Keypair,
    tx: mpsc::Sender<Result<RouteStreamResV1, Status>>,
) -> Result<()> {
    let timestamp = Utc::now().encode_timestamp();
    let signer: Vec<u8> = signing_key.public_key().into();
    let tx = &tx;
    route::active_route_stream(pool)
        .then(move |route| {
            let mut route_res = RouteStreamResV1 {
                action: ActionV1::Add.into(),
                data: Some(route_stream_res_v1::Data::Route(route.into())),
                timestamp,
                signer: signer.clone(),
                signature: vec![],
            };
            if let Ok(signature) = signing_key.sign(&route_res.encode_to_vec()) {
                route_res.signature = signature;
                tx.send(Ok(route_res))
            } else {
                tx.send(Err(Status::internal("failed to sign route")))
            }
        })
        .map_err(|err| anyhow!(err))
        .try_fold((), |acc, _| async move { Ok(acc) })
        .await
}

async fn stream_existing_euis(
    pool: &Pool<Postgres>,
    signing_key: &Keypair,
    tx: mpsc::Sender<Result<RouteStreamResV1, Status>>,
) -> Result<()> {
    let timestamp = Utc::now().encode_timestamp();
    let signer: Vec<u8> = signing_key.public_key().into();
    let tx = &tx;
    route::eui_stream(pool)
        .then(move |eui_pair| {
            let mut eui_pair_res = RouteStreamResV1 {
                action: ActionV1::Add.into(),
                data: Some(route_stream_res_v1::Data::EuiPair(eui_pair.into())),
                timestamp,
                signer: signer.clone(),
                signature: vec![],
            };
            if let Ok(signature) = signing_key.sign(&eui_pair_res.encode_to_vec()) {
                eui_pair_res.signature = signature;
                tx.send(Ok(eui_pair_res))
            } else {
                tx.send(Err(Status::internal("failed to sign eui pair")))
            }
        })
        .map_err(|err| anyhow!(err))
        .try_fold((), |acc, _| async move { Ok(acc) })
        .await
}

async fn stream_existing_devaddrs(
    pool: &Pool<Postgres>,
    signing_key: &Keypair,
    tx: mpsc::Sender<Result<RouteStreamResV1, Status>>,
) -> Result<()> {
    let timestamp = Utc::now().encode_timestamp();
    let signer: Vec<u8> = signing_key.public_key().into();
    let tx = &tx;
    route::devaddr_range_stream(pool)
        .then(move |devaddr_range| {
            let mut devaddr_range_res = RouteStreamResV1 {
                action: ActionV1::Add.into(),
                data: Some(route_stream_res_v1::Data::DevaddrRange(
                    devaddr_range.into(),
                )),
                timestamp,
                signer: signer.clone(),
                signature: vec![],
            };
            if let Ok(signature) = signing_key.sign(&devaddr_range_res.encode_to_vec()) {
                devaddr_range_res.signature = signature;
                tx.send(Ok(devaddr_range_res))
            } else {
                tx.send(Err(Status::internal("failed to sign devaddr range")))
            }
        })
        .map_err(|err| anyhow!(err))
        .try_fold((), |acc, _| async move { Ok(acc) })
        .await
}
