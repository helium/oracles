use crate::{
    admin::{AuthCache, KeyType},
    lora_field::{DevAddrConstraint, DevAddrRange, EuiPair, Skf},
    org::{self, OrgStoreError},
    route::{self, Route, RouteStorageError},
    telemetry, update_channel, verify_public_key, GrpcResult, GrpcStreamRequest, GrpcStreamResult,
};
use anyhow::{anyhow, Result};
use chrono::{DateTime, TimeZone, Utc};
use file_store::traits::{MsgVerify, TimestampEncode};
use futures::{
    future::TryFutureExt,
    stream::{StreamExt, TryStreamExt},
};
use helium_crypto::{Keypair, PublicKey, Sign};
use helium_proto::{
    services::iot_config::{
        self, route_skf_update_req_v1, route_stream_res_v1, ActionV1, DevaddrRangeV1, EuiPairV1,
        RouteCreateReqV1, RouteDeleteReqV1, RouteDevaddrRangesResV1, RouteEuisResV1,
        RouteGetDevaddrRangesReqV1, RouteGetEuisReqV1, RouteGetReqV1, RouteListReqV1,
        RouteListResV1, RouteResV1, RouteSkfGetReqV1, RouteSkfListReqV1, RouteSkfUpdateReqV1,
        RouteSkfUpdateResV1, RouteStreamReqV1, RouteStreamResV1, RouteUpdateDevaddrRangesReqV1,
        RouteUpdateEuisReqV1, RouteUpdateReqV1, RouteV1, SkfV1,
    },
    Message,
};
use sqlx::{Pool, Postgres};
use std::{pin::Pin, sync::Arc};
use tokio::sync::{broadcast, mpsc};
use tonic::{Request, Response, Status};

const UPDATE_BATCH_LIMIT: usize = 5_000;
const SKF_UPDATE_LIMIT: usize = 100;

pub struct RouteService {
    auth_cache: AuthCache,
    pool: Pool<Postgres>,
    update_channel: broadcast::Sender<RouteStreamResV1>,
    signing_key: Arc<Keypair>,
}

#[derive(Clone, Debug)]
enum OrgId<'a> {
    Oui(u64),
    RouteId(&'a str),
}

impl RouteService {
    pub fn new(signing_key: Arc<Keypair>, auth_cache: AuthCache, pool: Pool<Postgres>) -> Self {
        Self {
            auth_cache,
            pool,
            update_channel: update_channel(),
            signing_key,
        }
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
        .map_err(|err| match err {
            OrgStoreError::RouteIdParse(id_err) => Status::invalid_argument(id_err.to_string()),
            _ => Status::internal("auth verification error"),
        })?;

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

    async fn verify_request_signature_or_stream<'a, R>(
        &self,
        signer: &PublicKey,
        request: &R,
        id: OrgId<'a>,
    ) -> Result<(), Status>
    where
        R: MsgVerify,
    {
        if self
            .verify_stream_request_signature(signer, request)
            .is_ok()
        {
            return Ok(());
        }
        self.verify_request_signature(signer, request, id).await
    }

    fn sign_response(&self, response: &[u8]) -> Result<Vec<u8>, Status> {
        self.signing_key
            .sign(response)
            .map_err(|_| Status::internal("response signing error"))
    }

    async fn update_validator(
        &self,
        route_id: &str,
        check_constraints: bool,
    ) -> Result<DevAddrEuiValidator, OrgStoreError> {
        let admin_keys = self.auth_cache.get_keys_by_type(KeyType::Administrator);

        DevAddrEuiValidator::new(route_id, admin_keys, &self.pool, check_constraints).await
    }

    async fn validate_skf_devaddrs<'a>(
        &self,
        route_id: &'a str,
        updates: &[route_skf_update_req_v1::RouteSkfUpdateV1],
    ) -> Result<(), Status> {
        let ranges: Vec<DevAddrRange> = route::list_devaddr_ranges_for_route(route_id, &self.pool)
            .map_err(|err| match err {
                RouteStorageError::UuidParse(_) => {
                    Status::invalid_argument(format!("unable to parse route_id: {route_id}"))
                }
                _ => Status::internal("error retrieving devaddrs for route"),
            })?
            .filter_map(|range| async move { range.ok() })
            .collect()
            .await;

        for update in updates {
            let devaddr = update.devaddr.into();
            if !ranges.iter().any(|range| range.contains_addr(devaddr)) {
                let ranges = ranges
                    .iter()
                    .map(|r| format!("{} -- {}", r.start_addr, r.end_addr))
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(Status::invalid_argument(format!(
                    "devaddr {devaddr} not within registered ranges for route {route_id} :: {ranges}"
                )));
            }
        }

        Ok(())
    }
}

#[tonic::async_trait]
impl iot_config::Route for RouteService {
    async fn list(&self, request: Request<RouteListReqV1>) -> GrpcResult<RouteListResV1> {
        let request = request.into_inner();
        telemetry::count_request("route", "list");

        let signer = verify_public_key(&request.signer)?;
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
            timestamp: Utc::now().encode_timestamp_millis(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp.encode_to_vec())?;

        Ok(Response::new(resp))
    }

    async fn get(&self, request: Request<RouteGetReqV1>) -> GrpcResult<RouteResV1> {
        let request = request.into_inner();
        telemetry::count_request("route", "get");

        let signer = verify_public_key(&request.signer)?;
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
            timestamp: Utc::now().encode_timestamp_millis(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp.encode_to_vec())?;

        Ok(Response::new(resp))
    }

    async fn create(&self, request: Request<RouteCreateReqV1>) -> GrpcResult<RouteResV1> {
        let request = request.into_inner();
        telemetry::count_request("route", "create");

        let signer = verify_public_key(&request.signer)?;
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
            timestamp: Utc::now().encode_timestamp_millis(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp.encode_to_vec())?;

        Ok(Response::new(resp))
    }

    async fn update(&self, request: Request<RouteUpdateReqV1>) -> GrpcResult<RouteResV1> {
        let request = request.into_inner();
        telemetry::count_request("route", "update");

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

        let signer = verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request, OrgId::RouteId(&route.id))
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
            timestamp: Utc::now().encode_timestamp_millis(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp.encode_to_vec())?;

        Ok(Response::new(resp))
    }

    async fn delete(&self, request: Request<RouteDeleteReqV1>) -> GrpcResult<RouteResV1> {
        let request = request.into_inner();
        telemetry::count_request("route", "delete");

        let signer = verify_public_key(&request.signer)?;
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
            timestamp: Utc::now().encode_timestamp_millis(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp.encode_to_vec())?;

        Ok(Response::new(resp))
    }

    type streamStream = GrpcStreamResult<RouteStreamResV1>;
    async fn stream(&self, request: Request<RouteStreamReqV1>) -> GrpcResult<Self::streamStream> {
        let request = request.into_inner();
        telemetry::count_request("route", "stream");

        let signer = verify_public_key(&request.signer)?;
        self.verify_stream_request_signature(&signer, &request)?;

        let since = Utc
            .timestamp_millis_opt(request.since as i64)
            .single()
            .ok_or_else(|| Status::invalid_argument("unable to parse since timestamp"))?;

        tracing::info!("client subscribed to route stream");
        let pool = self.pool.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(20);
        let signing_key = self.signing_key.clone();

        let mut route_updates = self.subscribe_to_routes();

        tokio::spawn(async move {
            let broadcast = stream_existing_routes(&pool, since, &signing_key, tx.clone())
                .and_then(|_| stream_existing_euis(&pool, since, &signing_key, tx.clone()))
                .and_then(|_| stream_existing_devaddrs(&pool, since, &signing_key, tx.clone()))
                .and_then(|_| stream_existing_skfs(&pool, since, &signing_key, tx.clone()))
                .await;
            if let Err(error) = broadcast {
                tracing::error!(
                    ?error,
                    "Error occurred streaming current routing configuration"
                );
                return;
            }

            tracing::info!("existing routes sent; streaming updates as available");
            telemetry::route_stream_subscribe();
            while let Ok(update) = route_updates.recv().await {
                if tx.send(Ok(update)).await.is_err() {
                    telemetry::route_stream_unsubscribe();
                    return;
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
        telemetry::count_request("route", "get-euis");

        let signer = verify_public_key(&request.signer)?;
        self.verify_request_signature_or_stream(
            &signer,
            &request,
            OrgId::RouteId(&request.route_id),
        )
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
        let request = request.into_inner();
        telemetry::count_request("route", "update-euis");

        let mut incoming_stream = request.peekable();
        let mut validator: DevAddrEuiValidator = Pin::new(&mut incoming_stream)
            .peek()
            .await
            .map(|first_update| async move {
                match first_update {
                    Ok(ref update) => match update.eui_pair {
                        Some(ref eui_pair) => self
                            .update_validator(&eui_pair.route_id, false)
                            .await
                            .map_err(|err| {
                                Status::internal(format!("unable to verify updates: {err:?}"))
                            }),
                        None => Err(Status::invalid_argument("no eui pairs provided")),
                    },
                    Err(_) => Err(Status::invalid_argument("no eui pairs provided")),
                }
            })
            .ok_or_else(|| Status::invalid_argument("no eui pairs provided"))?
            .await?;

        incoming_stream
            .map_ok(|update| match validator.validate_update(&update) {
                Ok(()) => Ok(update),
                Err(reason) => Err(Status::invalid_argument(format!(
                    "invalid update request: {reason:?}"
                ))),
            })
            .try_chunks(UPDATE_BATCH_LIMIT)
            .map_err(|err| Status::internal(format!("eui pair updates failed to batch: {err:?}")))
            .and_then(|batch| async move {
                batch
                    .into_iter()
                    .collect::<Result<Vec<RouteUpdateEuisReqV1>, Status>>()
            })
            .and_then(|batch| async move {
                batch
                    .into_iter()
                    .map(
                        |update: RouteUpdateEuisReqV1| match (update.action(), update.eui_pair) {
                            (ActionV1::Add, Some(eui_pair)) => Ok((ActionV1::Add, eui_pair)),
                            (ActionV1::Remove, Some(eui_pair)) => Ok((ActionV1::Remove, eui_pair)),
                            _ => Err(Status::invalid_argument("invalid eui pair update request")),
                        },
                    )
                    .collect::<Result<Vec<(ActionV1, EuiPairV1)>, Status>>()
            })
            .try_for_each(|batch: Vec<(ActionV1, EuiPairV1)>| async move {
                let (to_add, to_remove): (Vec<(ActionV1, EuiPairV1)>, Vec<(ActionV1, EuiPairV1)>) =
                    batch
                        .into_iter()
                        .partition(|(action, _update)| action == &ActionV1::Add);
                telemetry::count_eui_updates(to_add.len(), to_remove.len());
                tracing::debug!(
                    adding = to_add.len(),
                    removing = to_remove.len(),
                    "updating eui pairs"
                );
                let adds_update: Vec<EuiPair> =
                    to_add.into_iter().map(|(_, add)| add.into()).collect();
                let removes_update: Vec<EuiPair> = to_remove
                    .into_iter()
                    .map(|(_, remove)| remove.into())
                    .collect();
                route::update_euis(
                    &adds_update,
                    &removes_update,
                    &self.pool,
                    self.signing_key.clone(),
                    self.clone_update_channel(),
                )
                .await
                .map_err(|err| {
                    tracing::error!("eui pair update failed: {err:?}");
                    Status::internal(format!("eui pair update failed: {err:?}"))
                })
            })
            .await?;

        let mut resp = RouteEuisResV1 {
            timestamp: Utc::now().encode_timestamp_millis(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp.encode_to_vec())?;

        Ok(Response::new(resp))
    }

    type get_devaddr_rangesStream = GrpcStreamResult<DevaddrRangeV1>;
    async fn get_devaddr_ranges(
        &self,
        request: Request<RouteGetDevaddrRangesReqV1>,
    ) -> GrpcResult<Self::get_devaddr_rangesStream> {
        let request = request.into_inner();
        telemetry::count_request("route", "get-devaddr-ranges");

        let signer = verify_public_key(&request.signer)?;
        self.verify_request_signature_or_stream(
            &signer,
            &request,
            OrgId::RouteId(&request.route_id),
        )
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
        let request = request.into_inner();
        telemetry::count_request("route", "update-devaddr-ranges");

        let mut incoming_stream = request.peekable();
        let mut validator: DevAddrEuiValidator = Pin::new(&mut incoming_stream)
            .peek()
            .await
            .map(|first_update| async move {
                match first_update {
                    Ok(ref update) => match update.devaddr_range {
                        Some(ref devaddr_range) => self
                            .update_validator(&devaddr_range.route_id, true)
                            .await
                            .map_err(|err| {
                                Status::internal(format!("unable to verify update {err:?}"))
                            }),
                        None => Err(Status::invalid_argument("no devaddr range provided")),
                    },
                    Err(_) => Err(Status::invalid_argument("no devaddr range provided")),
                }
            })
            .ok_or_else(|| Status::invalid_argument("no devaddr range provided"))?
            .await?;

        incoming_stream
            .map_ok(|update| match validator.validate_update(&update) {
                Ok(()) => Ok(update),
                Err(reason) => Err(Status::invalid_argument(format!(
                    "invalid update request: {reason:?}"
                ))),
            })
            .try_chunks(UPDATE_BATCH_LIMIT)
            .map_err(|err| {
                Status::internal(format!("devaddr range update failed to batch: {err:?}"))
            })
            .and_then(|batch| async move {
                batch
                    .into_iter()
                    .collect::<Result<Vec<RouteUpdateDevaddrRangesReqV1>, Status>>()
            })
            .and_then(|batch| async move {
                batch
                    .into_iter()
                    .map(|update: RouteUpdateDevaddrRangesReqV1| {
                        match (update.action(), update.devaddr_range) {
                            (ActionV1::Add, Some(range)) => Ok((ActionV1::Add, range)),
                            (ActionV1::Remove, Some(range)) => Ok((ActionV1::Remove, range)),
                            _ => Err(Status::invalid_argument(
                                "invalid devaddr range update request",
                            )),
                        }
                    })
                    .collect::<Result<Vec<(ActionV1, DevaddrRangeV1)>, Status>>()
            })
            .try_for_each(|batch: Vec<(ActionV1, DevaddrRangeV1)>| async move {
                let (to_add, to_remove): (
                    Vec<(ActionV1, DevaddrRangeV1)>,
                    Vec<(ActionV1, DevaddrRangeV1)>,
                ) = batch
                    .into_iter()
                    .partition(|(action, _update)| action == &ActionV1::Add);
                telemetry::count_devaddr_updates(to_add.len(), to_remove.len());
                tracing::debug!(
                    adding = to_add.len(),
                    removing = to_remove.len(),
                    "updating devaddr ranges"
                );
                let adds_update: Vec<DevAddrRange> =
                    to_add.into_iter().map(|(_, add)| add.into()).collect();
                let removes_update: Vec<DevAddrRange> = to_remove
                    .into_iter()
                    .map(|(_, remove)| remove.into())
                    .collect();
                route::update_devaddr_ranges(
                    &adds_update,
                    &removes_update,
                    &self.pool,
                    self.signing_key.clone(),
                    self.clone_update_channel(),
                )
                .await
                .map_err(|err| {
                    tracing::error!("devaddr range update failed: {err:?}");
                    Status::internal("devaddr range update failed")
                })
            })
            .await?;

        let mut resp = RouteDevaddrRangesResV1 {
            timestamp: Utc::now().encode_timestamp_millis(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp.encode_to_vec())?;

        Ok(Response::new(resp))
    }

    type list_skfsStream = GrpcStreamResult<SkfV1>;
    async fn list_skfs(
        &self,
        request: Request<RouteSkfListReqV1>,
    ) -> GrpcResult<Self::list_skfsStream> {
        let request = request.into_inner();
        telemetry::count_request("route", "list-skfs");

        let signer = verify_public_key(&request.signer)?;
        self.verify_request_signature_or_stream(
            &signer,
            &request,
            OrgId::RouteId(&request.route_id),
        )
        .await?;

        let pool = self.pool.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(20);

        tracing::debug!(
            route_id = request.route_id,
            "listing session key filters for route"
        );

        tokio::spawn(async move {
            let mut skf_stream = match route::list_skfs_for_route(&request.route_id, &pool) {
                Ok(skfs) => skfs,
                Err(RouteStorageError::UuidParse(err)) => {
                    _ = tx
                        .send(Err(Status::invalid_argument(format!("{}", err))))
                        .await;
                    return;
                }
                Err(_) => {
                    _ = tx
                        .send(Err(Status::internal(format!(
                            "failed retrieving skfs for route {}",
                            &request.route_id
                        ))))
                        .await;
                    return;
                }
            };

            while let Some(skf) = skf_stream.next().await {
                let message = match skf {
                    Ok(skf) => Ok(skf.into()),
                    Err(bad_skf) => Err(Status::internal(format!("invalid skf: {:?}", bad_skf))),
                };
                if tx.send(message).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(GrpcStreamResult::new(rx)))
    }

    type get_skfsStream = GrpcStreamResult<SkfV1>;
    async fn get_skfs(
        &self,
        request: Request<RouteSkfGetReqV1>,
    ) -> GrpcResult<Self::get_skfsStream> {
        let request = request.into_inner();
        telemetry::count_request("route", "get-skfs");

        let signer = verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request, OrgId::RouteId(&request.route_id))
            .await?;

        let pool = self.pool.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(20);

        tracing::debug!(
            route_id = request.route_id,
            "listing session key filters for route and devaddr"
        );

        tokio::spawn(async move {
            let mut skf_stream = match route::list_skfs_for_route_and_devaddr(
                &request.route_id,
                request.devaddr.into(),
                &pool,
            ) {
                Ok(skfs) => skfs,
                Err(RouteStorageError::UuidParse(err)) => {
                    _ = tx
                        .send(Err(Status::invalid_argument(format!("{}", err))))
                        .await;
                    return;
                }
                Err(_) => {
                    _ = tx
                        .send(Err(Status::internal(format!(
                            "failed retrieving skfs for route {} and devaddr {}",
                            &request.route_id, &request.devaddr
                        ))))
                        .await;
                    return;
                }
            };

            while let Some(skf) = skf_stream.next().await {
                let message = match skf {
                    Ok(skf) => Ok(skf.into()),
                    Err(bad_skf) => Err(Status::internal(format!("invalid skf: {:?}", bad_skf))),
                };
                if tx.send(message).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(GrpcStreamResult::new(rx)))
    }

    async fn update_skfs(
        &self,
        request: Request<RouteSkfUpdateReqV1>,
    ) -> GrpcResult<RouteSkfUpdateResV1> {
        let request = request.into_inner();
        telemetry::count_request("route", "update-skfs");

        if request.updates.len() > SKF_UPDATE_LIMIT {
            return Err(Status::invalid_argument(
                "exceeds 100 skf update limit per request",
            ));
        };

        let signer = verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request, OrgId::RouteId(&request.route_id))
            .await?;

        self.validate_skf_devaddrs(&request.route_id, &request.updates)
            .await?;

        let (to_add, to_remove): (Vec<(ActionV1, Skf)>, Vec<(ActionV1, Skf)>) = request
            .updates
            .into_iter()
            .map(|update: route_skf_update_req_v1::RouteSkfUpdateV1| {
                (
                    update.action(),
                    Skf::new(
                        request.route_id.clone(),
                        update.devaddr.into(),
                        update.session_key,
                        update.max_copies,
                    ),
                )
            })
            .partition(|(action, _update)| action == &ActionV1::Add);
        telemetry::count_skf_updates(to_add.len(), to_remove.len());
        tracing::debug!(
            adding = to_add.len(),
            removing = to_remove.len(),
            "updating session key filters"
        );
        let adds_update: Vec<Skf> = to_add.into_iter().map(|(_, add)| add).collect();
        let removes_update: Vec<Skf> = to_remove.into_iter().map(|(_, remove)| remove).collect();
        route::update_skfs(
            &adds_update,
            &removes_update,
            &self.pool,
            self.signing_key.clone(),
            self.clone_update_channel(),
        )
        .await
        .map_err(|err| {
            tracing::error!("session key update failed: {err:?}");
            Status::internal(format!("session key update failed {err:?}"))
        })?;

        let mut resp = RouteSkfUpdateResV1 {
            timestamp: Utc::now().encode_timestamp_millis(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp.encode_to_vec())?;
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
    DevAddrOutOfBounds(String),
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
    ) -> Result<Self, OrgStoreError> {
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
                Err(DevAddrEuiValidationError::DevAddrOutOfBounds(format!(
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
    since: DateTime<Utc>,
    signing_key: &Keypair,
    tx: mpsc::Sender<Result<RouteStreamResV1, Status>>,
) -> Result<()> {
    let timestamp = Utc::now().encode_timestamp_millis();
    let signer: Vec<u8> = signing_key.public_key().into();
    let tx = &tx;
    route::route_stream(pool, since)
        .then(move |(route, deleted)| {
            let mut route_res = RouteStreamResV1 {
                action: if deleted {
                    ActionV1::Remove
                } else {
                    ActionV1::Add
                }
                .into(),
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
    since: DateTime<Utc>,
    signing_key: &Keypair,
    tx: mpsc::Sender<Result<RouteStreamResV1, Status>>,
) -> Result<()> {
    let timestamp = Utc::now().encode_timestamp_millis();
    let signer: Vec<u8> = signing_key.public_key().into();
    let tx = &tx;
    route::eui_stream(pool, since)
        .then(move |(eui_pair, deleted)| {
            let mut eui_pair_res = RouteStreamResV1 {
                action: if deleted {
                    ActionV1::Remove
                } else {
                    ActionV1::Add
                }
                .into(),
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
    since: DateTime<Utc>,
    signing_key: &Keypair,
    tx: mpsc::Sender<Result<RouteStreamResV1, Status>>,
) -> Result<()> {
    let timestamp = Utc::now().encode_timestamp_millis();
    let signer: Vec<u8> = signing_key.public_key().into();
    let tx = &tx;
    route::devaddr_range_stream(pool, since)
        .then(move |(devaddr_range, deleted)| {
            let mut devaddr_range_res = RouteStreamResV1 {
                action: if deleted {
                    ActionV1::Remove
                } else {
                    ActionV1::Add
                }
                .into(),
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

async fn stream_existing_skfs(
    pool: &Pool<Postgres>,
    since: DateTime<Utc>,
    signing_key: &Keypair,
    tx: mpsc::Sender<Result<RouteStreamResV1, Status>>,
) -> Result<()> {
    let timestamp = Utc::now().encode_timestamp_millis();
    let signer: Vec<u8> = signing_key.public_key().into();
    route::skf_stream(pool, since)
        .then(|(skf, deleted)| {
            let mut skf_res = RouteStreamResV1 {
                action: if deleted {
                    ActionV1::Remove
                } else {
                    ActionV1::Add
                }
                .into(),
                data: Some(route_stream_res_v1::Data::Skf(skf.into())),
                timestamp,
                signer: signer.clone(),
                signature: vec![],
            };
            if let Ok(signature) = signing_key.sign(&skf_res.encode_to_vec()) {
                skf_res.signature = signature;
                tx.send(Ok(skf_res))
            } else {
                tx.send(Err(Status::internal("failed to sign session key filter")))
            }
        })
        .map_err(|err| anyhow!(err))
        .try_fold((), |acc, _| async move { Ok(acc) })
        .await
}
