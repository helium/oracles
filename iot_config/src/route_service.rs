use crate::{
    admin::{AuthCache, KeyType},
    lora_field::{DevAddrConstraint, DevAddrRange, EuiPair},
    org::{self, get_org_pubkeys, get_org_pubkeys_by_route},
    route::{self, Route, RouteStorageError},
    GrpcResult, GrpcStreamRequest, GrpcStreamResult,
};
use anyhow::Result;
use file_store::traits::MsgVerify;
use futures::stream::StreamExt;
use helium_proto::services::iot_config::{
    self, route_stream_res_v1, ActionV1, DevaddrRangeV1, EuiPairV1, RouteCreateReqV1,
    RouteDeleteReqV1, RouteDevaddrRangesResV1, RouteEuisResV1, RouteGetDevaddrRangesReqV1,
    RouteGetEuisReqV1, RouteGetReqV1, RouteListReqV1, RouteListResV1, RouteStreamReqV1,
    RouteStreamResV1, RouteUpdateDevaddrRangesReqV1, RouteUpdateEuisReqV1, RouteUpdateReqV1,
    RouteV1,
};
use sqlx::{Pool, Postgres};
use tokio::sync::broadcast::{Receiver, Sender};
use tonic::{Request, Response, Status};

pub struct RouteService {
    auth_cache: AuthCache,
    pool: Pool<Postgres>,
    update_channel: Sender<RouteStreamResV1>,
    shutdown: triggered::Listener,
}

#[derive(Clone, Debug)]
enum OrgId<'a> {
    Oui(u64),
    RouteId(&'a str),
}

impl RouteService {
    pub fn new(auth_cache: AuthCache, pool: Pool<Postgres>, shutdown: triggered::Listener) -> Self {
        let (update_tx, _) = tokio::sync::broadcast::channel(128);

        Self {
            auth_cache,
            pool,
            update_channel: update_tx,
            shutdown,
        }
    }

    fn subscribe_to_routes(&self) -> Receiver<RouteStreamResV1> {
        self.update_channel.subscribe()
    }

    pub fn clone_update_channel(&self) -> Sender<RouteStreamResV1> {
        self.update_channel.clone()
    }

    async fn verify_request_signature<'a, R>(
        &self,
        request: &R,
        id: OrgId<'a>,
    ) -> Result<(), Status>
    where
        R: MsgVerify,
    {
        if self
            .auth_cache
            .verify_signature(KeyType::Administrator, request)
            .await
            .is_ok()
        {
            tracing::debug!("request authorized by admin");
            return Ok(());
        }

        let org_keys = match id {
            OrgId::Oui(oui) => get_org_pubkeys(oui, &self.pool).await,
            OrgId::RouteId(route_id) => get_org_pubkeys_by_route(route_id, &self.pool).await,
        }
        .map_err(|_| Status::internal("auth verification error"))?;

        for pubkey in org_keys.iter() {
            if request.verify(pubkey).is_ok() {
                tracing::debug!("request authorized by delegate {pubkey}");
                return Ok(());
            }
        }
        Err(Status::permission_denied("unauthorized request signature"))
    }

    async fn verify_stream_request_signature<R>(&self, request: &R) -> Result<(), Status>
    where
        R: MsgVerify,
    {
        if self
            .auth_cache
            .verify_signature(KeyType::PacketRouter, request)
            .await
            .is_ok()
        {
            tracing::debug!("request authorized for registered packet router");
            Ok(())
        } else if self
            .auth_cache
            .verify_signature(KeyType::Administrator, request)
            .await
            .is_ok()
        {
            tracing::debug!("request authorized by admin");
            Ok(())
        } else {
            Err(Status::permission_denied("unauthorized request signature"))
        }
    }
}

#[tonic::async_trait]
impl iot_config::Route for RouteService {
    async fn list(&self, request: Request<RouteListReqV1>) -> GrpcResult<RouteListResV1> {
        let request = request.into_inner();

        self.verify_request_signature(&request, OrgId::Oui(request.oui))
            .await?;

        let proto_routes: Vec<RouteV1> = route::list_routes(request.oui, &self.pool)
            .await
            .map_err(|_| Status::internal("route list failed"))?
            .into_iter()
            .map(|route| route.into())
            .collect();

        Ok(Response::new(RouteListResV1 {
            routes: proto_routes,
        }))
    }

    async fn get(&self, request: Request<RouteGetReqV1>) -> GrpcResult<RouteV1> {
        let request = request.into_inner();

        self.verify_request_signature(&request, OrgId::RouteId(&request.id))
            .await?;

        let route = route::get_route(&request.id, &self.pool)
            .await
            .map_err(|_| Status::internal("fetch route failed"))?;

        Ok(Response::new(route.into()))
    }

    async fn create(&self, request: Request<RouteCreateReqV1>) -> GrpcResult<RouteV1> {
        let request = request.into_inner();

        self.verify_request_signature(&request, OrgId::Oui(request.oui))
            .await?;

        let route: Route = request
            .route
            .ok_or("missing route")
            .map_err(Status::invalid_argument)?
            .into();

        if route.oui != request.oui {
            return Err(Status::invalid_argument(
                "request oui does not match route oui",
            ));
        }

        let new_route: Route = route::create_route(route, &self.pool, self.clone_update_channel())
            .await
            .map_err(|err| {
                tracing::error!("route create failed {err:?}");
                Status::internal("route create failed")
            })?;

        Ok(Response::new(new_route.into()))
    }

    async fn update(&self, request: Request<RouteUpdateReqV1>) -> GrpcResult<RouteV1> {
        let request = request.into_inner();

        let route: Route = request
            .clone()
            .route
            .ok_or("missing route")
            .map_err(Status::invalid_argument)?
            .into();

        self.verify_request_signature(&request, OrgId::Oui(route.oui))
            .await?;

        let updated_route = route::update_route(route, &self.pool, self.clone_update_channel())
            .await
            .map_err(|_| Status::internal("update route failed"))?;

        Ok(Response::new(updated_route.into()))
    }

    async fn delete(&self, request: Request<RouteDeleteReqV1>) -> GrpcResult<RouteV1> {
        let request = request.into_inner();

        self.verify_request_signature(&request, OrgId::RouteId(&request.id))
            .await?;

        let route = route::get_route(&request.id, &self.pool)
            .await
            .map_err(|_| Status::internal("fetch route failed"))?;

        route::delete_route(&request.id, &self.pool, self.clone_update_channel())
            .await
            .map_err(|_| Status::internal("delete route failed"))?;

        Ok(Response::new(route.into()))
    }

    type streamStream = GrpcStreamResult<RouteStreamResV1>;
    async fn stream(&self, request: Request<RouteStreamReqV1>) -> GrpcResult<Self::streamStream> {
        let request = request.into_inner();

        self.verify_stream_request_signature(&request).await?;

        tracing::info!("client subscribed to route stream");
        let pool = self.pool.clone();
        let shutdown_listener = self.shutdown.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(20);

        let mut route_updates = self.subscribe_to_routes();

        tokio::spawn(async move {
            let mut active_routes = route::active_route_stream(&pool);

            while let Some(active_route) = active_routes.next().await {
                if (tx.send(Ok(RouteStreamResV1 {
                    action: ActionV1::Add.into(),
                    data: Some(route_stream_res_v1::Data::Route(active_route.into())),
                })))
                .await
                .is_err()
                {
                    break;
                }
            }

            let mut eui_pairs = route::eui_stream(&pool);

            while let Some(eui_pair) = eui_pairs.next().await {
                if (tx.send(Ok(RouteStreamResV1 {
                    action: ActionV1::Add.into(),
                    data: Some(route_stream_res_v1::Data::EuiPair(eui_pair.into())),
                })))
                .await
                .is_err()
                {
                    break;
                }
            }

            let mut devaddr_ranges = route::devaddr_range_stream(&pool);

            while let Some(devaddr_range) = devaddr_ranges.next().await {
                if (tx.send(Ok(RouteStreamResV1 {
                    action: ActionV1::Add.into(),
                    data: Some(route_stream_res_v1::Data::DevaddrRange(
                        devaddr_range.into(),
                    )),
                })))
                .await
                .is_err()
                {
                    break;
                }
            }

            while let Ok(update) = route_updates.recv().await {
                if shutdown_listener.is_triggered() || (tx.send(Ok(update)).await).is_err() {
                    break;
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

        self.verify_request_signature(&request, OrgId::RouteId(&request.route_id))
            .await?;

        let pool = self.pool.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(20);

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

        if let Ok(Some(first_update)) = request.message().await {
            if let Some(eui_pair) = &first_update.eui_pair {
                self.verify_request_signature(&first_update, OrgId::RouteId(&eui_pair.route_id))
                    .await?;
                match first_update.action() {
                    ActionV1::Add => to_add.push(eui_pair.into()),
                    ActionV1::Remove => to_remove.push(eui_pair.into()),
                }
            }
        } else {
            return Err(Status::invalid_argument("no eui pair provided"));
        }

        while let Ok(Some(update)) = request.message().await {
            match (update.action(), update.eui_pair) {
                (ActionV1::Add, Some(eui_pair)) => to_add.push(eui_pair.into()),
                (ActionV1::Remove, Some(eui_pair)) => to_remove.push(eui_pair.into()),
                _ => return Err(Status::invalid_argument("no eui pair provided")),
            }
        }
        tracing::debug!(
            adding = to_add.len(),
            removing = to_remove.len(),
            "updating euis"
        );

        route::update_euis(&to_add, &to_remove, &self.pool, self.update_channel.clone())
            .await
            .map_err(|err| {
                tracing::error!("eui update failed: {err:?}");
                Status::internal("eui update failed")
            })?;

        Ok(Response::new(RouteEuisResV1 {}))
    }

    type get_devaddr_rangesStream = GrpcStreamResult<DevaddrRangeV1>;
    async fn get_devaddr_ranges(
        &self,
        request: Request<RouteGetDevaddrRangesReqV1>,
    ) -> GrpcResult<Self::get_devaddr_rangesStream> {
        let request = request.into_inner();

        self.verify_request_signature(&request, OrgId::RouteId(&request.route_id))
            .await?;

        let (tx, rx) = tokio::sync::mpsc::channel(20);
        let pool = self.pool.clone();

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

        let mut constraint_bounds = vec![];
        let mut to_add: Vec<DevAddrRange> = vec![];
        let mut to_remove: Vec<DevAddrRange> = vec![];

        if let Ok(Some(first_update)) = request.message().await {
            if let Some(devaddr) = &first_update.devaddr_range {
                self.verify_request_signature(&first_update, OrgId::RouteId(&devaddr.route_id))
                    .await?;
                let mut constraints = org::get_constraints_by_route(&devaddr.route_id, &self.pool)
                    .await
                    .map_err(|_| Status::internal("no devaddr constraints for org"))?;
                match first_update.action() {
                    ActionV1::Add => {
                        let add_range = devaddr.into();
                        verify_range_in_bounds(&constraints, &add_range)
                            .map(|_| to_add.push(add_range))?
                    }
                    ActionV1::Remove => to_remove.push(devaddr.into()),
                };
                constraint_bounds.append(&mut constraints)
            }
        } else {
            return Err(Status::invalid_argument("no devaddr range provided"));
        };

        while let Ok(Some(update)) = request.message().await {
            match (update.action(), update.devaddr_range) {
                (ActionV1::Add, Some(devaddr)) => {
                    let add_range = devaddr.into();
                    verify_range_in_bounds(&constraint_bounds, &add_range)
                        .map(|_| to_add.push(add_range))?
                }
                (ActionV1::Remove, Some(devaddr)) => to_remove.push(devaddr.into()),
                _ => return Err(Status::invalid_argument("no devaddr range provided")),
            }
        }
        tracing::debug!(
            adding = to_add.len(),
            removing = to_remove.len(),
            "updating devaddr ranges"
        );

        route::update_devaddr_ranges(&to_add, &to_remove, &self.pool, self.update_channel.clone())
            .await
            .map_err(|err| {
                tracing::error!("devaddr range update failed: {err:?}");
                Status::internal("devaddr range update failed")
            })?;
        Ok(Response::new(RouteDevaddrRangesResV1 {}))
    }
}

fn verify_range_in_bounds(
    constraints: &[DevAddrConstraint],
    range: &DevAddrRange,
) -> Result<(), Status> {
    for constraint in constraints {
        if constraint.contains(range) {
            return Ok(());
        }
    }
    Err(Status::invalid_argument(
        "devaddr range outside of org constraints",
    ))
}
