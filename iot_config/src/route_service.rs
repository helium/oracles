use crate::{
    lora_field::{DevAddrRange, EuiPair},
    org::{get_org_pubkeys, get_org_pubkeys_by_route, OrgStatus},
    route::{self, Route},
    GrpcResult, GrpcStreamRequest, GrpcStreamResult, Settings,
};
use anyhow::Result;
use file_store::traits::MsgVerify;
use futures::stream::StreamExt;
use helium_crypto::PublicKey;
use helium_proto::services::iot_config::{
    self, route_stream_res_v1, ActionV1, DevaddrRangeV1, EuiPairV1, RouteCreateReqV1,
    RouteDeleteDevaddrRangesReqV1, RouteDeleteEuisReqV1, RouteDeleteReqV1, RouteDevaddrRangesResV1,
    RouteEuisResV1, RouteGetDevaddrRangesReqV1, RouteGetEuisReqV1, RouteGetReqV1, RouteListReqV1,
    RouteListResV1, RouteStreamReqV1, RouteStreamResV1, RouteUpdateDevaddrRangesReqV1,
    RouteUpdateEuisReqV1, RouteUpdateReqV1, RouteV1,
};
use sqlx::{Pool, Postgres};
use tokio::{
    pin,
    sync::broadcast::{Receiver, Sender},
};
use tonic::{Request, Response, Status};

pub struct RouteService {
    admin_pubkey: PublicKey,
    pool: Pool<Postgres>,
    update_channel: Sender<RouteStreamResV1>,
}

impl RouteService {
    pub async fn new(settings: &Settings) -> Result<Self> {
        let (update_tx, _) = tokio::sync::broadcast::channel(128);

        Ok(Self {
            admin_pubkey: settings.admin_pubkey()?,
            pool: settings.database.connect(10).await?,
            update_channel: update_tx,
        })
    }

    fn subscribe_to_routes(&self) -> Receiver<RouteStreamResV1> {
        self.update_channel.subscribe()
    }

    pub fn clone_update_channel(&self) -> Sender<RouteStreamResV1> {
        self.update_channel.clone()
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

    fn verify_authorized_signature<R>(
        &self,
        request: &R,
        signatures: Vec<PublicKey>,
    ) -> Result<(), Status>
    where
        R: MsgVerify,
    {
        if request.verify(&self.admin_pubkey).is_ok() {
            tracing::debug!("request authorized by admin");
            return Ok(());
        }

        for pubkey in signatures.iter() {
            if request.verify(pubkey).is_ok() {
                tracing::debug!("request authorized by org");
                return Ok(());
            }
        }
        Err(Status::permission_denied("unauthorized request signature"))
    }
}

#[tonic::async_trait]
impl iot_config::Route for RouteService {
    async fn list(&self, request: Request<RouteListReqV1>) -> GrpcResult<RouteListResV1> {
        let request = request.into_inner();

        let org_keys = get_org_pubkeys(request.oui, &self.pool)
            .await
            .map_err(|_| Status::internal("authorization error"))?;
        self.verify_authorized_signature(&request, org_keys)?;

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

        let org_keys = get_org_pubkeys_by_route(&request.id, &self.pool)
            .await
            .map_err(|_| Status::internal("authorization error"))?;
        self.verify_authorized_signature(&request, org_keys)?;

        let route = route::get_route(&request.id, &self.pool)
            .await
            .map_err(|_| Status::internal("fetch route failed"))?;

        Ok(Response::new(route.into()))
    }

    async fn create(&self, request: Request<RouteCreateReqV1>) -> GrpcResult<RouteV1> {
        let request = request.into_inner();

        let org_keys = get_org_pubkeys(request.oui, &self.pool)
            .await
            .map_err(|_| Status::internal("authorization error"))?;
        self.verify_authorized_signature(&request, org_keys)?;

        let route: Route = request
            .route
            .ok_or("missing route")
            .map_err(Status::invalid_argument)?
            .into();

        let new_route: Route =
            route::create_route(route.clone(), &self.pool, self.clone_update_channel())
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

        let org_keys = get_org_pubkeys(route.oui, &self.pool)
            .await
            .map_err(|_| Status::internal("authorization error"))?;
        self.verify_authorized_signature(&request, org_keys)?;

        let updated_route = route::update_route(route, &self.pool, self.clone_update_channel())
            .await
            .map_err(|_| Status::internal("update route failed"))?;

        Ok(Response::new(updated_route.into()))
    }

    async fn delete(&self, request: Request<RouteDeleteReqV1>) -> GrpcResult<RouteV1> {
        let request = request.into_inner();

        let org_keys = get_org_pubkeys_by_route(&request.id, &self.pool)
            .await
            .map_err(|_| Status::internal("authorization error"))?;
        self.verify_authorized_signature(&request, org_keys)?;

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
        _ = self.verify_admin_signature(request)?;

        tracing::info!("client subscribed to route stream");
        let pool = self.pool.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(20);

        let mut route_updates = self.subscribe_to_routes();

        tokio::spawn(async move {
            let active_routes = route::route_stream_by_status(OrgStatus::Enabled, &pool).await;

            pin!(active_routes);

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

            while let Ok(update) = route_updates.recv().await {
                if (tx.send(Ok(update)).await).is_err() {
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

        let euis = route::list_euis_for_route(&request.route_id, &self.pool)
            .await
            .map_err(|_| Status::internal("get euis failed"))?;

        let (tx, rx) = tokio::sync::mpsc::channel(20);
        tokio::spawn(async move {
            for eui in euis {
                if tx.send(Ok(eui.into())).await.is_err() {
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

    async fn delete_euis(
        &self,
        request: Request<RouteDeleteEuisReqV1>,
    ) -> GrpcResult<RouteEuisResV1> {
        let request = request.into_inner();
        route::delete_euis(&request.route_id, &self.pool, self.update_channel.clone())
            .await
            .map_err(|_| Status::internal("eui delete failed"))?;
        Ok(Response::new(RouteEuisResV1 {}))
    }

    type get_devaddr_rangesStream = GrpcStreamResult<DevaddrRangeV1>;
    async fn get_devaddr_ranges(
        &self,
        request: Request<RouteGetDevaddrRangesReqV1>,
    ) -> GrpcResult<Self::get_devaddr_rangesStream> {
        let request = request.into_inner();

        let devaddrs = route::list_devaddr_ranges_for_route(&request.route_id, &self.pool)
            .await
            .map_err(|_| Status::internal("get devaddr ranges failed"))?;

        let (tx, rx) = tokio::sync::mpsc::channel(20);
        tokio::spawn(async move {
            for devaddr in devaddrs {
                if tx.send(Ok(devaddr.into())).await.is_err() {
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

        while let Ok(Some(update)) = request.message().await {
            match (update.action(), update.devaddr_range) {
                (ActionV1::Add, Some(devaddr)) => to_add.push(devaddr.into()),
                (ActionV1::Remove, Some(devaddr)) => to_remove.push(devaddr.into()),
                _ => return Err(Status::invalid_argument("no devaddr range provided")),
            }
        }
        tracing::debug!(
            adding = to_add.len(),
            removing = to_remove.len(),
            "updating devaddr ranges"
        );

        // TODO: check devaddr ranges against org constraints.
        route::update_devaddr_ranges(&to_add, &to_remove, &self.pool, self.update_channel.clone())
            .await
            .map_err(|err| {
                tracing::error!("devaddr range update failed: {err:?}");
                Status::internal("devaddr range update failed")
            })?;
        Ok(Response::new(RouteDevaddrRangesResV1 {}))
    }

    async fn delete_devaddr_ranges(
        &self,
        request: Request<RouteDeleteDevaddrRangesReqV1>,
    ) -> GrpcResult<RouteDevaddrRangesResV1> {
        let request = request.into_inner();
        route::delete_devaddr_ranges(&request.route_id, &self.pool, self.update_channel.clone())
            .await
            .map_err(|_| Status::internal("devaddr range delete failed"))?;
        Ok(Response::new(RouteDevaddrRangesResV1 {}))
    }
}
