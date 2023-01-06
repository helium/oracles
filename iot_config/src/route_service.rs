use crate::{
    lora_field::{DevAddrRange, Eui},
    org::OrgStatus,
    route::{self, Route},
    GrpcResult, GrpcStreamResult, Settings,
};
use anyhow::Result;
use futures::stream::StreamExt;
use helium_proto::services::iot_config::{
    self, ActionV1, RouteCreateReqV1, RouteDeleteReqV1, RouteDevaddrsReqV1, RouteDevaddrsResV1,
    RouteEuisReqV1, RouteEuisResV1, RouteGetReqV1, RouteListReqV1, RouteListResV1,
    RouteStreamReqV1, RouteStreamResV1, RouteUpdateReqV1, RouteV1,
};
use sqlx::{Pool, Postgres};
use std::sync::Arc;
use tokio::{
    pin,
    sync::broadcast::{Receiver, Sender},
};
use tonic::{Request, Response, Status};

pub struct RouteService {
    pool: Pool<Postgres>,
    update_channel: Arc<Sender<RouteStreamResV1>>,
}

impl RouteService {
    pub async fn new(settings: &Settings) -> Result<Self> {
        let (update_tx, _) = tokio::sync::broadcast::channel(128);

        Ok(Self {
            pool: settings.database.connect(10).await?,
            update_channel: Arc::new(update_tx),
        })
    }

    fn subscribe_to_routes(&self) -> Receiver<RouteStreamResV1> {
        self.update_channel.subscribe()
    }
}

#[tonic::async_trait]
impl iot_config::Route for RouteService {
    async fn list(&self, request: Request<RouteListReqV1>) -> GrpcResult<RouteListResV1> {
        let request = request.into_inner();

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

        let route = route::get_route(&request.id, &self.pool)
            .await
            .map_err(|_| Status::internal("fetch route failed"))?;

        Ok(Response::new(route.into()))
    }

    async fn create(&self, request: Request<RouteCreateReqV1>) -> GrpcResult<RouteV1> {
        let request = request.into_inner();

        let route: Route = request
            .route
            .ok_or("missing route")
            .map_err(Status::invalid_argument)?
            .into();

        let new_route: Route =
            route::create_route(route.clone(), &self.pool, self.update_channel.clone())
                .await
                .map_err(|_| Status::internal("route create failed"))?;

        Ok(Response::new(new_route.into()))
    }

    async fn update(&self, request: Request<RouteUpdateReqV1>) -> GrpcResult<RouteV1> {
        let request = request.into_inner();
        let route_proto = request
            .route
            .ok_or("missing route")
            .map_err(Status::invalid_argument)?;

        let route: Route = route_proto.into();

        let updated_route = route::update_route(route, &self.pool, self.update_channel.clone())
            .await
            .map_err(|_| Status::internal("update route failed"))?;

        Ok(Response::new(updated_route.into()))
    }

    async fn delete(&self, request: Request<RouteDeleteReqV1>) -> GrpcResult<RouteV1> {
        let request = request.into_inner();
        let route_id = request.id;

        let route = route::get_route(&route_id, &self.pool)
            .await
            .map_err(|_| Status::internal("fetch route failed"))?;

        route::delete_route(&route_id, &self.pool, self.update_channel.clone())
            .await
            .map_err(|_| Status::internal("delete route failed"))?;

        Ok(Response::new(route.into()))
    }

    async fn euis(&self, request: Request<RouteEuisReqV1>) -> GrpcResult<RouteEuisResV1> {
        let request = request.into_inner();
        let euis: Vec<Eui> = request.euis.iter().map(|eui| eui.into()).collect();

        route::modify_euis(
            &request.id,
            request.action(),
            &euis,
            &self.pool,
            self.update_channel.clone(),
        )
        .await
        .map_err(|_| Status::internal("eui modify failed"))?;

        Ok(Response::new(RouteEuisResV1 {
            id: request.id,
            action: request.action,
            euis: request.euis,
        }))
    }

    async fn devaddrs(
        &self,
        request: Request<RouteDevaddrsReqV1>,
    ) -> GrpcResult<RouteDevaddrsResV1> {
        let request = request.into_inner();
        let ranges: Vec<DevAddrRange> = request
            .devaddr_ranges
            .iter()
            .map(|range| range.into())
            .collect();

        route::modify_devaddr_ranges(
            &request.id,
            request.action(),
            &ranges,
            &self.pool,
            self.update_channel.clone(),
        )
        .await
        .map_err(|_| Status::internal("devaddr ranges modify failed"))?;

        Ok(Response::new(RouteDevaddrsResV1 {
            id: request.id,
            action: request.action,
            devaddr_ranges: request.devaddr_ranges,
        }))
    }

    type streamStream = GrpcStreamResult<RouteStreamResV1>;
    async fn stream(&self, _request: Request<RouteStreamReqV1>) -> GrpcResult<Self::streamStream> {
        tracing::info!("client subscribed to route stream");
        let pool = self.pool.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(20);

        let mut route_updates = self.subscribe_to_routes();

        tokio::spawn(async move {
            let active_routes = route::route_stream_by_status(OrgStatus::Enabled, &pool).await;

            pin!(active_routes);

            while let Some(active_route) = active_routes.next().await {
                if (tx.send(Ok(RouteStreamResV1 {
                    action: ActionV1::Create.into(),
                    route: Some(active_route.into()),
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
}
