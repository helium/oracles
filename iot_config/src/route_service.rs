use crate::{GrpcResult, GrpcStreamResult};
use helium_proto::services::iot_config::{
    self, RouteCreateReqV1, RouteDeleteReqV1, RouteDevaddrsReqV1, RouteDevaddrsResV1,
    RouteEuisReqV1, RouteEuisResV1, RouteGetReqV1, RouteListReqV1, RouteListResV1,
    RouteStreamReqV1, RouteStreamResV1, RouteUpdateReqV1, RouteV1,
};
use tonic::Request;

pub struct RouteService {}

#[tonic::async_trait]
impl iot_config::Route for RouteService {
    async fn list(&self, _request: Request<RouteListReqV1>) -> GrpcResult<RouteListResV1> {
        unimplemented!()
    }

    async fn get(&self, _request: Request<RouteGetReqV1>) -> GrpcResult<RouteV1> {
        unimplemented!()
    }

    async fn create(&self, _request: Request<RouteCreateReqV1>) -> GrpcResult<RouteV1> {
        unimplemented!()
    }

    async fn update(&self, _request: Request<RouteUpdateReqV1>) -> GrpcResult<RouteV1> {
        unimplemented!()
    }

    async fn delete(&self, _request: Request<RouteDeleteReqV1>) -> GrpcResult<RouteV1> {
        unimplemented!()
    }

    async fn euis(&self, _request: Request<RouteEuisReqV1>) -> GrpcResult<RouteEuisResV1> {
        unimplemented!()
    }

    async fn devaddrs(
        &self,
        _request: Request<RouteDevaddrsReqV1>,
    ) -> GrpcResult<RouteDevaddrsResV1> {
        unimplemented!()
    }

    type streamStream = GrpcStreamResult<RouteStreamResV1>;
    async fn stream(&self, _request: Request<RouteStreamReqV1>) -> GrpcResult<Self::streamStream> {
        unimplemented!()
    }
}
