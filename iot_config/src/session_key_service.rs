use crate::{GrpcResult, GrpcStreamRequest, GrpcStreamResult};
use helium_proto::services::iot_config::{
    self, SessionKeyFilterGetReqV1, SessionKeyFilterListReqV1, SessionKeyFilterStreamReqV1,
    SessionKeyFilterStreamResV1, SessionKeyFilterUpdateReqV1, SessionKeyFilterUpdateResV1,
    SessionKeyFilterV1,
};
use tonic::Request;

pub struct SessionKeyFilterService {}

#[tonic::async_trait]
impl iot_config::SessionKeyFilter for SessionKeyFilterService {
    type listStream = GrpcStreamResult<SessionKeyFilterV1>;
    async fn list(
        &self,
        _request: Request<SessionKeyFilterListReqV1>,
    ) -> GrpcResult<Self::listStream> {
        unimplemented!();
    }

    type getStream = GrpcStreamResult<SessionKeyFilterV1>;
    async fn get(
        &self,
        _request: Request<SessionKeyFilterGetReqV1>,
    ) -> GrpcResult<Self::getStream> {
        unimplemented!();
    }

    async fn update(
        &self,
        _request: GrpcStreamRequest<SessionKeyFilterUpdateReqV1>,
    ) -> GrpcResult<SessionKeyFilterUpdateResV1> {
        unimplemented!();
    }

    type streamStream = GrpcStreamResult<SessionKeyFilterStreamResV1>;
    async fn stream(
        &self,
        _request: Request<SessionKeyFilterStreamReqV1>,
    ) -> GrpcResult<Self::streamStream> {
        unimplemented!();
    }
}
