use crate::{GrpcResult, GrpcStreamResult};
use helium_proto::services::iot_config::{
    self, SessionKeyFilterCreateReqV1, SessionKeyFilterDeleteReqV1, SessionKeyFilterGetReqV1,
    SessionKeyFilterListReqV1, SessionKeyFilterListResV1, SessionKeyFilterStreamReqV1,
    SessionKeyFilterStreamResV1, SessionKeyFilterUpdateReqV1, SessionKeyFilterV1,
};
use tonic::Request;

pub struct SessionKeyFilterService {}

#[tonic::async_trait]
impl iot_config::SessionKeyFilter for SessionKeyFilterService {
    async fn list(
        &self,
        _request: Request<SessionKeyFilterListReqV1>,
    ) -> GrpcResult<SessionKeyFilterListResV1> {
        unimplemented!()
    }

    async fn get(
        &self,
        _request: Request<SessionKeyFilterGetReqV1>,
    ) -> GrpcResult<SessionKeyFilterV1> {
        unimplemented!()
    }

    async fn create(
        &self,
        _request: Request<SessionKeyFilterCreateReqV1>,
    ) -> GrpcResult<SessionKeyFilterV1> {
        unimplemented!()
    }

    async fn update(
        &self,
        _request: Request<SessionKeyFilterUpdateReqV1>,
    ) -> GrpcResult<SessionKeyFilterV1> {
        unimplemented!()
    }

    async fn delete(
        &self,
        _request: Request<SessionKeyFilterDeleteReqV1>,
    ) -> GrpcResult<SessionKeyFilterV1> {
        unimplemented!()
    }

    type streamStream = GrpcStreamResult<SessionKeyFilterStreamResV1>;
    async fn stream(
        &self,
        _request: Request<SessionKeyFilterStreamReqV1>,
    ) -> GrpcResult<Self::streamStream> {
        unimplemented!()
    }
}
