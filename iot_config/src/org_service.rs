use crate::GrpcResult;
use helium_proto::services::iot_config::{
    self, OrgCreateHeliumReqV1, OrgCreateRoamerReqV1, OrgDisableReqV1, OrgDisableResV1,
    OrgGetReqV1, OrgListReqV1, OrgListResV1, OrgResV1,
};
use tonic::Request;

pub struct OrgService {}

#[tonic::async_trait]
impl iot_config::Org for OrgService {
    async fn list(&self, _request: Request<OrgListReqV1>) -> GrpcResult<OrgListResV1> {
        unimplemented!()
    }

    async fn get(&self, _request: Request<OrgGetReqV1>) -> GrpcResult<OrgResV1> {
        unimplemented!()
    }

    async fn create_helium(&self, _request: Request<OrgCreateHeliumReqV1>) -> GrpcResult<OrgResV1> {
        unimplemented!()
    }

    async fn create_roamer(&self, _request: Request<OrgCreateRoamerReqV1>) -> GrpcResult<OrgResV1> {
        unimplemented!()
    }

    async fn disable(&self, _request: Request<OrgDisableReqV1>) -> GrpcResult<OrgDisableResV1> {
        unimplemented!()
    }
}
