use crate::{lora_field, org, GrpcResult, Settings, HELIUM_NET_ID};
use anyhow::Result;
use file_store::traits::MsgVerify;
use helium_crypto::{Network, PublicKey};
use helium_proto::services::iot_config::{
    self, OrgCreateHeliumReqV1, OrgCreateRoamerReqV1, OrgDisableReqV1, OrgDisableResV1,
    OrgEnableReqV1, OrgEnableResV1, OrgGetReqV1, OrgListReqV1, OrgListResV1, OrgResV1, OrgV1,
};
use sqlx::{Pool, Postgres};
use tonic::{Request, Response, Status};

pub struct OrgService {
    pool: Pool<Postgres>,
    required_network: Network,
}

impl OrgService {
    pub async fn new(settings: &Settings) -> Result<Self> {
        Ok(Self {
            pool: settings.database.connect(10).await?,
            required_network: settings.network,
        })
    }

    fn verify_network(&self, public_key: PublicKey) -> Result<PublicKey, Status> {
        if self.required_network == public_key.network {
            Ok(public_key)
        } else {
            Err(Status::invalid_argument("invalid network"))
        }
    }

    fn verify_public_key(&self, bytes: &[u8]) -> Result<PublicKey, Status> {
        PublicKey::try_from(bytes).map_err(|_| Status::invalid_argument("invalid public key"))
    }

    fn verify_signature<R>(
        &self,
        public_key: PublicKey,
        request: R,
    ) -> Result<(PublicKey, R), Status>
    where
        R: MsgVerify,
    {
        request
            .verify(&public_key)
            .map_err(|_| Status::invalid_argument("invalid signature"))?;
        Ok((public_key, request))
    }
}

#[tonic::async_trait]
impl iot_config::Org for OrgService {
    async fn list(&self, _request: Request<OrgListReqV1>) -> GrpcResult<OrgListResV1> {
        let proto_orgs: Vec<OrgV1> = org::list(&self.pool)
            .await
            .map_err(|_| Status::internal("org list failed"))?
            .into_iter()
            .map(|org| org.into())
            .collect();

        Ok(Response::new(OrgListResV1 { orgs: proto_orgs }))
    }

    async fn get(&self, request: Request<OrgGetReqV1>) -> GrpcResult<OrgResV1> {
        let request = request.into_inner();

        let org = org::get_with_constraints(request.oui, &self.pool)
            .await
            .map_err(|_| Status::internal("org get failed"))?;
        let net_id = org
            .constraints
            .start_addr
            .to_net_id()
            .map_err(|_| Status::internal("net id error"))?;

        Ok(Response::new(OrgResV1 {
            org: Some(org.org.into()),
            net_id: net_id.into(),
            devaddr_ranges: vec![org.constraints.into()],
        }))
    }

    async fn create_helium(&self, request: Request<OrgCreateHeliumReqV1>) -> GrpcResult<OrgResV1> {
        let request = request.into_inner();

        let (_, req) = self
            .verify_public_key(request.signer.as_ref())
            .and_then(|pub_key| self.verify_network(pub_key))
            .and_then(|pub_key| self.verify_signature(pub_key, request))?;

        self.verify_public_key(req.owner.as_ref())
            .and_then(|pub_key| self.verify_network(pub_key))?;
        self.verify_public_key(req.payer.as_ref())
            .and_then(|pub_key| self.verify_network(pub_key))?;

        let requested_addrs = req.devaddrs;
        let devaddr_range = org::next_helium_devaddr(&self.pool)
            .await
            .map_err(|_| Status::failed_precondition("helium address unavailable"))?
            .to_range(requested_addrs);

        let org = org::create_org(req.owner.into(), req.payer.into(), vec![], &self.pool)
            .await
            .map_err(|_| Status::internal("org save failed"))?;

        org::insert_constraints(org.oui, HELIUM_NET_ID, &devaddr_range, &self.pool)
            .await
            .map_err(|_| Status::internal("org constraints save failed"))?;

        Ok(Response::new(OrgResV1 {
            org: Some(org.into()),
            net_id: HELIUM_NET_ID.into(),
            devaddr_ranges: vec![devaddr_range.into()],
        }))
    }

    async fn create_roamer(&self, request: Request<OrgCreateRoamerReqV1>) -> GrpcResult<OrgResV1> {
        let request = request.into_inner();

        let (_, req) = self
            .verify_public_key(request.signer.as_ref())
            .and_then(|pub_key| self.verify_network(pub_key))
            .and_then(|pub_key| self.verify_signature(pub_key, request))?;

        self.verify_public_key(req.owner.as_ref())
            .and_then(|pub_key| self.verify_network(pub_key))?;
        self.verify_public_key(req.payer.as_ref())
            .and_then(|pub_key| self.verify_network(pub_key))?;

        let net_id = lora_field::net_id(req.net_id);
        let devaddr_range = net_id
            .full_range()
            .map_err(|_| Status::invalid_argument("invalid net_id"))?;

        let org = org::create_org(req.owner.into(), req.payer.into(), vec![], &self.pool)
            .await
            .map_err(|_| Status::internal("org save failed"))?;

        org::insert_constraints(org.oui, net_id, &devaddr_range, &self.pool)
            .await
            .map_err(|_| Status::internal("org constraints save failed"))?;

        Ok(Response::new(OrgResV1 {
            org: Some(org.into()),
            net_id: net_id.into(),
            devaddr_ranges: vec![devaddr_range.into()],
        }))
    }

    async fn disable(&self, _request: Request<OrgDisableReqV1>) -> GrpcResult<OrgDisableResV1> {
        unimplemented!()
    }

    async fn enable(&self, _request: Request<OrgEnableReqV1>) -> GrpcResult<OrgEnableResV1> {
        unimplemented!()
    }
}
