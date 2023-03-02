use crate::{price_generator::MessageReceiver, GrpcResult};
use helium_proto::{
    services::price_oracle::{PriceOracle, PriceOracleReportV1, PriceOracleReqV1},
    BlockchainTokenTypeV1,
};
use tonic::{Request, Status};

pub struct PriceService {
    price_watch: MessageReceiver,
}

impl PriceService {
    pub fn new(price_watch: MessageReceiver) -> anyhow::Result<Self> {
        Ok(Self { price_watch })
    }
}

#[tonic::async_trait]
impl PriceOracle for PriceService {
    async fn price_oracle(
        &self,
        request: Request<PriceOracleReqV1>,
    ) -> GrpcResult<PriceOracleReportV1> {
        let price_request = request.into_inner();
        let request_tt = price_request.token_type;

        let price = &*self.price_watch.borrow();

        match BlockchainTokenTypeV1::from_i32(request_tt) {
            None => Err(Status::not_found("unknown token_type {request_tt}")),
            Some(tt) => {
                if price.token_type == tt {
                    let inner_resp: PriceOracleReportV1 = price.clone().into();
                    metrics::increment_counter!("price_server_get_count");
                    Ok(tonic::Response::new(inner_resp))
                } else {
                    Err(Status::invalid_argument("invalid token_type {request_tt}"))
                }
            }
        }
    }
}
