pub mod authorization_client;
pub mod carrier_service_client;
pub mod entity_client;
pub mod hex_boosting_client;
mod settings;
pub mod sub_dao_client;

use std::time::Duration;

pub use crate::gateway::client::GatewayClient;
pub use authorization_client::AuthorizationClient;
pub use carrier_service_client::CarrierServiceClient;
pub use entity_client::EntityClient;
pub use settings::Settings;
pub use sub_dao_client::SubDaoClient;

pub const CACHE_EVICTION_FREQUENCY: Duration = Duration::from_secs(60 * 60);

#[derive(thiserror::Error, Debug)]
pub enum ClientError {
    #[error("error signing request {0}")]
    SigningError(#[from] helium_crypto::Error),
    #[error("grpc error response {0}")]
    GrpcError(#[from] tonic::Status),
    #[error("error verifying response signature {0}")]
    VerificationError(#[from] file_store::Error),
    #[error("error parsing gateway location {0}")]
    LocationParseError(#[from] std::num::ParseIntError),
    #[error("unknown service provider {0}")]
    UnknownServiceProvider(String),
    #[error("Invalid GatewayInfo proto response {0}")]
    InvalidGatewayInfoProto(#[from] crate::gateway::info::GatewayInfoProtoParseError),
    #[error("Invalid SubDaoRewardInfo proto response {0}")]
    InvalidSubDaoRewardInfoProto(
        #[from] crate::sub_dao_epoch_reward_info::SubDaoRewardInfoParseError,
    ),
}

macro_rules! call_with_retry {
    ($rpc:expr) => {{
        use tonic::Code;

        let mut attempt = 1;
        loop {
            match $rpc.await {
                Ok(resp) => break Ok(resp),
                Err(status) => match status.code() {
                    Code::Cancelled | Code::DeadlineExceeded | Code::Unavailable => {
                        if attempt < 3 {
                            attempt += 1;
                            tokio::time::sleep(Duration::from_secs(attempt)).await;
                            continue;
                        } else {
                            break Err(status);
                        }
                    }
                    _ => break Err(status),
                },
            }
        }
    }};
}

pub(crate) use call_with_retry;
