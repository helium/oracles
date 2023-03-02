pub mod error;
pub mod price_generator;
pub mod price_service;
pub mod settings;

pub use error::PriceError;
pub use price_generator::PriceGenerator;
pub use settings::Settings;

use tonic::{Response, Status};

pub type GrpcResult<T> = Result<Response<T>, Status>;
