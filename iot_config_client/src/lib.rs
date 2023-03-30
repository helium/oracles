pub mod gateway_resolver;
pub mod iot_config_client;
pub mod region_params_resolver;
mod settings;

use futures::stream::BoxStream;
pub use settings::Settings;

pub type Stream<T> = BoxStream<'static, T>;
pub type GatewayInfoStream = Stream<gateway_resolver::GatewayInfo>;
