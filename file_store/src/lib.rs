extern crate tls_init;

pub mod cli;
pub mod coverage;
pub mod entropy_report;
mod error;
pub mod file_info;
pub mod file_info_poller;
pub mod file_sink;
pub mod file_source;
pub mod file_store;
pub mod file_upload;
pub mod hex_boost;
pub mod iot_beacon_report;
pub mod iot_invalid_poc;
pub mod iot_packet;
pub mod iot_valid_poc;
pub mod iot_witness_report;
pub mod mobile_ban;
pub mod mobile_radio_invalidated_threshold;
pub mod mobile_radio_threshold;
pub mod mobile_session;
pub mod mobile_subscriber;
pub mod mobile_transfer;
pub mod reward_manifest;
mod settings;
pub mod speedtest;
pub mod subscriber_verified_mapping_event;
pub mod subscriber_verified_mapping_event_ingest_report;
pub mod traits;
pub mod unique_connections;
pub mod usage_counts;
pub mod verified_subscriber_verified_mapping_event_ingest_report;
pub mod wifi_heartbeat;

use aws_config::BehaviorVersion;
pub use cli::bucket::FileFilter;
pub use error::{Error, Result};
pub use file_info::{FileInfo, FileType};
pub use file_sink::{FileSink, FileSinkBuilder};
pub use iot_valid_poc::SCALING_PRECISION;
pub use settings::Settings;

use bytes::BytesMut;
use futures::stream::BoxStream;

pub type Stream<T> = BoxStream<'static, Result<T>>;
pub type FileInfoStream = Stream<FileInfo>;
pub type BytesMutStream = Stream<BytesMut>;

pub async fn new_client(
    endpoint: Option<String>,
    _access_key_id: Option<String>,
    _secret_access_key: Option<String>,
) -> aws_sdk_s3::Client {
    let config = aws_config::defaults(BehaviorVersion::latest()).load().await;

    let mut s3_config = aws_sdk_s3::config::Builder::from(&config);
    if let Some(endpoint) = endpoint {
        s3_config = s3_config.endpoint_url(endpoint);
    }

    #[cfg(feature = "local")]
    {
        // NOTE(mj): If you see something like a DNS error, this is probably
        // the culprit. Need to find a way to make this configurable. It
        // would be nice to allow the "local" feature to be active, but not
        // enforce path style.
        s3_config = s3_config.force_path_style(true);

        if let Some((access_key_id, secret_access_key)) = _access_key_id.zip(_secret_access_key) {
            let creds = aws_sdk_s3::config::Credentials::builder()
                .provider_name("Static")
                .access_key_id(access_key_id)
                .secret_access_key(secret_access_key);

            s3_config = s3_config.credentials_provider(creds.build());
        }
    }

    aws_sdk_s3::Client::from_conf(s3_config.build())
}

#[cfg(test)]
tls_init::include_tls_tests!();
