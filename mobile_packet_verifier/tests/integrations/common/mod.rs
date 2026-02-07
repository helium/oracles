use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use helium_iceberg::IcebergTestHarness;
use mobile_packet_verifier::{iceberg, MobileConfigResolverExt};

pub async fn setup_iceberg() -> anyhow::Result<IcebergTestHarness> {
    let harness = IcebergTestHarness::new().await?;
    harness
        .create_table(iceberg::data_transfer_session::table_definition(
            harness.namespace(),
        ))
        .await?;
    Ok(harness)
}

enum ValidKeys {
    All,
    Only(Vec<PublicKeyBinary>),
}

impl ValidKeys {
    fn is_valid(&self, public_key: &PublicKeyBinary) -> bool {
        match self {
            ValidKeys::All => true,
            ValidKeys::Only(vec) => vec.contains(public_key),
        }
    }
}

pub struct TestMobileConfig {
    valid_gateways: ValidKeys,
    valid_routing_keys: ValidKeys,
}

#[async_trait::async_trait]
impl MobileConfigResolverExt for TestMobileConfig {
    async fn is_gateway_known(
        &self,
        public_key: &PublicKeyBinary,
        _gateway_query_time: &DateTime<Utc>,
    ) -> bool {
        self.valid_gateways.is_valid(public_key)
    }

    async fn is_routing_key_known(&self, public_key: &PublicKeyBinary) -> bool {
        self.valid_routing_keys.is_valid(public_key)
    }
}

impl TestMobileConfig {
    pub fn all_valid() -> Self {
        Self {
            valid_gateways: ValidKeys::All,
            valid_routing_keys: ValidKeys::All,
        }
    }

    pub fn valid_gateways(valid: Vec<PublicKeyBinary>) -> Self {
        Self {
            valid_gateways: ValidKeys::Only(valid),
            valid_routing_keys: ValidKeys::All,
        }
    }

    pub fn valid_routing_keys(valid: Vec<PublicKeyBinary>) -> Self {
        Self {
            valid_gateways: ValidKeys::All,
            valid_routing_keys: ValidKeys::Only(valid),
        }
    }
}

pub trait TestChannelExt {
    fn assert_num_msgs(&mut self, expected_count: usize) -> anyhow::Result<()>;
    fn assert_not_empty(&mut self) -> anyhow::Result<()>;
    fn assert_is_empty(&mut self) -> anyhow::Result<()>;
}

impl<T: std::fmt::Debug> TestChannelExt for tokio::sync::mpsc::Receiver<T> {
    fn assert_not_empty(&mut self) -> anyhow::Result<()> {
        match self.try_recv() {
            Ok(_) => (),
            other => panic!("expected message in channel: {other:?}"),
        }
        Ok(())
    }

    fn assert_is_empty(&mut self) -> anyhow::Result<()> {
        match self.try_recv() {
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => (),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => (),
            other => panic!("expected channel to be empty: {other:?}"),
        }
        Ok(())
    }

    fn assert_num_msgs(&mut self, expected_count: usize) -> anyhow::Result<()> {
        let mut count = 0;

        while self.try_recv().is_ok() {
            count += 1;
        }

        if count == expected_count {
            Ok(())
        } else {
            panic!("expected {expected_count} messages, got {count}");
        }
    }
}
