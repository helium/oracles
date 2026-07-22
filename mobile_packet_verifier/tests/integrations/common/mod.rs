use std::time::Duration;

use helium_iceberg::IcebergTestHarness;
use mobile_packet_verifier::{gateway::GatewayResolver, iceberg};

pub mod hotspot_inventory;

pub async fn setup_iceberg() -> anyhow::Result<IcebergTestHarness> {
    let harness = IcebergTestHarness::new_with_tables([
        iceberg::session::table_definition()?,
        iceberg::invalid_session::table_definition()?,
        iceberg::burned_session::table_definition()?,
        hotspot_inventory::table_definition()?,
    ])
    .await?;

    Ok(harness)
}

/// Build a real [`GatewayResolver`] backed by the harness's Trino, pointed at
/// the per-test `chain.mobile_hotspot_inventory` table. Seed the gateways that
/// should be considered known with [`hotspot_inventory::seed`] first.
pub async fn gateway_resolver(harness: &IcebergTestHarness) -> anyhow::Result<GatewayResolver> {
    let trino = trino_client::Client::from_client(harness.owned_trino().await?);
    // Tests exercise the startup snapshot + fallback only; they never build a
    // refresher, so no background refresh fires during a test.
    Ok(GatewayResolver::new_with_inventory_table(
        trino,
        hotspot_inventory::RESOLVER_TABLE,
        Duration::from_secs(3600),
    )
    .await)
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
