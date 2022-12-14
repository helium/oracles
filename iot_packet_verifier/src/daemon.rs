use crate::ingest;
use crate::verifier::{VerifiedStream, Verifier};
use anyhow::Result;
use file_store::{file_sink, FileStore};
use node_follower::{follower_service::FollowerService, gateway_resp::GatewayInfoResolver};
use std::collections::HashMap;

pub struct Daemon {
    follower: FollowerService,
    file_store: FileStore,
    subnet_rewards: file_sink::MessageSender,
    subnet_debits: file_sink::MessageSender,
}

impl Daemon {
    async fn handle_tick(&mut self) -> anyhow::Result<()> {
        let VerifiedStream {
            gateway_counters,
            oui_counters,
        } = Verifier::new(ingest::ingest_reports(&self.file_store, todo!()))
            .verify_stream()
            .await;

        let mut owner_rewards = HashMap::<Vec<u8>, u64>::new();
        for (gateway, amount) in gateway_counters {
            let owner = self.follower.resolve_gateway_info(&gateway).await?.owner;
            *owner_rewards.entry(owner).or_default() += amount;
        }

        // TODO: Write to subnet_rewards

        for (oui, amount) in oui_counters {
            // TODO: look up payer account, check amounts, debits

            // TODO: write out subnet_debits
        }

        Ok(())
    }
}
