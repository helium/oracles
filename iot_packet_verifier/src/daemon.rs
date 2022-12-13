use crate::ingest;
use crate::verifier::{VerifiedEpoch, Verifier};
use anyhow::Result;
use file_store::{file_sink, FileStore};
use node_follower::{follower_service::FollowerService, gateway_resp::GatewayInfoResolver};

pub struct Daemon {
    follower: FollowerService,
    file_store: FileStore,
    subnet_rewards: file_sink::MessageSender,
    subnet_debits: file_sink::MessageSender,
}

impl Daemon {
    async fn handle_tick(&mut self) -> anyhow::Result<()> {
        let VerifiedEpoch {
            gateway_counters,
            oui_counters,
        } = Verifier::new(ingest::ingest_reports(&self.file_store, todo!()))
            .verify_epoch()
            .await;

        for (gateway, amount) in gateway_counters {
            let account = self.follower.resolve_gateway_info(&gateway).await?;

            // TODO: Write to subnet_rewards
        }

        for (oui, amount) in oui_counters {
            // TODO: look up payer account, check amounts, debits

            // TODO: write out subnet_debits
        }

        Ok(())
    }
}
