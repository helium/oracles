use crate::{
    follower::{FollowerService, Meta},
    keypair::Keypair,
    pending_txn::{PendingTxn, Status},
    subnetwork_rewards::{construct_txn, SubnetworkRewards},
    transaction::client::TransactionService,
    ConsensusTxnTrigger, Result,
};
use chrono::Utc;
use helium_proto::{blockchain_txn::Txn, BlockchainTxn};
use poc_store::FileStore;
use sqlx::{Pool, Postgres};
use tokio::sync::broadcast;

pub struct Server {
    trigger_receiver: broadcast::Receiver<ConsensusTxnTrigger>,
    pool: Pool<Postgres>,
    keypair: Keypair,
    txn_service: TransactionService,
}

impl Server {
    pub async fn new(
        pool: Pool<Postgres>,
        trigger_receiver: broadcast::Receiver<ConsensusTxnTrigger>,
        keypair: Keypair,
    ) -> Result<Self> {
        let result = Self {
            pool,
            trigger_receiver,
            keypair,
            txn_service: TransactionService::from_env()?,
        };
        Ok(result)
    }

    pub async fn run(&mut self, shutdown: triggered::Listener) -> Result {
        tracing::info!("starting rewards server");

        loop {
            if shutdown.is_triggered() {
                tracing::info!("stopping rewards server");
                return Ok(());
            }
            tokio::select! {
                _ = shutdown.clone() => (),
                trigger = self.trigger_receiver.recv() => {
                    if let Ok(trigger) = trigger {
                        if self.handle_trigger(trigger).await.is_err() {
                            tracing::error!("failed to handle trigger!")
                        }
                    } else {
                        tracing::error!("failed to recv trigger!")
                    }
                }
            }
        }
    }

    pub async fn handle_trigger(&mut self, trigger: ConsensusTxnTrigger) -> Result {
        tracing::info!("chain trigger received {:#?}", trigger);

        match PendingTxn::list(&self.pool, Status::Failed).await {
            Ok(Some(failed_pending_txns)) => {
                tracing::error!("found failed_pending_txns {:#?}", failed_pending_txns)
            }
            Err(_) => {
                tracing::error!("unable to list failed_pending_txns!")
            }
            Ok(None) => match Meta::last_reward_end_time(&self.pool).await {
                Err(_) => {
                    tracing::error!("unable to get failed_pending_txns!")
                }
                Ok(None) => {
                    let kv = handle_first_reward(&self.pool, &trigger).await;
                    tracing::info!("inserted kv: {:#?}", kv);
                }
                Ok(Some(last_reward_end_time)) => {
                    tracing::info!("found last_reward_end_time: {:#?}", last_reward_end_time);

                    let store = FileStore::from_env().await?;
                    let follower_service = FollowerService::from_env()?;
                    let rewards = SubnetworkRewards::from_last_reward_end_time(
                        store,
                        follower_service,
                        last_reward_end_time,
                    )
                    .await?;

                    match Meta::last_reward_height(&self.pool).await? {
                        None => {
                            tracing::error!("cannot continue, no known last_reward_height!")
                        }
                        Some(last_reward_height) => {
                            let _ = &self
                                .handle_rewards(
                                    rewards,
                                    last_reward_height + 1,
                                    trigger.block_height as i64,
                                )
                                .await;
                        }
                    }
                }
            },
        }
        Ok(())
    }

    async fn handle_rewards(
        &mut self,
        rewards: SubnetworkRewards,
        start_epoch: i64,
        end_epoch: i64,
    ) -> Result {
        if rewards.is_empty() {
            tracing::info!("nothing to reward");
            return Ok(());
        }

        let (txn, txn_hash_str) = construct_txn(&self.keypair, rewards, start_epoch, end_epoch)?;
        // insert in the pending_txn tbl (status: created)
        let pt = PendingTxn::insert_new(&self.pool, txn_hash_str.clone()).await?;
        tracing::info!("inserted pending_txn: {:?}", pt);

        // submit the txn
        if let Ok(_resp) = self
            .txn_service
            .submit(
                BlockchainTxn {
                    txn: Some(Txn::SubnetworkRewards(txn)),
                },
                pt.created_at.to_string().as_bytes().to_vec(),
            )
            .await
        {
            PendingTxn::update(&self.pool, &txn_hash_str, Status::Pending, Utc::now()).await?;
        }
        Ok(())
    }
}

async fn handle_first_reward(pool: &Pool<Postgres>, trigger: &ConsensusTxnTrigger) -> Result<Meta> {
    tracing::info!("no last_reward_end_time found, just insert trigger block_timestamp");

    Meta::insert_kv(
        pool,
        "last_reward_end_time",
        &trigger.block_timestamp.to_string(),
    )
    .await
}
