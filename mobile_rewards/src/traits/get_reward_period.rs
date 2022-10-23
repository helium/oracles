use crate::Result;
use async_trait::async_trait;

#[derive(Debug)]
pub struct RewardPeriod {
    pub start: u64,
    pub end: u64,
}

impl RewardPeriod {
    pub fn new(start: u64, end: u64) -> Self {
        Self { start, end }
    }
}

#[async_trait]
pub trait GetRewardPeriod {
    async fn reward_period(&mut self) -> Result<RewardPeriod>;
}
