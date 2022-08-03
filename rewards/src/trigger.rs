#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsensusTxnTrigger {
    pub block_height: i64,
    pub block_timestamp: i64,
}

impl ConsensusTxnTrigger {
    pub fn new(block_height: i64, block_timestamp: i64) -> Self {
        Self {
            block_height,
            block_timestamp,
        }
    }
}
