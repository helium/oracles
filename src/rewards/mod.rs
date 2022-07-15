pub mod emissions;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trigger {
    pub block_height: u64,
}

impl Trigger {
    pub fn new(block_height: u64) -> Self {
        Self { block_height }
    }
}
