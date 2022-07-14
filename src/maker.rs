use crate::PublicKey;
use once_cell::sync::OnceCell;
use serde::Serialize;
use std::str::FromStr;

#[derive(Serialize)]
pub struct Maker {
    pub pubkey: PublicKey,
    pub description: &'static str,
}

impl Maker {
    fn new(pubkey: &'static str, description: &'static str) -> Self {
        Self {
            pubkey: PublicKey::from_str(pubkey).expect("maker public key"),
            description,
        }
    }
}

pub fn allowed() -> &'static Vec<Maker> {
    static CELL: OnceCell<Vec<Maker>> = OnceCell::new();
    CELL.get_or_init(|| {
        vec![
            Maker::new(
                "13y2EqUUzyQhQGtDSoXktz8m5jHNSiwAKLTYnHNxZq2uH5GGGym",
                "FreedomFi",
            ),
            Maker::new(
                "14sKWeeYWQWrBSnLGq79uRQqZyw3Ldi7oBdxbF6a54QboTNBXDL",
                "Bobcat 5G",
            ),
        ]
    })
}

pub fn allows(pubkey: &PublicKey) -> bool {
    allowed().iter().any(|maker| maker.pubkey.eq(pubkey))
}
