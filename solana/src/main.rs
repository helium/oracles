use clap::{Parser, ValueEnum};
use helium_crypto::{PublicKey, PublicKeyBinary};
use sha2::{Digest, Sha256};
use solana_sdk::pubkey::Pubkey;

#[derive(Parser)]
#[clap(about = "Look up the Delegated Data Credit account for a Helium router key")]
struct Cli {
    #[clap(value_enum)]
    mode: Dnt,
    payer: PublicKey,
}

#[derive(ValueEnum, Clone)]
enum Dnt {
    Mobile,
    Iot,
}

fn main() {
    let Cli { mode, payer } = Cli::parse();
    let sub_dao: Pubkey = match mode {
        Dnt::Mobile => "Gm9xDCJawDEKDrrQW6haw94gABaYzQwCq4ZQU8h8bd22"
            .parse()
            .unwrap(),
        Dnt::Iot => "39Lw1RH6zt8AJvKn3BTxmUDofzduCM2J3kSaGDZ8L7Sk"
            .parse()
            .unwrap(),
    };
    let payer = PublicKeyBinary::from(payer);
    let mut hasher = Sha256::new();
    hasher.update(payer.to_string());
    let sha_digest = hasher.finalize();
    let (ddc_key, _) = Pubkey::find_program_address(
        &[
            "delegated_data_credits".as_bytes(),
            sub_dao.as_ref(),
            &sha_digest,
        ],
        &helium_anchor_gen::data_credits::ID,
    );
    println!("https://explorer.solana.com/address/{ddc_key}");
}
