use clap::Parser;
use helium_crypto::{PublicKey, PublicKeyBinary};
use sha2::{Digest, Sha256};
use solana_sdk::pubkey::Pubkey;

#[derive(Parser)]
#[clap(about = "Look up the DC escrow account for a Payer account")]
struct Cli {
    payer: PublicKey,
}

fn main() {
    let Cli { payer } = Cli::parse();
    let sub_dao: Pubkey = "39Lw1RH6zt8AJvKn3BTxmUDofzduCM2J3kSaGDZ8L7Sk"
        .parse()
        .unwrap();
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
        &data_credits::ID,
    );
    let (escrow_account, _) = Pubkey::find_program_address(
        &["escrow_dc_account".as_bytes(), &ddc_key.to_bytes()],
        &data_credits::ID,
    );
    println!("https://explorer.solana.com/address/{escrow_account}");
}
