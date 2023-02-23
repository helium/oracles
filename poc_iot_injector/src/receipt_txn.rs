use base64::Engine;
use file_store::{
    iot_valid_poc::{IotPoc, IotValidBeaconReport, IotVerifiedWitnessReport},
    traits::MsgDecode,
};
use helium_crypto::{Keypair, Sign};
use helium_proto::{
    blockchain_txn::Txn, BlockchainPocPathElementV1, BlockchainPocReceiptV1,
    BlockchainPocWitnessV1, BlockchainTxn, BlockchainTxnPocReceiptsV2, Message,
};
use once_cell::sync::Lazy;
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
use rust_decimal::{prelude::ToPrimitive, Decimal};
use rust_decimal_macros::dec;
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// REWARD_SHARE_MULTIPLIER is set to 10000 to ensure that hotspots with very low hex_scale values
/// are still getting some rewards.
static REWARD_SHARE_MULTIPLIER: Lazy<Decimal> = Lazy::new(|| dec!(10000));

const SIGNAL_MULTIPLIER: i32 = 10;
const SNR_MULTIPLIER: f32 = 10.0;
type PocPath = Vec<BlockchainPocPathElementV1>;

#[derive(Clone, Debug)]
pub struct TxnDetails {
    pub txn: BlockchainTxn,
    pub hash: Vec<u8>,
    pub hash_b64_url: String,
}

#[derive(thiserror::Error, Debug)]
pub enum TxnConstructionError {
    #[error("decode error: {0}")]
    FileStoreError(#[from] file_store::Error),
    #[error("signing error: {0}")]
    CryptoError(#[from] Box<helium_crypto::Error>),
}

pub fn handle_report_msg(
    msg: prost::bytes::BytesMut,
    keypair: Arc<Keypair>,
    max_witnesses_per_receipt: u64,
) -> Result<TxnDetails, TxnConstructionError> {
    // Path is always single element, till we decide to change it at some point.
    let mut path: PocPath = Vec::with_capacity(1);
    let iot_poc = IotPoc::decode(msg)?;

    // verifier now places a cap on the number of selected witnesses
    // so squishing here is now duplicate
    // but leaving it in as backup in case
    // verifier and injector diverge on this point
    // the squishing will only take place anyway if the
    // selected witness count exceeds max_witnesses_per_receipt
    // which it should not
    let mut poc_witnesses = construct_poc_witnesses(iot_poc.selected_witnesses);
    maybe_squish_witnesses(
        &mut poc_witnesses,
        &iot_poc.poc_id,
        max_witnesses_per_receipt as usize,
    );

    let (poc_receipt, beacon_received_ts) = construct_poc_receipt(iot_poc.beacon_report);

    // TODO: Double check whether the gateway in the poc_receipt is challengee?
    let path_element =
        construct_path_element(poc_receipt.clone().gateway, poc_receipt, poc_witnesses);

    path.push(path_element);

    let (bare_txn, hash, hash_b64_url) = construct_signed_txn(path, beacon_received_ts, &keypair)?;
    Ok(TxnDetails {
        txn: wrap_txn(bare_txn),
        hash,
        hash_b64_url,
    })
}

/// Maybe squish poc_witnesses if the length is > max_witnesses_per_receipt
fn maybe_squish_witnesses(
    poc_witnesses: &mut Vec<BlockchainPocWitnessV1>,
    poc_id: &[u8],
    max_witnesses_per_receipt: usize,
) {
    if poc_witnesses.len() <= max_witnesses_per_receipt {
        return;
    }

    // Seed a random number from the poc_id for shuffling the witnesses
    let seed = Sha256::digest(poc_id);
    let mut rng = StdRng::from_seed(seed.into());

    // Shuffle and truncate witnesses
    poc_witnesses.shuffle(&mut rng);
    poc_witnesses.truncate(max_witnesses_per_receipt)
}

fn wrap_txn(txn: BlockchainTxnPocReceiptsV2) -> BlockchainTxn {
    BlockchainTxn {
        txn: Some(Txn::PocReceiptsV2(txn)),
    }
}

fn construct_signed_txn(
    path: PocPath,
    timestamp: i64,
    keypair: &Keypair,
) -> Result<(BlockchainTxnPocReceiptsV2, Vec<u8>, String), TxnConstructionError> {
    let strip_path = strip_path(path.clone());

    let mut txn = BlockchainTxnPocReceiptsV2 {
        challenger: keypair.public_key().to_vec(),
        secret: vec![],
        onion_key_hash: vec![],
        path: strip_path,
        fee: 0,
        signature: vec![],
        block_hash: vec![],
        timestamp: timestamp as u64,
    };

    txn.signature = sign_txn(&txn, keypair)?;

    // Re-attach the original (unstripped) path back to the txn after signing
    txn.path = path;

    let (txn_hash, txn_hash_b64url) = hash_txn(&txn);
    Ok((txn, txn_hash, txn_hash_b64url))
}

/// Before signing the txn we strip the path of hex_scale and reward_unit contained in the
/// individual receipt and witnesses
fn strip_path(path: PocPath) -> PocPath {
    let mut strip_path: PocPath = Vec::with_capacity(1);
    for element in path {
        let challengee = element.challengee;
        let mut new_element = BlockchainPocPathElementV1 {
            challengee,
            witnesses: vec![],
            receipt: None,
        };
        let mut witnesses = vec![];
        for mut witness in element.witnesses {
            witness.reward_unit = 0;
            witness.hex_scale = 0;
            witnesses.push(witness)
        }
        new_element.witnesses = witnesses;
        if let Some(mut receipt) = element.receipt {
            receipt.hex_scale = 0;
            receipt.reward_unit = 0;
            new_element.receipt = Some(receipt)
        }
        strip_path.push(new_element)
    }
    strip_path
}

fn construct_path_element(
    challengee: Vec<u8>,
    poc_receipt: BlockchainPocReceiptV1,
    poc_witnesses: Vec<BlockchainPocWitnessV1>,
) -> BlockchainPocPathElementV1 {
    BlockchainPocPathElementV1 {
        challengee,
        receipt: Some(poc_receipt),
        witnesses: poc_witnesses,
    }
}

fn construct_poc_witnesses(
    witness_reports: Vec<IotVerifiedWitnessReport>,
) -> Vec<BlockchainPocWitnessV1> {
    let mut poc_witnesses: Vec<BlockchainPocWitnessV1> = Vec::with_capacity(witness_reports.len());
    for witness_report in witness_reports {
        let witness_invalid_reason = witness_report.invalid_reason as i32;
        let hex_scale = witness_report.hex_scale;
        let reward_unit = witness_report.reward_unit;
        let reward_shares = ((hex_scale * reward_unit) * *REWARD_SHARE_MULTIPLIER)
            .to_u32()
            .unwrap_or_default();

        if reward_shares == 0 && witness_invalid_reason == 0 {
            tracing::warn!(
                "valid witness with zero reward shares.
                pubkey: {},
                hex_scale: {}
                reward_unit: {}",
                witness_report.report.pub_key.clone(),
                witness_report.hex_scale,
                witness_report.reward_unit
            );
        }
        // NOTE: channel is irrelevant now
        let poc_witness = BlockchainPocWitnessV1 {
            gateway: witness_report.report.pub_key.into(),
            timestamp: witness_report.report.timestamp.timestamp() as u64,
            signal: witness_report.report.signal / SIGNAL_MULTIPLIER,
            packet_hash: witness_report.report.data,
            signature: witness_report.report.signature,
            snr: witness_report.report.snr as f32 / SNR_MULTIPLIER,
            frequency: hz_to_mhz(witness_report.report.frequency),
            datarate: witness_report.report.datarate.to_string(),
            channel: witness_invalid_reason,
            reward_shares,
            reward_unit: reward_unit.to_u32().unwrap_or(0),
            hex_scale: hex_scale.to_u32().unwrap_or(0),
        };

        poc_witnesses.push(poc_witness)
    }
    poc_witnesses
}

fn hz_to_mhz(freq_hz: u64) -> f32 {
    // MHZTOHZ = 1000000;
    let freq_mhz = Decimal::from(freq_hz) / Decimal::from(1000000);
    freq_mhz.to_f32().unwrap_or_default()
}

fn construct_poc_receipt(beacon_report: IotValidBeaconReport) -> (BlockchainPocReceiptV1, i64) {
    let hex_scale = beacon_report.hex_scale;
    let reward_unit = beacon_report.reward_unit;
    let reward_shares = ((hex_scale * reward_unit) * *REWARD_SHARE_MULTIPLIER)
        .to_u32()
        .unwrap_or_default();

    // NOTE: This timestamp will also be used as the top-level txn timestamp
    let beacon_received_ts = beacon_report.received_timestamp.timestamp_millis();

    // NOTE: signal, origin, snr and addr_hash are irrelevant now
    let poc_receipt = BlockchainPocReceiptV1 {
        gateway: beacon_report.report.pub_key.into(),
        timestamp: beacon_report.report.timestamp.timestamp() as u64,
        signal: 0,
        data: beacon_report.report.data,
        origin: 0,
        signature: beacon_report.report.signature,
        snr: 0.0,
        frequency: hz_to_mhz(beacon_report.report.frequency),
        channel: beacon_report.report.channel,
        datarate: beacon_report.report.datarate.to_string(),
        tx_power: beacon_report.report.tx_power,
        addr_hash: vec![],
        reward_shares,
        reward_unit: reward_unit.to_u32().unwrap_or(0),
        hex_scale: hex_scale.to_u32().unwrap_or(0),
    };

    (poc_receipt, beacon_received_ts)
}

fn hash_txn(txn: &BlockchainTxnPocReceiptsV2) -> (Vec<u8>, String) {
    let mut txn = txn.clone();
    txn.signature = vec![];
    let digest = Sha256::digest(txn.encode_to_vec()).to_vec();
    (
        digest.clone(),
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&digest),
    )
}

fn sign_txn(
    txn: &BlockchainTxnPocReceiptsV2,
    keypair: &Keypair,
) -> Result<Vec<u8>, Box<helium_crypto::Error>> {
    let mut txn = txn.clone();
    txn.signature = vec![];
    Ok(keypair.sign(&txn.encode_to_vec())?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use helium_crypto::{KeyTag, KeyType, Keypair, Network, Verify};
    use rust_decimal_macros::dec;

    #[test]
    fn txn_construction_test() {
        const ENTROPY: [u8; 32] = [
            248, 55, 78, 168, 99, 123, 22, 203, 36, 250, 136, 86, 110, 119, 198, 170, 248, 55, 78,
            168, 99, 123, 22, 203, 36, 250, 136, 86, 110, 119, 198, 170,
        ];
        let kt = KeyTag {
            network: Network::MainNet,
            key_type: KeyType::Ed25519,
        };
        let keypair = Keypair::generate_from_entropy(kt, &ENTROPY).expect("keypair");
        let pubkey = keypair.public_key();

        let poc_witness = BlockchainPocWitnessV1 {
            gateway: vec![],
            timestamp: 123,
            signal: 0,
            packet_hash: vec![],
            signature: vec![],
            snr: 0.0,
            frequency: 0.0,
            datarate: "dr".to_string(),
            channel: 0,
            reward_shares: 0,
            reward_unit: 2,
            hex_scale: 3,
        };
        let poc_receipt = BlockchainPocReceiptV1 {
            gateway: vec![],
            timestamp: 123,
            signal: 0,
            data: vec![],
            origin: 0,
            signature: vec![],
            snr: 0.0,
            frequency: 0.0,
            channel: 0,
            datarate: "dr".to_string(),
            tx_power: -1,
            addr_hash: vec![],
            reward_shares: 0,
            reward_unit: 1,
            hex_scale: 4,
        };
        let poc_path_element = BlockchainPocPathElementV1 {
            challengee: vec![],
            receipt: Some(poc_receipt),
            witnesses: vec![poc_witness],
        };
        let path = vec![poc_path_element];

        // txn0 will be signed over the path with the reward_unit and hex_scale
        let mut txn0 = BlockchainTxnPocReceiptsV2 {
            challenger: keypair.public_key().to_vec(),
            secret: vec![],
            onion_key_hash: vec![],
            path: path.clone(),
            fee: 0,
            signature: vec![],
            block_hash: vec![],
            timestamp: 456,
        };
        let signature0 = sign_txn(&txn0, &keypair).expect("unable to sign txn");
        txn0.signature = signature0.clone();
        let (_txn_hash, txn0_hash_b64_url) = hash_txn(&txn0);

        // txn1 signature will be stripped of the reward_unit and hex_scale
        let (txn1, _, txn1_hash_b64_url) = construct_signed_txn(path.clone(), 456, &keypair)
            .expect("unable to construct signed txn");

        // The txn hashes should be equal
        assert_eq!(txn0_hash_b64_url, txn1_hash_b64_url);

        let mut txn0 = txn0;
        txn0.signature = vec![];
        // This txn0 should be verifiable as is
        assert!(pubkey.verify(&txn0.encode_to_vec(), &signature0).is_ok());

        let signature1 = txn1.clone().signature;

        // The two signatures should be different
        assert_ne!(signature0, signature1);

        let mut txn1 = txn1;
        txn1.signature = vec![];
        // For txn1, we strip the path before verifying
        txn1.path = strip_path(path);
        assert!(pubkey.verify(&txn1.encode_to_vec(), &signature1).is_ok());
    }

    #[test]
    fn max_witnesses_per_receipt_test() {
        let poc_witness = BlockchainPocWitnessV1 {
            gateway: vec![],
            timestamp: 123,
            signal: 0,
            packet_hash: vec![],
            signature: vec![],
            snr: 0.0,
            frequency: 0.0,
            datarate: "dr".to_string(),
            channel: 0,
            reward_shares: 0,
            reward_unit: 0,
            hex_scale: 0,
        };
        let poc_id: Vec<u8> = vec![0];
        let max_witnesses_per_receipt = 14;

        let mut to_be_squished_witnesses = vec![poc_witness.clone(); 20];
        assert_eq!(20, to_be_squished_witnesses.len());
        maybe_squish_witnesses(
            &mut to_be_squished_witnesses,
            &poc_id,
            max_witnesses_per_receipt,
        );
        assert_eq!(14, to_be_squished_witnesses.len());
        let mut non_squished_witnesses = vec![poc_witness.clone(); 14];
        assert_eq!(14, non_squished_witnesses.len());
        maybe_squish_witnesses(
            &mut non_squished_witnesses,
            &poc_id,
            max_witnesses_per_receipt,
        );
        assert_eq!(14, non_squished_witnesses.len());
        let mut non_squished_witnesses2 = vec![poc_witness; 10];
        assert_eq!(10, non_squished_witnesses2.len());
        maybe_squish_witnesses(
            &mut non_squished_witnesses2,
            &poc_id,
            max_witnesses_per_receipt,
        );
        assert_eq!(10, non_squished_witnesses2.len());
    }

    #[test]
    fn reward_share_test() {
        // hex_scale: 0.9, reward_unit: 0.09, reward_shares: 810
        let hex_scale = dec!(0.9);
        let reward_unit = dec!(0.09);
        let dec_rs = hex_scale * reward_unit;
        assert_eq!(
            (dec_rs * *REWARD_SHARE_MULTIPLIER)
                .to_u32()
                .unwrap_or_default(),
            810
        );

        // hex_scale: 0.09, reward_unit: 0.09, reward_shares: 81
        let hex_scale = dec!(0.09);
        let reward_unit = dec!(0.09);
        let dec_rs = hex_scale * reward_unit;
        assert_eq!(
            (dec_rs * *REWARD_SHARE_MULTIPLIER)
                .to_u32()
                .unwrap_or_default(),
            81
        );

        // hex_scale: 0.009, reward_unit: 0.09, reward_shares: 8
        let hex_scale = dec!(0.009);
        let reward_unit = dec!(0.09);
        let dec_rs = hex_scale * reward_unit;
        assert_eq!(
            (dec_rs * *REWARD_SHARE_MULTIPLIER)
                .to_u32()
                .unwrap_or_default(),
            8
        );

        // hex_scale: 0.0009, reward_unit: 0.09, reward_shares: 0
        let hex_scale = dec!(0.0009);
        let reward_unit = dec!(0.09);
        let dec_rs = hex_scale * reward_unit;
        assert_eq!(
            (dec_rs * *REWARD_SHARE_MULTIPLIER)
                .to_u32()
                .unwrap_or_default(),
            0
        );

        // hex_scale: 0.1945, reward_unit: 0.0000, reward_shares: 0
        let hex_scale = dec!(0.1945);
        let reward_unit = dec!(0.0000);
        let dec_rs = hex_scale * reward_unit;
        assert_eq!(
            (dec_rs * *REWARD_SHARE_MULTIPLIER)
                .to_u32()
                .unwrap_or_default(),
            0
        );
    }
}
