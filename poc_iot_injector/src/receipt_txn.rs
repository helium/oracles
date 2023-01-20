use file_store::{
    iot_valid_poc::{IotValidBeaconReport, IotValidPoc, IotValidWitnessReport},
    traits::MsgDecode,
};
use helium_crypto::{Keypair, Sign};
use helium_proto::{
    blockchain_txn::Txn, BlockchainPocPathElementV1, BlockchainPocReceiptV1,
    BlockchainPocWitnessV1, BlockchainTxn, BlockchainTxnPocReceiptsV2, Message,
};
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
use rust_decimal::{prelude::ToPrimitive, Decimal};
use sha2::{Digest, Sha256};
use std::sync::Arc;

const REWARD_SHARE_MULTIPLIER: Decimal = Decimal::ONE_HUNDRED;
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
    #[error("zero rewards_shares")]
    ZeroRewards,
    #[error("zero witnesses")]
    ZeroWitnesses,
}

pub fn handle_report_msg(
    msg: prost::bytes::BytesMut,
    keypair: Arc<Keypair>,
    max_witnesses_per_receipt: u64,
) -> Result<TxnDetails, TxnConstructionError> {
    // Path is always single element, till we decide to change it at some point.
    let mut path: PocPath = Vec::with_capacity(1);
    let iot_valid_poc = IotValidPoc::decode(msg)?;

    let mut poc_witnesses = construct_poc_witnesses(iot_valid_poc.witness_reports);
    maybe_squish_witnesses(
        &mut poc_witnesses,
        &iot_valid_poc.poc_id,
        max_witnesses_per_receipt as usize,
    );

    let (poc_receipt, beacon_received_ts) = construct_poc_receipt(iot_valid_poc.beacon_report)?;

    // TODO: Double check whether the gateway in the poc_receipt is challengee?
    let path_element =
        construct_path_element(poc_receipt.clone().gateway, poc_receipt, poc_witnesses)?;

    path.push(path_element);

    let (bare_txn, hash, hash_b64_url) = construct_bare_txn(path, beacon_received_ts, &keypair)?;
    Ok(TxnDetails {
        txn: wrap_txn(bare_txn),
        hash,
        hash_b64_url,
    })
}

/// Maybe squish poc_witnesses if the length is > max_witnesses_per_receipt
fn maybe_squish_witnesses(
    poc_witnesses: &mut Vec<BlockchainPocWitnessV1>,
    poc_id: &Vec<u8>,
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

fn construct_bare_txn(
    path: PocPath,
    timestamp: i64,
    keypair: &Keypair,
) -> Result<(BlockchainTxnPocReceiptsV2, Vec<u8>, String), TxnConstructionError> {
    let mut txn = BlockchainTxnPocReceiptsV2 {
        challenger: keypair.public_key().to_vec(),
        secret: vec![],
        onion_key_hash: vec![],
        path,
        fee: 0,
        signature: vec![],
        block_hash: vec![],
        timestamp: timestamp as u64,
    };

    txn.signature = sign_txn(&txn, keypair)?;
    let (txn_hash, txn_hash_b64url) = hash_txn(&txn);
    Ok((txn, txn_hash, txn_hash_b64url))
}

fn construct_path_element(
    challengee: Vec<u8>,
    poc_receipt: BlockchainPocReceiptV1,
    poc_witnesses: Vec<BlockchainPocWitnessV1>,
) -> Result<BlockchainPocPathElementV1, TxnConstructionError> {
    if poc_witnesses.is_empty() {
        return Err(TxnConstructionError::ZeroWitnesses);
    }
    Ok(BlockchainPocPathElementV1 {
        challengee,
        receipt: Some(poc_receipt),
        witnesses: poc_witnesses,
    })
}

fn construct_poc_witnesses(
    witness_reports: Vec<IotValidWitnessReport>,
) -> Vec<BlockchainPocWitnessV1> {
    let mut poc_witnesses: Vec<BlockchainPocWitnessV1> = Vec::with_capacity(witness_reports.len());
    for witness_report in witness_reports {
        let reward_shares = ((witness_report.hex_scale * witness_report.reward_unit)
            * REWARD_SHARE_MULTIPLIER)
            .to_u32()
            .unwrap_or_default();

        if reward_shares > 0 {
            // NOTE: channel is irrelevant now
            let poc_witness = BlockchainPocWitnessV1 {
                gateway: witness_report.report.pub_key.into(),
                timestamp: witness_report.report.timestamp.timestamp() as u64,
                signal: witness_report.report.signal,
                packet_hash: witness_report.report.data,
                signature: witness_report.report.signature,
                snr: witness_report.report.snr as f32,
                frequency: hz_to_mhz(witness_report.report.frequency),
                datarate: witness_report.report.datarate.to_string(),
                channel: 0,
                reward_shares,
            };

            poc_witnesses.push(poc_witness)
        }
    }
    poc_witnesses
}

fn hz_to_mhz(freq_hz: u64) -> f32 {
    // MHZTOHZ = 1000000;
    let freq_mhz = Decimal::from(freq_hz) / Decimal::from(1000000);
    freq_mhz.to_f32().unwrap_or_default()
}

fn construct_poc_receipt(
    beacon_report: IotValidBeaconReport,
) -> Result<(BlockchainPocReceiptV1, i64), TxnConstructionError> {
    let reward_shares = ((beacon_report.hex_scale * beacon_report.reward_unit)
        * REWARD_SHARE_MULTIPLIER)
        .to_u32()
        .unwrap_or_default();
    if reward_shares == 0 {
        return Err(TxnConstructionError::ZeroRewards);
    }

    // NOTE: This timestamp will also be used as the top-level txn timestamp
    let beacon_received_ts = beacon_report.received_timestamp.timestamp_millis();

    // NOTE: signal, origin, snr and addr_hash are irrelevant now
    Ok((
        BlockchainPocReceiptV1 {
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
        },
        beacon_received_ts,
    ))
}

fn hash_txn(txn: &BlockchainTxnPocReceiptsV2) -> (Vec<u8>, String) {
    let mut txn = txn.clone();
    txn.signature = vec![];
    let digest = Sha256::digest(txn.encode_to_vec()).to_vec();
    (
        digest.clone(),
        base64::encode_config(&digest, base64::URL_SAFE_NO_PAD),
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
    use rust_decimal_macros::dec;

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
        // dec_rs: 1.26932270, hex_scale: 0.7090, reward_unit: 1.7903
        let hex_scale = dec!(0.7090);
        let reward_unit = dec!(1.7903);
        let dec_rs = hex_scale * reward_unit;
        assert_eq!(
            (dec_rs * REWARD_SHARE_MULTIPLIER)
                .to_u32()
                .unwrap_or_default(),
            126
        );

        // dec_rs: 0, hex_scale: 0.1945, reward_unit: 0.0000
        let hex_scale = dec!(0.1945);
        let reward_unit = dec!(0.0000);
        let dec_rs = hex_scale * reward_unit;
        assert_eq!(
            (dec_rs * REWARD_SHARE_MULTIPLIER)
                .to_u32()
                .unwrap_or_default(),
            0
        );

        // dec_rs: 0, hex_scale: 0.09, reward_unit: 0.09
        let hex_scale = dec!(0.09);
        let reward_unit = dec!(0.09);
        let dec_rs = hex_scale * reward_unit;
        assert_eq!(
            (dec_rs * REWARD_SHARE_MULTIPLIER)
                .to_u32()
                .unwrap_or_default(),
            0
        );
    }
}
