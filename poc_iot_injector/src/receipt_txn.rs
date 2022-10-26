use crate::{keypair::Keypair, Error, Result};
use helium_crypto::Sign;
use helium_proto::{
    services::poc_lora::{
        LoraBeaconReportReqV1, LoraValidBeaconReportV1, LoraValidPocV1, LoraValidWitnessReportV1,
        LoraWitnessReportReqV1,
    },
    BlockchainPocPathElementV1, BlockchainPocReceiptV1, BlockchainPocWitnessV1,
    BlockchainTxnPocReceiptsV2, Message,
};
use rust_decimal::{prelude::ToPrimitive, Decimal, MathematicalOps};
use rust_decimal_macros::dec;
use sha2::{Digest, Sha256};
use std::sync::Arc;

// TODO: These should ideally be coming in from some configuration service
const WITNESS_REDUNDANCY: u32 = 4;
const POC_REWARD_DECAY_RATE: Decimal = dec!(0.8);
const HIP15_TX_REWARD_UNIT_CAP: Decimal = dec!(2.0);

type PocPath = Vec<BlockchainPocPathElementV1>;

pub fn handle_report_msg(
    msg: prost::bytes::BytesMut,
    keypair: Arc<Keypair>,
    timestamp: i64,
) -> Result<Option<(BlockchainTxnPocReceiptsV2, Vec<u8>, String)>> {
    // Path is always single element, till we decide to change it at some point.
    let mut path: PocPath = Vec::with_capacity(1);

    let LoraValidPocV1 {
        poc_id: _poc_id,
        beacon_report,
        witness_reports,
    } = LoraValidPocV1::decode(msg)?;

    let poc_witnesses = construct_poc_witnesses(witness_reports)?;

    // We expect poc_receipt to always be here, once it is in a report
    if let Some(poc_receipt) = construct_poc_receipt(beacon_report, poc_witnesses.len() as u32)? {
        // TODO: Double check whether the gateway in the poc_receipt is challengee?
        let path_element =
            construct_path_element(poc_receipt.clone().gateway, poc_receipt, poc_witnesses)?;

        path.push(path_element);

        Ok(Some(construct_txn(path, timestamp, &keypair)?))
    } else {
        Ok(None)
    }
}

fn construct_txn(
    path: PocPath,
    timestamp: i64,
    keypair: &Keypair,
) -> Result<(BlockchainTxnPocReceiptsV2, Vec<u8>, String)> {
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
) -> Result<BlockchainPocPathElementV1> {
    let path_element = BlockchainPocPathElementV1 {
        challengee,
        receipt: Some(poc_receipt),
        witnesses: poc_witnesses,
    };
    Ok(path_element)
}

fn construct_poc_witnesses(
    witness_reports: Vec<LoraValidWitnessReportV1>,
) -> Result<Vec<BlockchainPocWitnessV1>> {
    let num_witnesses = witness_reports.len();
    let mut poc_witnesses: Vec<BlockchainPocWitnessV1> = Vec::with_capacity(num_witnesses);
    for witness_report in witness_reports {
        let LoraValidWitnessReportV1 {
            hex_scale: witness_hex_scale,
            report: witness_report_req,
            ..
        } = witness_report;

        if let Some(witness_report_req) = witness_report_req {
            let LoraWitnessReportReqV1 {
                pub_key: witness_pub_key,
                data: witness_packet,
                timestamp: witness_timestamp,
                signal: witness_signal,
                snr: witness_snr,
                frequency: witness_frequency,
                datarate: witness_datarate,
                signature: witness_signature,
                ..
            } = witness_report_req;

            let witness_hex_scale =
                Decimal::from_f32_retain(witness_hex_scale).unwrap_or_else(|| dec!(1.0));
            let reward_unit = poc_challengee_reward_unit(num_witnesses as u32)?;
            let reward_shares = witness_hex_scale * reward_unit;
            let reward_shares = reward_shares.to_u32().unwrap_or_default();

            // NOTE: channel is irrelevant now
            let poc_witness = BlockchainPocWitnessV1 {
                gateway: witness_pub_key,
                timestamp: witness_timestamp,
                signal: witness_signal,
                packet_hash: witness_packet,
                signature: witness_signature,
                snr: witness_snr as f32,
                frequency: hz_to_mhz(witness_frequency),
                datarate: witness_datarate.to_string(),
                channel: 0,
                reward_shares,
            };

            poc_witnesses.push(poc_witness)
        }
    }
    Ok(poc_witnesses)
}

fn hz_to_mhz(freq_hz: u64) -> f32 {
    // MHZTOHZ = 1000000;
    let freq_mhz = Decimal::from(freq_hz) / Decimal::from(1000000);
    freq_mhz.to_f32().unwrap_or_default()
}

fn construct_poc_receipt(
    beacon_report: Option<LoraValidBeaconReportV1>,
    num_witnesses: u32,
) -> Result<Option<BlockchainPocReceiptV1>> {
    if let Some(beacon_report) = beacon_report {
        let LoraValidBeaconReportV1 {
            hex_scale: beacon_hex_scale,
            report: beacon_report_req,
            ..
        } = beacon_report;

        if let Some(beacon_report_req) = beacon_report_req {
            let LoraBeaconReportReqV1 {
                pub_key: beacon_pub_key,
                data: beacon_data,
                frequency: beacon_frequency,
                channel: beacon_channel,
                datarate: beacon_datarate,
                tx_power: beacon_tx_power,
                timestamp: beacon_timestamp,
                signature: beacon_signature,
                ..
            } = beacon_report_req;

            let beacon_hex_scale =
                Decimal::from_f32_retain(beacon_hex_scale).unwrap_or_else(|| dec!(1.0));
            let reward_unit = poc_challengee_reward_unit(num_witnesses)?;
            let reward_shares = beacon_hex_scale * reward_unit;
            let reward_shares = reward_shares.to_u32().unwrap_or_default();

            // NOTE: signal, origin, snr and addr_hash are irrelevant now
            Ok(Some(BlockchainPocReceiptV1 {
                gateway: beacon_pub_key,
                timestamp: beacon_timestamp,
                signal: 0,
                data: beacon_data,
                origin: 0,
                signature: beacon_signature,
                snr: 0.0,
                frequency: hz_to_mhz(beacon_frequency),
                channel: beacon_channel,
                datarate: beacon_datarate.to_string(),
                tx_power: beacon_tx_power,
                addr_hash: vec![],
                reward_shares,
            }))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

fn hash_txn(txn: &BlockchainTxnPocReceiptsV2) -> (Vec<u8>, String) {
    let mut txn = txn.clone();
    txn.signature = vec![];
    let digest = Sha256::digest(&txn.encode_to_vec()).to_vec();
    (
        digest.clone(),
        base64::encode_config(&digest, base64::URL_SAFE_NO_PAD),
    )
}

fn sign_txn(txn: &BlockchainTxnPocReceiptsV2, keypair: &Keypair) -> Result<Vec<u8>> {
    let mut txn = txn.clone();
    txn.signature = vec![];
    Ok(keypair.sign(&txn.encode_to_vec())?)
}

// TODO: Arguably this functionality should live in the poc-iot-verifier.
// Once the hex_scale calculations are done, poc-iot-verifier should also attach the actual reward
// share in the generated report.
fn poc_challengee_reward_unit(num_witnesses: u32) -> Result<Decimal> {
    if num_witnesses == 0 {
        Err(Error::ZeroWitnesses)
    } else if num_witnesses < WITNESS_REDUNDANCY {
        Ok(Decimal::from(WITNESS_REDUNDANCY / num_witnesses))
    } else {
        let exp = WITNESS_REDUNDANCY - num_witnesses;
        if let Some(to_sub) = POC_REWARD_DECAY_RATE.checked_powu(exp as u64) {
            let unnormalized = dec!(2.0) - to_sub;
            let normalized_unit = normalize_reward_unit(unnormalized);
            Ok(normalized_unit)
        } else {
            Err(Error::InvalidExponent(exp.to_string()))
        }
    }
}

fn normalize_reward_unit(unnormalized: Decimal) -> Decimal {
    if unnormalized >= HIP15_TX_REWARD_UNIT_CAP {
        HIP15_TX_REWARD_UNIT_CAP
    } else {
        unnormalized
    }
}
