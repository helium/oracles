# poc-iot-injector

- The poc_iot_injector server gathers up iot-verifier valid S3 files for beacon
and witness reports.
- Converts those S3 files to `blockchain_poc_core_v1` proto so that the
  blockchain can understand the `blockchain_poc_receipts_v2` transactions.
- Signs the transactions with the `injector_server_keypair`.
- Submits the transactions to the follower node as a fire and forget event.

## S3 Inputs

| File Type | Pattern | |
| :--- | :-- | :-- |
| IotPoc | iot_poc.\* | [Proto](https://github.com/helium/proto/blob/149997d2a74e08679e56c2c892d7e46f2d0d1c46/src/service/poc_lora.proto#L162) |

