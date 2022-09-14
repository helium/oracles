# poc-iot-injector

- The poc_iot_injector server gathers up iot-verifier valid S3 files for beacon
and witness reports.
- Converts those S3 files to `blockchain_poc_core_v1` proto so that the
  blockchain can understand the `blockchain_poc_receipts_v2` transactions.
- Signs the transactions with the `injector_server_keypair`.
- Submits the transactions to the follower node as a fire and forget event.
