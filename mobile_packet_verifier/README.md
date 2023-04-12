# Mobile Packet Verifier

The mobile packet verifier reads data transfer sessions and accumulates the amount
of bytes downloaded and uploaded. After a specified period, it burns a proportional
amount of data credits from the payer and issues validated data transfer sessions 
so that the mobile verifier may reward the hotspots. 

The mobile packet verifier assumes that all payers have sufficient balance.

## S3 Inputs 

| File Type | Pattern | |
| :-- | :-- | :-- |
| DataTransferSessionIngestReport | data_transfer_session_ingest_report.* | [Proto](https://github.com/helium/proto/blob/40388d260fd3603f453a965dbc13f79470b5adcb/src/service/poc_mobile.proto#L212) |

## S3 Outputs

| File Type | Pattern | |
| :-- | :-- | :-- |
| ValidDataTransferSession | valid_data_transfer_session.* | [Proto](https://github.com/helium/proto/blob/40388d260fd3603f453a965dbc13f79470b5adcb/src/service/packet_verifier.proto#L24) |

