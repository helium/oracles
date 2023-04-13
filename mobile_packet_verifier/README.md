# Mobile Packet Verifier

The mobile packet verifier reads data transfer sessions and accumulates the amount
of bytes downloaded and uploaded. After a specified period, it burns a proportional
amount of data credits from the payer and issues validated data transfer sessions 
so that the mobile verifier may reward the hotspots. 

The mobile packet verifier does not check the balance of the payer and write out 
invalid data transfer sessions if it fails to debit the balance. The assumption is 
that payers will always have data credits to pay. If they do not, the mobile packet
verifier will error out, and will fail to write out validated data transfer sessions 
for rewards.

## S3 Inputs 

| File Type | Pattern | |
| :-- | :-- | :-- |
| DataTransferSessionIngestReport | data_transfer_session_ingest_report.* | [Proto](https://github.com/helium/proto/blob/40388d260fd3603f453a965dbc13f79470b5adcb/src/service/poc_mobile.proto#L212) |

## S3 Outputs

| File Type | Pattern | |
| :-- | :-- | :-- |
| ValidDataTransferSession | valid_data_transfer_session.* | [Proto](https://github.com/helium/proto/blob/40388d260fd3603f453a965dbc13f79470b5adcb/src/service/packet_verifier.proto#L24) |

