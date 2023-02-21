# Ingest

## IOT

### S3 Inputs

| File Type | |
| :--- | :-- |
| LoraBeaconReportReqV1 | [Proto](https://github.com/helium/proto/blob/149997d2a74e08679e56c2c892d7e46f2d0d1c46/src/service/poc_lora.proto#L42) |
| LoraWitnessReportReqV1 | [Proto](https://github.com/helium/proto/blob/149997d2a74e08679e56c2c892d7e46f2d0d1c46/src/service/poc_lora.proto#L64) |

### S3 Outputs
| File Type | Pattern | |
| :--- | :-- | :-- |
| IotBeaconIngestReport | iot_beacon_ingest_report.\* | [Proto](https://github.com/helium/proto/blob/149997d2a74e08679e56c2c892d7e46f2d0d1c46/src/service/poc_lora.proto#L83) |
| IotWitnessIngestReport | iot_witness_ingest_report.\* | [Proto](https://github.com/helium/proto/blob/149997d2a74e08679e56c2c892d7e46f2d0d1c46/src/service/poc_lora.proto#L90) |

## Mobile

### S3 Inputs

| File Type | |
| :--- | :-- |
| SpeedtestReqV1 | [Proto](https://github.com/helium/proto/blob/149997d2a74e08679e56c2c892d7e46f2d0d1c46/src/service/poc_mobile.proto#L7) |
| CellHeartbeatReqV1 | [Proto](https://github.com/helium/proto/blob/149997d2a74e08679e56c2c892d7e46f2d0d1c46/src/service/poc_mobile.proto#L31) |
| DataTransferSessionReqV1 | [Proto](https://github.com/helium/proto/blob/149997d2a74e08679e56c2c892d7e46f2d0d1c46/src/service/poc_mobile.proto#L154) |

### S3 Outputs

| File Type | Pattern | |
| :--- | :-- | :-- |
| CellHeartbeatIngestReport | heartbeat_report.\* | [Proto](https://github.com/helium/proto/blob/149997d2a74e08679e56c2c892d7e46f2d0d1c46/src/service/poc_mobile.proto#L50) |
| CellSpeedtestIngestReport | speedtest_report.\* | [Proto](https://github.com/helium/proto/blob/149997d2a74e08679e56c2c892d7e46f2d0d1c46/src/service/poc_mobile.proto#L25) |
| DataTransferSessionIngestReport | data_transfer_session_ingest_report.\* | [Proto](https://github.com/helium/proto/blob/149997d2a74e08679e56c2c892d7e46f2d0d1c46/src/service/poc_mobile.proto#L177) |

