# Mobile Verifier 

## S3 Inputs

| File Type | Pattern | |
| :--- | :-- | :-- |
| CellHeartbeatIngestReport | heartbeat_report.\* | [Proto](https://github.com/helium/proto/blob/149997d2a74e08679e56c2c892d7e46f2d0d1c46/src/service/poc_mobile.proto#L50) |
| CellSpeedtestIngestReport | speedtest_report.\* | [Proto](https://github.com/helium/proto/blob/149997d2a74e08679e56c2c892d7e46f2d0d1c46/src/service/poc_mobile.proto#L25) |
| CoverageObjectIngestReport | coverage_object_ingest_report.\* | [Proto](https://github.com/helium/proto/blob/9fc57133ed1e760c3f1b65dd22d55c09c84832da/src/service/poc_mobile.proto#L157) |

## Iceberg Inputs

| Table | Contents |
| :-- | :-- |
| data_transfer.burned_sessions | Burned data transfer sessions written by mobile-packet-verifier, read through Trino for the data transfer reward pool |

## Iceberg Outputs

| Table | Contents |
| :-- | :-- |
| hotspots.heartbeats / hotspots.invalid_heartbeats | Validated wifi heartbeats, valid and rejected |
| hotspots.speedtests / hotspots.invalid_speedtests | Validated speedtests, valid and rejected |
| hotspots.speedtest_avgs / hotspots.invalid_speedtest_avgs | Speedtest averages, valid and failing |
| hotspots.bans / hotspots.invalid_bans | Ban and unban reports, authorized and rejected |
| rewards.data_transfer | Per-hotspot data transfer rewards for each epoch |
| rewards.service_provider | Service provider rewards for each epoch |
| rewards.unallocated | Unallocated rewards for each epoch |

## Server 

The server runs every 24 hours and will verify rewards within the last 24 hours. 

The server accepts the following environmental variables for configuration: 

- `FOLLOWER_URI`: The URI for the follower service to connect to. 
- `INPUT_BUCKET_ENDPOINT`
- `INPUT_BUCKET_REGION`
- `INPUT_BUCKET` 

## Client 

The command line client accepts the following flags: 

- `--after`: Datetime to validate shares after.
- `--before`: Datetime to validate shares before.
- `--input_bucket`: Input S3 bucket.
- `--output_bucket`: Output S3 bucket.
