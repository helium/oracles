# Mobile Verifier 

This crates provides a command line utility and server that validates shares within an S3 bucket. 

Upon completion of a given time range, the mobile verifier will write the following files to the 
given output bucket: 

- `file_list`: List of files processed during verification.
- `shares`: List of validated shares with their rewarded bones (`weight`).
- `speed_shares`: List of validated speedtest shares. 
- `invalid_shares`: List of invalid shares. 
- `invalid_speed_shares`: List of invalid speedtest shares. 
- `speed_shares_moving_avg`
- `cell_shares`
- `hotspot_shares`
- `owner_shares`
- `missing_owner_shares`
- `owner_emissions` 

## Server 

The server runs every 24 hours and will verify rewards within the last 24 hours. 

The server accepts the following environmental variables for configuration: 

- `FOLLOWER_URI`: The URI for the follower service to connect to. 
- `INPUT_BUCKET_ENDPOINT`
- `INPUT_BUCKET_REGION`
- `INPUT_BUCKET` 
- `OUTPUT_BUCKET_ENDPOINT`
- `OUTPUT_BUCKET_REGION`
- `OUTPUT_BUCKET`

## Client 

The command line client accepts the following flags: 

- `--after`: Datetime to validate shares after.
- `--before`: Datetime to validate shares before.
- `--input_bucket`: Input S3 bucket.
- `--output_bucket`: Output S3 bucket.
