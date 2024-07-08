# Mobile Stack Testing

## Setup

### 1. Generate Data

**NOTE:** Data is auto-generated. If you do not wish to change it, skip these steps. The commands are here to show how the data is generated.

- Run `test-mobile assignment` and move the generated files[^files] to `docker/mobile/localstack/data/mobile-verifier-data-sets/`.
- Run `AWS_ACCESS_KEY_ID=X AWS_SECRET_ACCESS_KEY=X AWS_SESSION_TOKEN=X test-mobile price` and move the generated file to `docker/mobile/localstack/data/mobile-price/`. This can also be run when LocalStack is up and will upload files.

### 2. Build Docker Images

- Navigate to the `docker` directory: `cd docker/mobile`
- Build the Docker images: `docker compose build`

### 3. Run Tests

- Run the integration tests: `cargo test --package test-mobile --test integration_test -- --nocapture`

**NOTE:** The test will run `docker compose up` on start and `docker compose down -v` at the end (if the test is successful).

[^files]: Maps of hexes used 
![Hexes](docs/hexes.jpg "Hexes")

## Others

### Keypairs

- `13te9quF3s24VNrQmBRHnoNSwWPg48Jh2hfJdqFQoiFYiDcDAsp` Coverage object
- `14FGkBKPAdBuCtKGFkSnUmvoUBkJGjKVLrPrNLXKN3NgMiLTtwm` Mobile Verifier
- `14c5dZUZgFEVcocB3mfcjhXEVqDuafnpzghgyr2i422okXVByPr` Mobile Packer Verifier
- `131kC5gTPFfTyzziHbh2PWz2VSdF4gDvhoC5vqCz25N7LFtDocF` Mobile Config