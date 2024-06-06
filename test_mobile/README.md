# Mobile Stack Testing

## Setup

### 1. Generate data

**NOTE:** Data is auto-generated. If you do not wish to change it, skip these steps. The commands are here to show how the data is generated.

- Run `test-mobile assignment` and move the generated files[^files] to `docker/mobile/localstack/data/mobile-verifier-data-sets/`
- Run `AWS_ACCESS_KEY_ID=X AWS_SECRET_ACCESS_KEY=X AWS_SESSION_TOKEN=X test-mobile price` and move the generated file to `docker/mobile/localstack/data/mobile-price/`. This can also be ran when localstack is up and will uopload files. 

### 2. Build Docker images

- Navigate to the `docker` directory: `cd docker`
- Build the Docker images: `docker compose build`

### 3. Run tests

- Run the integration tests: `cargo test --package test-mobile --test integration_test -- --nocapture`

**NOTE:** The test will `docker compose up` on start and `docker compose stop` at the end. It is up to **you** to `docker compose down` if you want to clean up.

[^files]: Maps of hexes used 
![Hexes](docs/hexes.jpg "Hexes")