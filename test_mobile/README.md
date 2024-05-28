# Mobile Stack Testing

# Setup

## 1. Generate Mobile Verifier data

`../target/release/test-mobile assignment` and move files [^files] to `docker/mobile/localstack/data/mobile-verifier-data-sets`

## 2. Build Docker images

`cd docker && docker compose build`

[^files]: Maps of hexes used 

![Hexes](docs/hexes.png "Hexes")