# Mobile Stack Testing

# Setup

## 1. Generate data

`test-mobile assignment` and move generated files [^files] to `docker/mobile/localstack/data/mobile-verifier-data-sets/`

`test-mobile price` and move generated file to `docker/mobile/localstack/data/mobile-price/`

## 2. Build Docker images

`cd docker && docker compose build`

[^files]: Maps of hexes used 

![Hexes](docs/hexes.png "Hexes")