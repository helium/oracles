# Price Oracle Server

The price oracle server serves up HNT/USD price data acquired from the
[pyth.network](https://pyth.network) Hermes HTTP API and stores it in an S3
bucket.

The price oracle server:

- Requests the HNT price at a regular interval (60s) from the configured
  Hermes endpoint (`/v2/updates/price/latest?ids[]=<feed_id>`). In case of
  failure it uses the previously fetched price and stores the same with an
  updated timestamp.
- Stores and uploads [price_report](https://github.com/helium/proto/blob/master/src/price_report.proto) to an S3 bucket.
