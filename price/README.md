# Price Oracle Server

The price oracle server serves up price data for helium token(s) acquired from
the [pyth.network](https://pyth.network) and stores it in an S3 bucket.

The supported tokens are:
- HNT
- HST
- MOBILE
- IOT

Note that currently only HNT-USD prices are available.

The price oracle server:

- Requests price for HNT token at a regular interval (60s) from pyth via solana
  RpcClient. In case of failure it uses the previously fetched price and stores
  the same with an updated timestamp.
- Stores and uploads [price_report](https://github.com/helium/proto/blob/master/src/price_report.proto) to an S3 bucket.
