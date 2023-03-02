# Price Oracle Server

The price oracle server serves up price data for helium token(s) acquired from
the [pyth.network](https://pyth.network).

The supported tokens are:
- HNT
- HST
- MOBILE
- IOT

The price oracle server:

- Requests price for HNT token at a regular interval (60s) from pyth.
- Stores and uploads [price_report](TODO) to a bucket.
- TODO: Acquire price for MOBILE and IOT token(s) when available on pyth.
