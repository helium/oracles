# Entropy Server

The entropy server serves up entropy data for hotspot beacons to ensure that
hotspots are online and avoid scripted replay attacks.

It is used primarily by the IoT hotspot PoC mechanism.

The entropy server

- Generates entropy on a regular interval (60s). The entropy can be sourced from
  any secure, reliable online source. The initial implementation relies on a
  Solana JSON-RPC source over TLS to collect Solana block hashes as entropy.
- Stores and uploads generated entropy to a bucket for use by verifier(s)
- Serves it up over http

Note that TLS termination and CDN use is required to expose the server publicly.

## Configuration

The following environment variables are used by the server:

| Name            | Value                                     |
| --------------- | ----------------------------------------- |
| API_SOCKET_ADDR | Listen address for http api               |
| BUCKET          | Bucket to send entropy values to          |
| ENTROPY_STORE   | Absolute path for temporary entropy store |
| ENTROPY_URL     | URL for entropy request                   |
