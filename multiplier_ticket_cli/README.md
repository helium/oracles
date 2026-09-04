# Multiplier Ticket CLI

Submits HIP-150 data transfer multiplier tickets to the mobile ingestor.

A ticket grants one hotspot a multiplier on the data credits its rewardable
bytes convert to. Both sides move together: the payer burns that many times the
data credits, and the hotspot's share of the mobile data pool grows in the same
proportion.

## Usage

```
multiplier-ticket-cli \
    --keypair ./issuer.bin \
    --ingest-url http://ingest:9080 \
    submit --hotspot <b58 pubkey> --multiplier 1.5 --message "why"
```

Without `--commit` the ticket is printed and nothing is sent:

```json
{
  "committed": false,
  "hotspot_pubkey": "14ApnkU9WdbAxn7dh5NYAfgHubJKwxT5F4yF1kKMrTPMfdRB2VJ",
  "message": "venue pilot",
  "multiplier": "1.5",
  "signed_timestamp": "2026-09-02T00:24:52.795Z",
  "signer": "14ApnkU9WdbAxn7dh5NYAfgHubJKwxT5F4yF1kKMrTPMfdRB2VJ"
}
```

Add `--commit` to send it. The output gains `"committed": true` and
`received_timestamp_ms`, which is the timestamp ingest stamped on it.

| Option | | |
| :-- | :-- | :-- |
| `--hotspot` | | Hotspot to grant the multiplier to, base58 |
| `--multiplier` | | 1 to 5 inclusive, up to 6 decimal places |
| `--message` | optional | Free text stored with the ticket |
| `--keypair` | `MULTIPLIER_TICKET_KEYPAIR` | Signing key, defaults to `./keypair.bin` |
| `--ingest-url` | `INGEST_URL` | Ingest gRPC endpoint |
| `--commit` | | Actually send it |

## Before you can submit

The signing key must be in the ingestor's `data_transfer_multiplier`
authorized keys. Anything else is refused at the gRPC boundary and leaves no
record anywhere.

## Things worth knowing

**Submitting is not reversible.** Every ticket that reaches ingest is written to
s3 and to `data_transfer.multiplier_ticket_history`, refusals included. There is
no delete — the only way back is another ticket, which lands on the record as a
second entry. Setting a multiplier of `1` is how a grant is revoked.

**A ticket takes effect from its signed timestamp**, not from when it is
processed, and it only applies to data that moved after that point. It starts
affecting burns once the packet verifier has processed the ticket file.

**The range is checked here as well as by the packet verifier.** The verifier is
what actually decides, but a ticket it refuses is refused *on the record*. The
local check keeps typos out of the audit log.

## Checking that it landed

The ticket is keyed on `(hotspot_pubkey, signed_timestamp)`, so both values from
the output above identify the row:

```sql
SELECT multiplier, status, received_timestamp, verified_timestamp
FROM data_transfer.multiplier_ticket_history
WHERE hotspot_pubkey = '<hotspot_pubkey>'
  AND signed_timestamp = TIMESTAMP '2026-09-02 00:24:52.795 UTC'
```

Note the timestamp changes shape between the two: the output prints
`2026-09-02T00:24:52.795Z`, Trino wants `2026-09-02 00:24:52.795 UTC`. Same
instant, and the milliseconds must be kept — the stored value has exactly that
precision and nothing more.

`status` says whether the packet verifier accepted it, and if not, why.
