# Reward Index

Maintains a running total of rewards per entity for the Helium Mobile Network.

## Iceberg Inputs

Each table below is streamed snapshot-by-snapshot; mobile-verifier's rewarder
appends one snapshot per table per epoch. Each table's epoch is recorded in the
`processed_writes` table in the same transaction as its rewards, so a snapshot
re-streamed after a crash is not counted twice.

| Table | Reward Type | Key |
| :--- | :-- | :-- |
| `rewards.data_transfer` | `mobile_gateway` | `hotspot_key` |
| `rewards.service_provider` | `mobile_service_provider` | `rewardable_entity_key` |
| `rewards.unallocated` | `mobile_unallocated` | `unallocated_reward_entity_key` setting |

Epochs starting before `iceberg.start_epoch_period` are skipped; they were
indexed from reward manifest files before reward-index read Iceberg.
