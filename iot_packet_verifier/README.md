# IoT Packet Verifier 

The IoT Packet Verifier reads published packet reports and verifies that the
payer has a sufficient balance to pay for the packet. If it does, it burns the 
data credits on the Solana chain, tells the config server to enable the owner,
and writes a valid packet report to S3. If the payer's balance is insufficient, 
the verifier will tell the config server to disable the owner and writes an 
invalid packet report to S3.

## S3 Inputs

| File Type | Pattern | |
| :-- | :-- | :-- |
| PacketRouterPacketReportV1 | packetreport.* | [Proto](https://github.com/helium/proto/blob/master/src/service/packet_router.proto#L8) |

## S3 Outputs

| File Type | Pattern | |
| :-- | :-- | :-- |
| ValidPacket | valid_packet.* | [Proto](https://github.com/helium/proto/blob/master/src/service/packet_verifier.proto#L5) |
| InvalidPacket | invalid_packet.* | [Proto](https://github.com/helium/proto/blob/master/src/service/packet_verifier.proto#L11) |

## Details of operation 

Checking the balance of an owner on the Solana chain is cheap, but burning data
credits is quite expensive, and we are limited to burning the data credits of 
one payer every one second. Therefore, the verifier is split into three parts:

- An in-memory cache that contains the previously recorded balance of the payer.
  This cache allows us to quickly debit a payer and check their balance without
  having to burn their credits on chain each packet.
- A postgres database that contains the pending burn amounts for each payer. When
  a payer is debited, the amount is added to the database, ensuring that if the
  packet verifier crashes the state will be recoverable.
- A burner process that polls the database for a random payer that exceeds a certain
  amount of data credits for payment. This process issues a burn transaction to 
  the Solana chain and will remove that burned amount from the in-memory cache.
