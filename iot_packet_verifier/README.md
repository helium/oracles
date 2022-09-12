# IoT Packet Verifier

IoT Packet Verifier (PV) interacts with the
[Helium Packet Router](https://github.com/helium/helium-packet-router/) (HPR)
for the business side of LoRaWAN traffic accounting.

HPR reports each routable packet publicly, which persists in S3 permanently
and considered read-only thereafter.

PV reads those reports and similarly publishes bookkeeping records to S3
from which the Injector credits rewards to gateway owners for providing the
network.  It debits Data Credits (DC) of customers using the network.

The Config Server (CS) provides current customers' OUIs, NetIDs and their
associated wallet public address.  It also gets public keys of each gateway
and wallets of their owners.

PV notifies CS of OUIs for which HPR should cease routing when an associated
wallet contain an insufficient balance.  PV also monitors for wallets
becoming funded again.
