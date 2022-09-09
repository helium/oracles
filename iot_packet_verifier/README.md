# IoT Packet Verifier

This interacts with the
[Helium Packet Router](https://github.com/helium/helium-packet-router/) (HPR)
for the business side of LoRaWAN traffic accounting.

HPR reports each routable packet publicly, which persists in S3 permanently
and considered read-only thereafter.

This component, the IoT Packet Verifier (PV), works in conjunction with the
Payment Server (PS).  Both interact with the Config Server (CS).

PV publishes publicly auditable bookkeeping records to S3 from which PS
credits rewards to gateway owners for providing the network. It debits Data
Credits (DC) of customers using the network.

CS provides PV and PS a customer list containing NetID-to-OUI mappings.  PS
then notifies CS of OUIs for which HPR should cease routing when an
associated wallet contain an insufficient balance per common accounting
practices.
