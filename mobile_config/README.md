# Mobile Config Service

The Mobile Config Service provides configuration settings and values for the
CBRS Mobile Helium Subnetwork. Actors on the Mobile subnetwork can interact with
the gRPC APIs provided by the Config service to perform various operations of the
network according to role, including but not limited to:

- Community Management (the Foundation) can issue network configuration variables
- Oracles can request hotspot information from the Solana chain

The Mobile Config service provides 3 major gRPC services:

## `hotspot`

provides metadata information about hotspots stored on the Solana chain and used
for figuring hotspot interactions in PoC algorithms and reward calculations

## `router`

validate the eligibility of a given router public key to burn data credits on
behalf of the network when the router attempts to send mobile traffic across the
network. routers whose public keys are not registered to the config service but
attempt to send data traffic are denied burn authority

## `admin`

administrative apis for managing auth keys and other service-wide settings
