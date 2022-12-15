# IoT Config Service

The IoT Config Service provides configuration settings and values for the
LoRaWAN IoT Helium Subnetwork. Actors on the IoT subnetwork can interact
with the gRPC APIs provided by the Config service to perform various
operations of the network according to role, including but not limited to:

- Community Management (the Foundation) can issue network configuration variables
  (formerly known as Chain Variables) to adjust PoC settings
- Users of the network can manage their organization's routes
- Gateways can request their region and associated region parameters

The IoT Config service provides 4 major gRPC services:

## `route`

provides routing information for devices on the LoRaWAN network to correctly
route packets and the management of those routes by their controlling organizations

## `org`

management of organizations using the Helium LoRaWAN network

## `session key filter`

management of session key filters by organizations to decrypt device and other
data associated with usage of the network

## `gateway`

configuration data provided to LoRaWAN gateways serving the network, including
the current region parameters for the region in which the gateway is asserted
