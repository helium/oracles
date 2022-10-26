# Density Scaler

This crate is designed to run a separate threaded tokio process within other applications
in the Helium Network Oracle infrastructure and provide h3dex density scaling factors on
request to the managing service for calculating coverage rewards relative to the population
density of a gateway's hex and neighboring hexes per the algorithms adopted in HIPs 15 and 17.

## API

The density scaler provides an API for communicating between the scaler server and the managing
application based on the tokio flavor of mpsc and oneshot channels. Before starting the scaler,
generate the two ends of the connection between your process and the scaler server with
`density_scaler::query_channel()`, which returns the two ends of the connection as structs. Pass the
receiving end to the scaler when the process is started and send messages to it through the sending
end via the `density_scaler::QuerySender.query(hex: String)` method. Await the result in the form
of a `Result<Option<f64>>` the success case float indicating the value by which rewards for the
requested hex should be multiplied.

## Server

The density scaler server runs as a separate tokio threaded process within other Helium Oracles,
such as the PoC Verifier for the IoT network. When starting the managing application (likely within
the main function) you must first generate the message channel for making queries and store the
sending half (QuerySender) in your own server. Pass the receiving half of the channel (QueryReceiver)
to the density scaler's `Server::run` method alongside the shutdown trigger of your application.
The scaler server runs in a configurable loop, periodically querying the on-chain source of asserted
gateway location data and rebuilding the density scaling map globally each time. With each new version
of the scaling factor lookup table, the density scaler will continue to server responses to individual
hex scaling queries as well as write the contents of the scaling factor lookup table as an output
report alongside other oracles' reports.

## Configuration

Density scaler behavior is adjustable through the following environment variables:

- `FOLLOWER_URI`: The fully-qualified protocol, URL or IP and port of the gateway grpc location stream.
                  Defaults to `"http://127.0.0.1:8080"` when not defined in the environment.
- `DENSITY_TRIGGER_INTERVAL_SECS`: The duration in seconds between runs of the server loop to regenerate the scaling
                  factor map. Defaults to 1800 (30 min) when not defined in the environment.
- `DENSITY_GW_STREAM_BATCH_SIZE`: The size of batches of gateway location info to request from the source of on-chain
                  location data in the grpc response stream. Defaults to 1000 when not defined in the environment.
