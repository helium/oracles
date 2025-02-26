# IoT Verifier

This service has three primarys tasks:

1.  Verify incoming POC reports.  Takes beacon, witness & entropy reports as inputs from an S3 bucket and verifies their integrity against a range of POC validations.  Resulting verified reports are outputted to another S3 bucket.  
2.  Verify incoming packets reports. Takes packet reports as inputs from an S3 bucket and verifies each as rewardable or non rewardable
3.  Calculate & distribute rewards.  Based on the results of the previous two tasks generate and assign rewards to participating gateways and output to S3.


## S3 Inputs

|                        |
|:-----------------------|
| IotBeaconIngestReport  |
| IotWitnessIngestReport |
| IotValidPacket         |
| EntropyReport          |

## POC Validations

The verifier will periodically query the incoming S3 bucket and process any new beacon, witness & entropy reports.  These will be inserted to the verifier DB.  The DB will then be periodically queried to retrieve any beacon reports with an expired entropy lifespan.  For each such beacon report a list of witnesses will be retrieved and both reports types will then be verified against the following POC related validations:

beacon reports
- `assertion check`: has the beaconing hotspot been asserted
- `denylist check`: is the beaconing hotspot on the denylist
- `valid entropy check`:  is the entropy included in the beacon report valid, 
- `capability check`: is the beaconing hotspot permitted to participate in POC
- `interval check`: is the beaconer permitted to beacon at the current time
- `entropy interval check`: was the beacon report received within the associated entropy's lifespan
- `data check`: does the reported broadcast data match that generated dynamically by the verifier

witness reports
- `assertion check`: has the witnessing hotspot been asserted
- `denylist check`: is the witnessing hotspot on the denylist
- `denylist edge check`: is the witnessing hotspot on a denied edge
- `self witness check`:  is the report a self witness
- `entropy interval check`: was the witness report received within the associated entropy's lifespan
- `lag check`:  was the report received within the permitted lag time of the first received report
- `packet check`: does the reported packet payload match that of the beaconers broadcast
- `capability check`: is the beaconing hotspot permitted to participate in POC
- `frequency check`: does the frequency of the witness report match that of the beaconers
- `region check`: is the witnessing hotspot located in the same region as the beaconer
- `cell distance check`: is the witnessing hotspot's hex beyond the min distance from the beaconer's hex
- `distance check`: is the witnessing hotspot beyond the min distance from the beaconer
- `rssi check`: is the RSSI of the witnessing hotspot valid ( based on free space path loss calc)




## S3 Outputs

| File Type               |
|:------------------------|
| IotPoc                  |
| IotInvalidBeaconReport  |
| IotInvalidWitnessReport |
| NonRewardablePacket     |
| IotRewardShare          |
| RewardManifest          |

## Levers to adjust should verifier be down for an extended period

The verifier by default is configured for continuous operation where it will keep current with incoming reports.  Should the verifier be down for an extended period, it may be desirable or necessary to tweak settings in order to enable the verifier to catch up to current without dropping any reports:

- `base_stale_period`: confirm this exceeds the look back period plus a buffer of ~10%
- `loader_window_max_lookback_age`: confirm this exceeds the look back period
- `poc_loader_window_width`: It is sometime desirable when loading historic data to do so in larger chunks.  Widening the window can result in more efficient loading of an extended backlog.  A sane setting would be between 15 mins to 1 hour depending on the lookback period

Be aware as the verifier catches up closer to current it will be necessary to reduce `poc_loader_window_width` back to its original value ( default 5 mins ).  The verifier's view of current is always equal to Now() - `poc_loader_window_width` * 4. As such in normal operation we want this setting's value to be lower rather than higher.    






