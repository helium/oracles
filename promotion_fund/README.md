* Promotion Fund Server

## S3 Inputs

| File Type | Pattern | |
| :---- | :---- | :---- |
| ServiceProviderPromotionFundV1 | service_provider_promotion_fund.\* | [Proto](https://github.com/helium/proto/blob/map/subscriber-referral/src/service_provider.proto#L9) | 

## S3 Outpus

| File Type | Pattern | |
| :---- | :---- | :---- |
| ServiceProviderPromotionFundV1 | service_provider_promotion_fund.\* | [Proto](https://github.com/helium/proto/blob/map/subscriber-referral/src/service_provider.proto#L9) |


## Server

The server loads the latest Service Provider Promotion Funds from S3, and every `Settings.solana_check_interval` Promotion Allocation for each Service Provider in the [proto enum](https://github.com/helium/proto/blob/376765fe006051d6dcccf709def58e7ed291b845/src/service_provider.proto#L5). If the Basis Points returned are different from what is stored in S3, a new report is be report. 
