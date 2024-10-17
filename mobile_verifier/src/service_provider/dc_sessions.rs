use std::{collections::HashMap, ops::Range};

use chrono::{DateTime, Utc};
use mobile_config::client::{carrier_service_client::CarrierServiceVerifier, ClientError};
use rust_decimal::{Decimal, RoundingStrategy};
use sqlx::PgPool;

use crate::{
    data_session,
    reward_shares::{dc_to_mobile_bones, DEFAULT_PREC},
};

use super::ServiceProviderId;

pub async fn fetch_dc_sessions(
    pool: &PgPool,
    carrier_client: &impl CarrierServiceVerifier<Error = ClientError>,
    reward_period: &Range<DateTime<Utc>>,
) -> anyhow::Result<ServiceProviderDCSessions> {
    let payer_dc_sessions =
        data_session::sum_data_sessions_to_dc_by_payer(pool, reward_period).await?;

    let mut dc_sessions = ServiceProviderDCSessions::default();
    for (payer_key, dc_amount) in payer_dc_sessions {
        let service_provider = carrier_client
            .payer_key_to_service_provider(&payer_key)
            .await?;
        dc_sessions.insert(
            service_provider as ServiceProviderId,
            Decimal::from(dc_amount),
        );
    }

    Ok(dc_sessions)
}

#[derive(Debug, Default)]
pub struct ServiceProviderDCSessions(pub(crate) HashMap<ServiceProviderId, Decimal>);

impl ServiceProviderDCSessions {
    pub fn insert(&mut self, service_provider: ServiceProviderId, dc: Decimal) {
        *self.0.entry(service_provider).or_insert(Decimal::ZERO) += dc;
    }

    pub fn all_transfer(&self) -> Decimal {
        self.0.values().sum()
    }

    pub fn iter(&self) -> impl Iterator<Item = (ServiceProviderId, Decimal)> + '_ {
        self.0.iter().map(|(k, v)| (*k, *v))
    }

    pub fn rewards_per_share(
        &self,
        total_sp_rewards: Decimal,
        mobile_bone_price: Decimal,
    ) -> anyhow::Result<Decimal> {
        // the total amount of DC spent across all service providers
        let total_sp_dc = self.all_transfer();
        // the total amount of service provider rewards in bones based on the spent DC
        let total_sp_rewards_used = dc_to_mobile_bones(total_sp_dc, mobile_bone_price);
        // cap the service provider rewards if used > pool total
        let capped_sp_rewards_used =
            Self::maybe_cap_service_provider_rewards(total_sp_rewards_used, total_sp_rewards);
        Ok(Self::calc_rewards_per_share(
            capped_sp_rewards_used,
            total_sp_dc,
        ))
    }

    fn maybe_cap_service_provider_rewards(
        total_sp_rewards_used: Decimal,
        total_sp_rewards: Decimal,
    ) -> Decimal {
        match total_sp_rewards_used <= total_sp_rewards {
            true => total_sp_rewards_used,
            false => total_sp_rewards,
        }
    }

    fn calc_rewards_per_share(total_rewards: Decimal, total_shares: Decimal) -> Decimal {
        if total_shares > Decimal::ZERO {
            (total_rewards / total_shares)
                .round_dp_with_strategy(DEFAULT_PREC, RoundingStrategy::MidpointNearestEven)
        } else {
            Decimal::ZERO
        }
    }
}

impl<F, I> From<F> for ServiceProviderDCSessions
where
    F: IntoIterator<Item = (I, Decimal)>,
    I: Into<ServiceProviderId>,
{
    fn from(iter: F) -> Self {
        let mut me = Self::default();
        for (k, v) in iter {
            me.insert(k.into(), v);
        }
        me
    }
}

#[cfg(test)]
pub mod tests {

    use chrono::Duration;
    use helium_proto::{ServiceProvider, ServiceProviderPromotion};

    use crate::data_session::HotspotDataSession;

    use super::*;

    impl ServiceProviderDCSessions {
        fn len(&self) -> usize {
            self.0.len()
        }
    }

    #[sqlx::test]
    fn multiple_payer_keys_accumulate_to_service_provider(pool: PgPool) -> anyhow::Result<()> {
        // Client always resolves to single service provider no matter payer key
        struct MockClient;

        #[async_trait::async_trait]
        impl CarrierServiceVerifier for MockClient {
            type Error = ClientError;

            async fn payer_key_to_service_provider<'a>(
                &self,
                _pubkey: &str,
            ) -> Result<ServiceProvider, ClientError> {
                Ok(ServiceProvider::HeliumMobile)
            }
        }

        // Save multiple data sessions with different payers
        let one = HotspotDataSession {
            pub_key: vec![0].into(),
            payer: vec![0].into(),
            upload_bytes: 1_000,
            download_bytes: 1_000,
            num_dcs: 2_000,
            received_timestamp: Utc::now(),
        };
        let two = HotspotDataSession {
            pub_key: vec![1].into(),
            payer: vec![1].into(),
            upload_bytes: 1_000,
            download_bytes: 1_000,
            num_dcs: 2_000,
            received_timestamp: Utc::now(),
        };
        let mut txn = pool.begin().await?;
        one.save(&mut txn).await?;
        two.save(&mut txn).await?;
        txn.commit().await?;

        let now = Utc::now();
        let epoch = now - Duration::hours(24)..now;

        // dc sessions should represent single payer, and all dc is combined
        let map = fetch_dc_sessions(&pool, &MockClient, &epoch).await?;
        assert_eq!(map.len(), 1);
        assert_eq!(map.all_transfer(), Decimal::from(4_000));

        Ok(())
    }
}
