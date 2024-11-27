use std::sync::Arc;

use chrono::{DateTime, Utc};
use mobile_config::client::carrier_service_client::CarrierServiceVerifier;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::service_provider::ServiceProviderId;

mod proto {
    pub use helium_proto::{service_provider_promotions::Promotion, ServiceProviderPromotions};
}

pub async fn get_promotions(
    client: Arc<dyn CarrierServiceVerifier>,
    epoch_start: &DateTime<Utc>,
) -> anyhow::Result<ServiceProviderPromotions> {
    let promos = client.list_incentive_promotions(epoch_start).await?;
    Ok(ServiceProviderPromotions(promos))
}

#[derive(Debug, Default, Clone)]
pub struct ServiceProviderPromotions(Vec<proto::ServiceProviderPromotions>);

impl ServiceProviderPromotions {
    pub fn into_proto(self) -> Vec<proto::ServiceProviderPromotions> {
        self.0
    }

    pub(crate) fn get_fund_percent(&self, sp_id: ServiceProviderId) -> Decimal {
        for promo in &self.0 {
            if promo.service_provider == sp_id {
                return Decimal::from(promo.incentive_escrow_fund_bps) / dec!(10_000);
            }
        }

        dec!(0)
    }

    pub(crate) fn get_active_promotions(&self, sp_id: ServiceProviderId) -> Vec<proto::Promotion> {
        for promo in &self.0 {
            if promo.service_provider == sp_id {
                return promo.promotions.clone();
            }
        }

        vec![]
    }
}

impl From<Vec<proto::ServiceProviderPromotions>> for ServiceProviderPromotions {
    fn from(value: Vec<proto::ServiceProviderPromotions>) -> Self {
        Self(value)
    }
}
