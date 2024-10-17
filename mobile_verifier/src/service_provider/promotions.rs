use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::service_provider::ServiceProviderId;

mod proto {
    pub use helium_proto::{Promotion, ServiceProviderPromotion};
}

#[derive(Debug, Default)]
pub struct ServiceProviderPromotions(pub(crate) Vec<proto::ServiceProviderPromotion>);

impl ServiceProviderPromotions {
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

impl From<Vec<proto::ServiceProviderPromotion>> for ServiceProviderPromotions {
    fn from(value: Vec<proto::ServiceProviderPromotion>) -> Self {
        Self(value)
    }
}
