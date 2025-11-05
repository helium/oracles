use rust_decimal::Decimal;

pub fn get_scheduled_tokens(total_emission_pool: Decimal) -> Decimal {
    crate::reward_shares::get_scheduled_tokens_for_service_providers(total_emission_pool)
}
