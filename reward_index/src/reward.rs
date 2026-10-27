#[derive(sqlx::Type, Debug, Clone, PartialEq, Eq, Hash)]
#[sqlx(type_name = "reward_type", rename_all = "snake_case")]
pub enum RewardType {
    MobileGateway,
    IotGateway,
    IotOperational,
    MobileSubscriber,
    MobileServiceProvider,
    MobileUnallocated,
    IotUnallocated,
    MobilePromotion,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RewardKey {
    pub key: String,
    pub reward_type: RewardType,
}
