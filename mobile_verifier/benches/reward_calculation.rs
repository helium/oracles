use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mobile_verifier::service_provider::reward::{ServiceProviderRewardInfos, RewardInfo};
use mobile_config::sub_dao_epoch_reward_info::EpochRewardInfo;
use rust_decimal_macros::dec;
use std::time::Duration;

fn create_test_data(num_service_providers: usize) -> ServiceProviderRewardInfos {
    let mut coll = Vec::with_capacity(num_service_providers);
    let total_sp_allocation = dec!(1000000);
    let reward_info = EpochRewardInfo::default();

    for i in 0..num_service_providers {
        let sp_id = i as i32;
        let bones = dec!(1000) * (i + 1) as u32;
        let promo_fund_perc = dec!(0.1);
        let promotions = vec![]; // Empty promotions for benchmark
        coll.push(RewardInfo::new(
            sp_id,
            bones,
            promo_fund_perc,
            total_sp_allocation,
            promotions,
        ));
    }

    ServiceProviderRewardInfos {
        coll,
        total_sp_allocation,
        reward_info,
    }
}

fn benchmark_reward_calculation(c: &mut Criterion) {
    let mut group = c.benchmark_group("reward_calculation");
    group.measurement_time(Duration::from_secs(10));

    // Test with different numbers of service providers
    for num_sp in [10, 100, 1000].iter() {
        group.bench_function(format!("{}_service_providers", num_sp), |b| {
            let reward_infos = create_test_data(*num_sp);
            b.iter(|| {
                black_box(reward_infos.iter_rewards());
            });
        });
    }

    group.finish();
}

criterion_group!(benches, benchmark_reward_calculation);
criterion_main!(benches); 