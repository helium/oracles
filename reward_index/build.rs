fn main() {
    prost_build::compile_protos(
        &["tests/fixtures/subscriber_reward_old.proto"],
        &["tests/fixtures"],
    )
    .unwrap();
}
