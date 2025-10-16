pub mod bucket;
pub mod dump;
pub mod dump_mobile_rewards;
pub mod info;

pub(crate) fn print_json<T: ?Sized + serde::Serialize>(value: &T) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}
