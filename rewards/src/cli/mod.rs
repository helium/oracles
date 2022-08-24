pub mod generate;
pub mod server;
use crate::Result;

pub(crate) fn print_json<T: ?Sized + serde::Serialize>(value: &T) -> Result<String> {
    let to_print = serde_json::to_string_pretty(value)?;
    // println!("{}", to_print);
    Ok(to_print)
}
