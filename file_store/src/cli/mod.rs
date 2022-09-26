pub mod bucket;
pub mod dump;
pub mod info;

use crate::Result;

pub(crate) fn print_json<T: ?Sized + serde::Serialize>(value: &T) -> Result {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}
