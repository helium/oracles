pub mod generate;
pub mod server;

pub(crate) fn print_json(value: &serde_json::Value) {
    println!("{:#}", value);
}
