//! The data types this service moves. One module per type: its records, its
//! input, its middleware, its output and the route that wires them together.
//!
//! A new type is a sibling module, a section on [`crate::Settings`] and a
//! route added in `cli::server`.

pub mod mobile_hotspot_history;
