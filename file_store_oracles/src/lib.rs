// Infrastructure
pub mod file_type;
pub mod traits;

// Cross-network shared types
pub mod network_common;
pub use network_common::*;

// Network-specific types
pub mod iot;
pub use iot::*;

pub mod mobile;
pub use mobile::*;

pub mod subscriber;
pub use subscriber::*;

// Re-exports
pub use file_type::FileType;

/// Useful in TryFrom implementations when going from a proto field to a rust
/// type that uses the proto enum as a member. It can automatically convert from
/// the underying i32 to the enum type or fallback to a provided error type.
pub fn prost_enum<Enum, Op, Err>(value: i32, map_err: Op) -> Result<Enum, Err>
where
    Enum: TryFrom<i32, Error = prost::UnknownEnumValue>,
    Op: FnOnce(prost::UnknownEnumValue) -> Err,
{
    Enum::try_from(value).map_err(map_err)
}
