//! Conversion into and from bytes.

use bytes::Bytes;

#[cfg(feature = "flexbuffers")]
pub mod flexbuffers;
#[cfg(feature = "prost")]
pub mod prost;
#[cfg(feature = "serde_json")]
pub mod serde_json;

/// Fallibly serialize a value to bytes. Almost like `TryInto<Vec<u8>>`, except for borrowing
/// instead of consuming, but defined here to allow for blanket implementations, e.g. for any type
/// implementing `Serialize`.
pub trait TryIntoBytes {
    type Error: std::error::Error + Send + Sync + 'static;

    fn try_into_bytes(&self) -> Result<Bytes, Self::Error>;
}

/// Fallibly construct a value from bytes. Like `TryFrom<Vec<u8>>`, but defined here to allow for
/// blanket implementations, e.g. for any type implementing `DeserializeOwned`.
pub trait TryFromBytes: Sized {
    type Error: std::error::Error + Send + Sync + 'static;

    fn try_from_bytes(bytes: Bytes) -> Result<Self, Self::Error>;
}
