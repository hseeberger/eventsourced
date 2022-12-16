//! Serialization to and deserialization from bytes.

#[cfg(feature = "flexbuffers")]
pub mod flexbuffers;
#[cfg(feature = "serde-json")]
#[cfg(not(feature = "flexbuffers"))]
pub mod serde_json;

use bytes::Bytes;

/// Fallibly serialize a value to bytes.
pub trait Binarize {
    type Error: std::error::Error + Send + Sync + 'static;

    fn to_bytes(&self) -> Result<Bytes, Self::Error>;
}

/// Fallibly construct a value from bytes.
pub trait Debinarize {
    type Ok;

    type Error: std::error::Error + Send + Sync + 'static;

    fn from_bytes(bytes: Bytes) -> Result<Self::Ok, Self::Error>;
}
