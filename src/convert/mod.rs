//! Conversion into and from bytes.

#[cfg(feature = "flexbuffers")]
pub mod flexbuffers;
#[cfg(feature = "serde_json")]
#[cfg(not(feature = "flexbuffers"))]
pub mod serde_json;

/// Fallibly serialize a value to bytes. Almost like `TryInto<Vec<u8>>` except for borrowing instead
/// of consuming, but defined here to allow for blanket implementations, e.g. for any type
/// implementing `Serialize`.
pub trait TryIntoBytes {
    type Error: std::error::Error + Send + Sync + 'static;

    fn try_into_bytes(&self) -> Result<Vec<u8>, Self::Error>;
}

/// Fallibly construct a value from bytes. Like `TryFrom<Vec<u8>>`, but defined here to allow for
/// blanket implementations, e.g. for any type implementing `DeserializeOwned`.
pub trait TryFromBytes: Sized {
    type Error: std::error::Error + Send + Sync + 'static;

    fn try_from_bytes(bytes: Vec<u8>) -> Result<Self, Self::Error>;
}
