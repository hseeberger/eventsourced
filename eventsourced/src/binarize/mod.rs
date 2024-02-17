//! Conversion to and from `Bytes`.

use bytes::Bytes;
use std::error::Error as StdError;

#[cfg_attr(docsrs, doc(cfg(feature = "prost")))]
#[cfg(feature = "prost")]
pub mod prost;

#[cfg_attr(docsrs, doc(cfg(feature = "serde_json")))]
#[cfg(feature = "serde_json")]
pub mod serde_json;

/// Conversion to and from `Bytes`.
pub trait Binarize<E, S>: Copy + Send + Sync + 'static {
    type EvtToBytesError: StdError + Send + Sync + 'static;
    type EvtFromBytesError: StdError + Send + Sync + 'static;

    type StateToBytesError: StdError + Send + Sync + 'static;
    type StateFromBytesError: StdError + Send + Sync + 'static;

    /// Convert an event to bytes.
    fn evt_to_bytes(&self, evt: &E) -> Result<Bytes, Self::EvtToBytesError>;

    /// Convert state to bytes.
    fn state_to_bytes(&self, evt: &S) -> Result<Bytes, Self::StateToBytesError>;

    /// Convert bytes to an event.
    fn evt_from_bytes(&self, bytes: Bytes) -> Result<E, Self::EvtFromBytesError>;

    /// Convert bytes to state.
    fn state_from_bytes(&self, bytes: Bytes) -> Result<S, Self::StateFromBytesError>;
}
