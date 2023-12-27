//! Persistence for snapshots.

mod noop;

pub use noop::*;

use crate::NonZeroU64;
use bytes::Bytes;
use std::{error::Error as StdError, fmt::Debug};

/// Persistence for snapshots.
#[trait_variant::make(SnapshotStore: Send)]
pub trait LocalSnapshotStore: Clone + 'static {
    type Id: Debug;

    type Error: StdError + Send + Sync + 'static;

    /// Save the given snapshot state for the given entity ID and sequence number.
    async fn save<S, ToBytes, ToBytesError>(
        &mut self,
        id: &Self::Id,
        seq_no: NonZeroU64,
        state: &S,
        to_bytes: &ToBytes,
    ) -> Result<(), Self::Error>
    where
        S: Send + Sync,
        ToBytes: Fn(&S) -> Result<Bytes, ToBytesError> + Sync,
        ToBytesError: StdError + Send + Sync + 'static;

    /// Find and possibly load the [Snapshot] for the given entity ID.
    async fn load<S, FromBytes, FromBytesError>(
        &self,
        id: &Self::Id,
        from_bytes: FromBytes,
    ) -> Result<Option<Snapshot<S>>, Self::Error>
    where
        FromBytes: Fn(Bytes) -> Result<S, FromBytesError> + Send,
        FromBytesError: StdError + Send + Sync + 'static;
}

/// Snapshot state along with its sequence number.
pub struct Snapshot<S> {
    pub seq_no: NonZeroU64,
    pub state: S,
}

impl<S> Snapshot<S> {
    #[allow(missing_docs)]
    pub fn new(seq_no: NonZeroU64, state: S) -> Self {
        Self { seq_no, state }
    }
}
