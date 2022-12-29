//! Persistence for snapshots.

mod noop;

pub use noop::*;

use crate::Metadata;
use bytes::Bytes;
use std::future::Future;
use uuid::Uuid;

/// Persistence for snapshots.
pub trait SnapshotStore {
    type Error: std::error::Error;

    /// Save the given snapshot state for the given entity ID and sequence number.
    fn save<'a, 'b, 'c, S, StateToBytes, StateToBytesError>(
        &'a mut self,
        id: Uuid,
        seq_no: u64,
        state: &'b S,
        meta: Metadata,
        state_to_bytes: &'c StateToBytes,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a
    where
        'b: 'a,
        'c: 'a,
        S: Send + Sync + 'a,
        StateToBytes: Fn(&S) -> Result<Bytes, StateToBytesError> + Send + Sync + 'static,
        StateToBytesError: std::error::Error + Send + Sync + 'static;

    /// Find and possibly load the [Snapshot] for the given entity ID.
    async fn load<S, StateFromBytes, StateFromBytesError>(
        &self,
        id: Uuid,
        state_from_bytes: &StateFromBytes,
    ) -> Result<Option<Snapshot<S>>, Self::Error>
    where
        StateFromBytes: Fn(Bytes) -> Result<S, StateFromBytesError> + Send + Sync + 'static,
        StateFromBytesError: std::error::Error + Send + Sync + 'static;
}

/// Snapshot state along with its sequence number and optional metadata.
pub struct Snapshot<S> {
    pub(crate) seq_no: u64,
    pub(crate) state: S,
    pub(crate) metadata: Metadata,
}

impl<S> Snapshot<S> {
    #[allow(missing_docs)]
    pub fn new(seq_no: u64, state: S, metadata: Metadata) -> Self {
        Self {
            seq_no,
            state,
            metadata,
        }
    }
}
