//! Persistence for snapshots.

mod noop;

pub use noop::*;

use crate::Metadata;
use bytes::Bytes;
use std::{error::Error as StdError, future::Future};
use uuid::Uuid;

/// Persistence for snapshots.
pub trait SnapshotStore {
    type Error: StdError + Send + Sync;

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
        StateToBytesError: StdError + Send + Sync + 'static;

    /// Find and possibly load the [Snapshot] for the given entity ID.
    async fn load<S, StateFromBytes, StateFromBytesError>(
        &self,
        id: Uuid,
        state_from_bytes: &StateFromBytes,
    ) -> Result<Option<Snapshot<S>>, Self::Error>
    where
        StateFromBytes: Fn(Bytes) -> Result<S, StateFromBytesError> + Send + Sync + 'static,
        StateFromBytesError: StdError + Send + Sync + 'static;
}

/// Snapshot state along with its sequence number and optional metadata.
pub struct Snapshot<S> {
    pub seq_no: u64,
    pub state: S,
    pub metadata: Metadata,
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
