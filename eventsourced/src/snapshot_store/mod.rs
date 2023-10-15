//! Persistence for snapshots.

mod noop;

pub use noop::*;

use crate::SeqNo;
use bytes::Bytes;
use std::{error::Error as StdError, future::Future};
use uuid::Uuid;

/// Persistence for snapshots.
pub trait SnapshotStore: Send + 'static {
    type Error: StdError;

    /// Save the given snapshot state for the given entity ID and sequence number.
    fn save<S, StateToBytes, StateToBytesError>(
        &mut self,
        id: Uuid,
        seq_no: SeqNo,
        state: S,
        state_to_bytes: &StateToBytes,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send
    where
        S: Send,
        StateToBytes: Fn(&S) -> Result<Bytes, StateToBytesError> + Sync,
        StateToBytesError: StdError;

    /// Find and possibly load the [Snapshot] for the given entity ID.
    fn load<S, StateFromBytes, StateFromBytesError>(
        &self,
        id: Uuid,
        state_from_bytes: StateFromBytes,
    ) -> impl Future<Output = Result<Option<Snapshot<S>>, Self::Error>>
    where
        StateFromBytes: Fn(Bytes) -> Result<S, StateFromBytesError>,
        StateFromBytesError: StdError;
}

/// Snapshot state along with its sequence number and optional metadata.
pub struct Snapshot<S> {
    pub seq_no: SeqNo,
    pub state: S,
}

impl<S> Snapshot<S> {
    #[allow(missing_docs)]
    pub fn new(seq_no: SeqNo, state: S) -> Self {
        Self { seq_no, state }
    }
}
