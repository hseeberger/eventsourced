//! Persistence for snapshots.

mod noop;

pub use noop::*;

use crate::SeqNo;
use bytes::Bytes;
use std::{error::Error as StdError, future::Future};
use uuid::Uuid;

/// Persistence for snapshots.
pub trait SnapshotStore: Clone + Send + 'static {
    type Error: StdError + Send + Sync + 'static;

    /// Save the given snapshot state for the given entity ID and sequence number.
    fn save<S, ToBytes, ToBytesError>(
        &mut self,
        id: Uuid,
        seq_no: SeqNo,
        state: S,
        state_to_bytes: &ToBytes,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send
    where
        S: Send,
        ToBytes: Fn(&S) -> Result<Bytes, ToBytesError> + Sync,
        ToBytesError: StdError + Send + Sync + 'static;

    /// Find and possibly load the [Snapshot] for the given entity ID.
    fn load<S, FromBytes, FromBytesError>(
        &self,
        id: Uuid,
        state_from_bytes: FromBytes,
    ) -> impl Future<Output = Result<Option<Snapshot<S>>, Self::Error>> + Send
    where
        FromBytes: Fn(Bytes) -> Result<S, FromBytesError> + Send,
        FromBytesError: StdError + Send + Sync + 'static;
}

/// Snapshot state along with its sequence number.
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
