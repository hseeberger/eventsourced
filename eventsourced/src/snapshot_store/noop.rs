//! A [SnapshotStore] implementation that does nothing.

use super::{Snapshot, SnapshotStore};
use crate::SeqNo;
use bytes::Bytes;
use std::{convert::Infallible, error::Error as StdError, fmt::Debug};
use uuid::Uuid;

/// A [SnapshotStore] implementation that does nothing.
#[derive(Debug, Clone, Copy)]
pub struct NoopSnapshotStore;

impl SnapshotStore for NoopSnapshotStore {
    type Error = Infallible;

    async fn save<S, StateToBytes, StateToBytesError>(
        &mut self,
        _id: Uuid,
        _seq_no: SeqNo,
        _state: S,
        _state_to_bytes: &StateToBytes,
    ) -> Result<(), Self::Error>
    where
        S: Send,
        StateToBytes: Fn(&S) -> Result<Bytes, StateToBytesError> + Sync,
        StateToBytesError: StdError,
    {
        Ok(())
    }

    async fn load<S, StateFromBytes, StateFromBytesError>(
        &self,
        _id: Uuid,
        _state_from_bytes: StateFromBytes,
    ) -> Result<Option<Snapshot<S>>, Self::Error>
    where
        StateFromBytes: Fn(Bytes) -> Result<S, StateFromBytesError>,
        StateFromBytesError: StdError,
    {
        Ok(None)
    }
}
