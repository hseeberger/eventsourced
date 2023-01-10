//! A [SnapshotStore] implementation that does nothing.

use super::{Snapshot, SnapshotStore};
use crate::Metadata;
use bytes::Bytes;
use std::{convert::Infallible, error::Error as StdError, fmt::Debug};
use uuid::Uuid;

/// A [SnapshotStore] implementation that does nothing.
#[derive(Debug, Clone, Copy)]
pub struct NoopSnapshotStore;

impl SnapshotStore for NoopSnapshotStore {
    type Error = Infallible;

    async fn save<'a, S, StateToBytes, StateToBytesError>(
        &'a mut self,
        _id: Uuid,
        _seq_no: u64,
        _state: &'a S,
        _metadata: Metadata,
        _state_to_bytes: &'a StateToBytes,
    ) -> Result<(), Self::Error>
    where
        S: Send + Sync + 'a,
        StateToBytes: Fn(&S) -> Result<Bytes, StateToBytesError> + Send + Sync + 'static,
        StateToBytesError: StdError + Send + Sync + 'static,
    {
        Ok(())
    }

    async fn load<'a, S, StateFromBytes, StateFromBytesError>(
        &'a self,
        _id: Uuid,
        _state_from_bytes: StateFromBytes,
    ) -> Result<Option<Snapshot<S>>, Self::Error>
    where
        S: 'a,
        StateFromBytes: Fn(Bytes) -> Result<S, StateFromBytesError> + Copy + Send + Sync + 'static,
        StateFromBytesError: StdError + Send + Sync + 'static,
    {
        Ok(None)
    }
}
