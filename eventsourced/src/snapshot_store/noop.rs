//! A [SnapshotStore] implementation that does nothing.

use super::{Snapshot, SnapshotStore};
use crate::Metadata;
use bytes::Bytes;
use std::{convert::Infallible, fmt::Debug};
use uuid::Uuid;

/// A [SnapshotStore] implementation that does nothing.
#[derive(Debug, Clone, Copy)]
pub struct NoopSnapshotStore;

impl SnapshotStore for NoopSnapshotStore {
    type Error = Infallible;

    async fn save<'a, 'b, 'c, S, StateToBytes, StateToBytesError>(
        &'a mut self,
        _id: Uuid,
        _seq_no: u64,
        _state: &'b S,
        _metadata: Metadata,
        _state_to_bytes: &'c StateToBytes,
    ) -> Result<(), Self::Error>
    where
        'b: 'a,
        'c: 'a,
        S: Send + Sync + 'a,
        StateToBytes: Fn(&S) -> Result<Bytes, StateToBytesError> + Send + Sync + 'static,
        StateToBytesError: std::error::Error + Send + Sync + 'static,
    {
        Ok(())
    }

    async fn load<S, StateFromBytes, StateFromBytesError>(
        &self,
        _id: Uuid,
        _state_from_bytes: &StateFromBytes,
    ) -> Result<Option<Snapshot<S>>, Self::Error>
    where
        StateFromBytes: Fn(Bytes) -> Result<S, StateFromBytesError> + Send + Sync + 'static,
        StateFromBytesError: std::error::Error + Send + Sync + 'static,
    {
        Ok(None)
    }
}
