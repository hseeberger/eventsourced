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

    async fn save<S, ToBytes, ToBytesError>(
        &mut self,
        _id: Uuid,
        _seq_no: SeqNo,
        _state: S,
        _state_to_bytes: &ToBytes,
    ) -> Result<(), Self::Error>
    where
        S: Send,
        ToBytes: Fn(&S) -> Result<Bytes, ToBytesError> + Sync,
        ToBytesError: StdError,
    {
        Ok(())
    }

    async fn load<S, FromBytes, FromBytesError>(
        &self,
        _id: Uuid,
        _state_from_bytes: FromBytes,
    ) -> Result<Option<Snapshot<S>>, Self::Error>
    where
        FromBytes: Fn(Bytes) -> Result<S, FromBytesError> + Send,
        FromBytesError: StdError + Send,
    {
        Ok(None)
    }
}
