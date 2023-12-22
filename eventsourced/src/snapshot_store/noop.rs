//! A [SnapshotStore] implementation that does nothing.

use crate::{SeqNo, Snapshot, SnapshotStore};
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
        _state: &S,
        _to_bytes: &ToBytes,
    ) -> Result<(), Self::Error>
    where
        S: Send + Sync,
        ToBytes: Fn(&S) -> Result<Bytes, ToBytesError> + Sync,
        ToBytesError: StdError,
    {
        Ok(())
    }

    async fn load<S, FromBytes, FromBytesError>(
        &self,
        _id: Uuid,
        _from_bytes: FromBytes,
    ) -> Result<Option<Snapshot<S>>, Self::Error>
    where
        FromBytes: Fn(Bytes) -> Result<S, FromBytesError> + Send,
        FromBytesError: StdError + Send,
    {
        Ok(None)
    }
}
