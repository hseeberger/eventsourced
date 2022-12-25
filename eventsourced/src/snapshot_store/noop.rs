//! A [SnapshotStore] implementation that does nothing.

use super::{Snapshot, SnapshotStore};
use crate::{
    convert::{TryFromBytes, TryIntoBytes},
    Metadata,
};
use std::{convert::Infallible, fmt::Debug};
use uuid::Uuid;

/// A [SnapshotStore] implementation that does nothing.
#[derive(Debug, Clone, Copy)]
pub struct NoopSnapshotStore;

impl SnapshotStore for NoopSnapshotStore {
    type Error = Infallible;

    async fn save<'a, 'b, S>(
        &'a mut self,
        _id: Uuid,
        _seq_no: u64,
        _state: &'b S,
        _metadata: Metadata,
    ) -> Result<(), Self::Error>
    where
        'b: 'a,
        S: TryIntoBytes + Send + Sync + 'a,
    {
        Ok(())
    }

    async fn load<S>(&self, _id: Uuid) -> Result<Option<Snapshot<S>>, Self::Error>
    where
        S: TryFromBytes,
    {
        Ok(None)
    }
}
