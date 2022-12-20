//! Persistence for snapshots.

#[cfg(feature = "nats")]
pub mod nats;

use crate::convert::{TryFromBytes, TryIntoBytes};
use std::future::Future;
use uuid::Uuid;

/// Persistence for snapshots.
pub trait SnapshotStore {
    type Error: std::error::Error;

    /// Save the given snapshot state for the given entity ID and sequence number.
    fn save<'a, 'b, S>(
        &'a mut self,
        id: Uuid,
        seq_no: u64,
        state: &'b S,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a
    where
        'b: 'a,
        S: TryIntoBytes + Send + Sync + 'a;

    /// Find and possibly load the [Snapshot] for the given entity ID.
    async fn load<S>(&self, id: Uuid) -> Result<Option<Snapshot<S>>, Self::Error>
    where
        S: TryFromBytes;
}

/// Snapshot state along with its sequence number.
pub struct Snapshot<S> {
    pub(crate) seq_no: u64,
    pub(crate) state: S,
}
