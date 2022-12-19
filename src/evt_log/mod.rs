//! Persistence for events.

#[cfg(feature = "nats")]
pub mod nats;

use crate::binarize::{Binarize, Debinarize};
use futures::Stream;
use std::future::Future;
use uuid::Uuid;

/// Persistence for events.
pub trait EvtLog {
    type Error: std::error::Error;

    /// Persist the given events for the given entity ID.
    fn persist<'a, 'b, E>(
        &'a mut self,
        id: Uuid,
        evts: &'b [E],
        seq_no: u64,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a
    where
        'b: 'a,
        E: Send + Sync + 'a,
        [E]: Binarize;

    /// Get the last sequence number for the given entity ID.
    async fn last_seq_no(&self, id: Uuid) -> Result<u64, Self::Error>;

    /// Get the events for the entity with the given ID in the given closed range of sequence
    /// numbers.
    async fn evts_by_id<E>(
        &mut self,
        id: Uuid,
        from_seq_no: u64,
        to_seq_no: u64,
    ) -> Result<impl Stream<Item = Result<E, Self::Error>>, Self::Error>
    where
        E: Send,
        Vec<E>: Debinarize;
}
