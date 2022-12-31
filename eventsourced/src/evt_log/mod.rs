//! Persistence for events.

use crate::Metadata;
use bytes::Bytes;
use futures::Stream;
use std::future::Future;
use uuid::Uuid;

/// Persistence for events.
pub trait EvtLog {
    type Error: std::error::Error;

    /// Persist the given events for the given entity ID and the given last sequence number.
    fn persist<'a, 'b, 'c, E, EvtToBytes, EvtToBytesError>(
        &'a self,
        id: Uuid,
        evts: &'b [E],
        last_seq_no: u64,
        evt_to_bytes: &'c EvtToBytes,
    ) -> impl Future<Output = Result<Metadata, Self::Error>> + Send + 'a
    where
        'b: 'a,
        'c: 'a,
        E: Send + Sync + 'a,
        EvtToBytes: Fn(&E) -> Result<Bytes, EvtToBytesError> + Send + Sync,
        EvtToBytesError: std::error::Error + Send + Sync + 'static;

    /// Get the last sequence number for the given entity ID.
    async fn last_seq_no(&self, id: Uuid) -> Result<u64, Self::Error>;

    /// Get the events for the given ID in the given closed range of sequence numbers.
    async fn evts_by_id<'a, E, EvtFromBytes, EvtFromBytesError>(
        &'a self,
        id: Uuid,
        from_seq_no: u64,
        to_seq_no: u64,
        meta: Metadata,
        evt_from_bytes: EvtFromBytes,
    ) -> Result<impl Stream<Item = Result<(u64, E), Self::Error>> + 'a, Self::Error>
    where
        E: Send + 'a,
        EvtFromBytes: Fn(Bytes) -> Result<E, EvtFromBytesError> + Copy + Send + Sync + 'static,
        EvtFromBytesError: std::error::Error + Send + Sync + 'static;
}
