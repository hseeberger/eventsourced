//! Persistence for events.

use crate::Metadata;
use bytes::Bytes;
use futures::Stream;
use std::{error::Error as StdError, future::Future};
use uuid::Uuid;

/// Persistence for events.
pub trait EvtLog: Clone + Send + Sync + 'static {
    type Error: StdError + Send + Sync;

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
        EvtToBytesError: StdError + Send + Sync + 'static;

    /// Get the last sequence number for the given entity ID.
    fn last_seq_no(&self, id: Uuid) -> impl Future<Output = Result<u64, Self::Error>> + Send + '_;

    /// Get the events for the given ID in the given closed range of sequence numbers.
    fn evts_by_id<'a, E, EvtFromBytes, EvtFromBytesError>(
        &'a self,
        id: Uuid,
        from_seq_no: u64,
        to_seq_no: u64,
        metadata: Metadata,
        evt_from_bytes: EvtFromBytes,
    ) -> impl Future<
        Output = Result<impl Stream<Item = Result<(u64, E), Self::Error>> + Send, Self::Error>,
    > + Send
           + 'a
    where
        E: Send + 'a,
        EvtFromBytes: Fn(Bytes) -> Result<E, EvtFromBytesError> + Copy + Send + Sync + 'static,
        EvtFromBytesError: StdError + Send + Sync + 'static;
}
