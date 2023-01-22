//! Persistence for events.

use crate::Metadata;
use bytes::Bytes;
use futures::Stream;
use std::{error::Error as StdError, fmt::Debug, future::Future, num::NonZeroU64};
use uuid::Uuid;

/// Persistence for events.
pub trait EvtLog: Clone + Send + Sync + 'static {
    type Error: StdError + Send + Sync;

    /// The maximum value for sequence numbers. Defaults to `u64::MAX` unless overriden by an
    /// implementation.
    const MAX_SEQ_NO: u64 = u64::MAX;

    /// Persist the given events for the given entity ID and the given last sequence number.
    ///
    /// # Panics
    /// - Panics if `evts` are empty.
    /// - Panics if `last_seq_no` is less or equal [MAX_SEQ_NO] - `evts.len()`.
    fn persist<'a, E, EvtToBytes, EvtToBytesError>(
        &'a mut self,
        id: Uuid,
        evts: &'a [E],
        last_seq_no: u64,
        evt_to_bytes: &'a EvtToBytes,
    ) -> impl Future<Output = Result<Metadata, Self::Error>> + Send
    where
        E: Debug + Send + Sync + 'a,
        EvtToBytes: Fn(&E) -> Result<Bytes, EvtToBytesError> + Send + Sync,
        EvtToBytesError: StdError + Send + Sync + 'static;

    /// Get the last sequence number for the given entity ID.
    fn last_seq_no(&self, id: Uuid) -> impl Future<Output = Result<u64, Self::Error>> + Send + '_;

    /// Get the events for the given ID in the given closed range of sequence numbers.
    ///
    /// # Panics
    /// - Panics if `from_seq_no` is not less than or equal to `to_seq_no`.
    /// - Panics if `to_seq_no` is not less or equal [MAX_SEQ_NO].
    fn evts_by_id<'a, E, EvtFromBytes, EvtFromBytesError>(
        &'a self,
        id: Uuid,
        from_seq_no: NonZeroU64,
        to_seq_no: u64,
        metadata: Metadata,
        evt_from_bytes: EvtFromBytes,
    ) -> impl Future<
        Output = Result<impl Stream<Item = Result<(u64, E), Self::Error>> + Send, Self::Error>,
    > + Send
    where
        E: Debug + Send + 'a,
        EvtFromBytes: Fn(Bytes) -> Result<E, EvtFromBytesError> + Copy + Send + Sync + 'static,
        EvtFromBytesError: StdError + Send + Sync + 'static;
}
