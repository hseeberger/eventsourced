//! Persistence for events.

use crate::SeqNo;
use bytes::Bytes;
use futures::Stream;
use std::{error::Error as StdError, future::Future, num::NonZeroU64};
use uuid::Uuid;

/// Persistence for events.
pub trait EvtLog: Send + 'static {
    type Error: StdError;

    /// The maximum value for sequence numbers. Defaults to `u64::MAX` unless overriden by an
    /// implementation.
    const MAX_SEQ_NO: SeqNo = SeqNo::new(NonZeroU64::MAX);

    /// Persist the given event and optional tag for the given entity ID and return the sequence
    /// number for the persisted event.
    fn persist<E, EvtToBytes, EvtToBytesError>(
        &mut self,
        evt: &E,
        tag: Option<String>,
        id: Uuid,
        evt_to_bytes: &EvtToBytes,
    ) -> impl Future<Output = Result<SeqNo, Self::Error>> + Send
    where
        EvtToBytes: Fn(&E) -> Result<Bytes, EvtToBytesError>,
        EvtToBytesError: StdError;

    /// Get the last sequence number for the given entity ID.
    fn last_seq_no(&self, id: Uuid) -> impl Future<Output = Result<Option<SeqNo>, Self::Error>>;

    /// Get the events for the given entity ID starting with the given sequence number.
    fn evts_by_id<E, EvtFromBytes, EvtFromBytesError>(
        &self,
        id: Uuid,
        from_seq_no: SeqNo,
        evt_from_bytes: EvtFromBytes,
    ) -> impl Future<Output = Result<impl Stream<Item = Result<(SeqNo, E), Self::Error>>, Self::Error>>
    where
        EvtFromBytes: Fn(Bytes) -> Result<E, EvtFromBytesError>,
        EvtFromBytesError: StdError;

    /// Get the events for the given tag starting with the given sequence number.
    fn evts_by_tag<E, EvtFromBytes, EvtFromBytesError>(
        &self,
        tag: String,
        from_seq_no: SeqNo,
        evt_from_bytes: EvtFromBytes,
    ) -> impl Future<
        Output = Result<impl Stream<Item = Result<(SeqNo, E), Self::Error>> + Send, Self::Error>,
    >
    where
        EvtFromBytes: Fn(Bytes) -> Result<E, EvtFromBytesError>,
        EvtFromBytesError: StdError;
}
