//! Persistence for events.

use crate::SeqNo;
use bytes::Bytes;
use futures::Stream;
use std::{error::Error as StdError, fmt::Debug, future::Future, num::NonZeroU64};
use uuid::Uuid;

/// Persistence for events.
pub trait EvtLog: Clone + Send + Sync + 'static {
    type Error: StdError + Send + Sync;

    /// The maximum value for sequence numbers. Defaults to `u64::MAX` unless overriden by an
    /// implementation.
    const MAX_SEQ_NO: SeqNo = SeqNo::new(NonZeroU64::MAX);

    /// Persist the given event and optional tag for the given entity ID and returns the sequence
    /// number for the persisted event.
    fn persist<'a, E, EvtToBytes, EvtToBytesError>(
        &'a mut self,
        id: Uuid,
        evt: &'a E,
        tag: Option<String>,
        evt_to_bytes: &'a EvtToBytes,
    ) -> impl Future<Output = Result<SeqNo, Self::Error>> + Send
    where
        E: Debug + Send + Sync + 'a,
        EvtToBytes: Fn(&E) -> Result<Bytes, EvtToBytesError> + Send + Sync,
        EvtToBytesError: StdError + Send + Sync + 'static;

    /// Get the last sequence number for the given entity ID.
    fn last_seq_no(
        &self,
        id: Uuid,
    ) -> impl Future<Output = Result<Option<SeqNo>, Self::Error>> + Send + '_;

    /// Get the events for the given entity ID starting with the given sequence number.
    fn evts_by_id<'a, E, EvtFromBytes, EvtFromBytesError>(
        &'a self,
        id: Uuid,
        from_seq_no: SeqNo,
        evt_from_bytes: EvtFromBytes,
    ) -> impl Future<
        Output = Result<impl Stream<Item = Result<(SeqNo, E), Self::Error>> + Send, Self::Error>,
    > + Send
    where
        E: Debug + Send + 'a,
        EvtFromBytes: Fn(Bytes) -> Result<E, EvtFromBytesError> + Copy + Send + Sync + 'static,
        EvtFromBytesError: StdError + Send + Sync + 'static;

    /// Get the events for the given tag starting with the given sequence number.
    fn evts_by_tag<'a, E, T, EvtFromBytes, EvtFromBytesError>(
        &'a self,
        tag: T,
        from_seq_no: SeqNo,
        evt_from_bytes: EvtFromBytes,
    ) -> impl Future<
        Output = Result<impl Stream<Item = Result<(SeqNo, E), Self::Error>> + Send, Self::Error>,
    > + Send
    where
        E: Debug + Send + 'a,
        EvtFromBytes: Fn(Bytes) -> Result<E, EvtFromBytesError> + Copy + Send + Sync + 'static,
        EvtFromBytesError: StdError + Send + Sync + 'static,
        T: Into<String> + Send;
}
