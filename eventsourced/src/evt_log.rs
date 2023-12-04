//! Persistence for events.

use crate::SeqNo;
use bytes::Bytes;
use futures::Stream;
use std::{error::Error as StdError, future::Future, num::NonZeroU64};
use uuid::Uuid;

/// Persistence for events.
pub trait EvtLog: Clone + Send + 'static {
    type Error: StdError + Send + Sync + 'static;

    /// The maximum value for sequence numbers. Defaults to `u64::MAX` unless overriden by an
    /// implementation.
    const MAX_SEQ_NO: SeqNo = SeqNo::new(NonZeroU64::MAX);

    /// Persist the given event and optional tag for the given entity type and ID and return the
    /// sequence number for the persisted event. The given last sequence number is used for
    /// optimistic locking, i.e. it must match the current last sequence number of the event log.
    fn persist<E, ToBytes, ToBytesError>(
        &mut self,
        evt: &E,
        tag: Option<&str>,
        r#type: &str,
        id: Uuid,
        last_seq_no: Option<SeqNo>,
        to_bytes: &ToBytes,
    ) -> impl Future<Output = Result<SeqNo, Self::Error>> + Send
    where
        E: Sync,
        ToBytes: Fn(&E) -> Result<Bytes, ToBytesError> + Sync,
        ToBytesError: StdError + Send + Sync + 'static;

    /// Get the last sequence number for the given entity type and ID.
    fn last_seq_no(
        &self,
        r#type: &str,
        id: Uuid,
    ) -> impl Future<Output = Result<Option<SeqNo>, Self::Error>> + Send;

    /// Get the events for the given entity ID starting with the given sequence number.
    fn evts_by_id<E, FromBytes, FromBytesError>(
        &self,
        r#type: &str,
        id: Uuid,
        from_seq_no: SeqNo,
        from_bytes: FromBytes,
    ) -> impl Future<
        Output = Result<impl Stream<Item = Result<(SeqNo, E), Self::Error>> + Send, Self::Error>,
    > + Send
    where
        E: Send,
        FromBytes: Fn(Bytes) -> Result<E, FromBytesError> + Copy + Send + Sync + 'static,
        FromBytesError: StdError + Send + Sync + 'static;

    /// Get the events for the given entity type starting with the given sequence number.
    fn evts_by_type<E, FromBytes, FromBytesError>(
        &self,
        r#type: &str,
        from_seq_no: SeqNo,
        from_bytes: FromBytes,
    ) -> impl Future<
        Output = Result<impl Stream<Item = Result<(SeqNo, E), Self::Error>> + Send, Self::Error>,
    > + Send
    where
        E: Send,
        FromBytes: Fn(Bytes) -> Result<E, FromBytesError> + Copy + Send + Sync + 'static,
        FromBytesError: StdError + Send + Sync + 'static;

    /// Get the events for the given tag starting with the given sequence number.
    fn evts_by_tag<E, FromBytes, FromBytesError>(
        &self,
        tag: String,
        from_seq_no: SeqNo,
        from_bytes: FromBytes,
    ) -> impl Future<
        Output = Result<impl Stream<Item = Result<(SeqNo, E), Self::Error>> + Send, Self::Error>,
    > + Send
    where
        E: Send,
        FromBytes: Fn(Bytes) -> Result<E, FromBytesError> + Copy + Send + Sync + 'static,
        FromBytesError: StdError + Send + Sync + 'static;
}
