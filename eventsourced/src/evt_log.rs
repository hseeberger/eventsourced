//! Persistence for events.

use bytes::Bytes;
use futures::Stream;
use std::{error::Error as StdError, fmt::Debug, future::Future, num::NonZeroU64};

/// Persistence for events.
pub trait EvtLog: Clone + Send + 'static {
    type Id: Debug;

    type Error: StdError + Send + Sync + 'static;

    /// The maximum value for sequence numbers. Defaults to `NonZeroU64::MAX` unless overriden by an
    /// implementation.
    const MAX_SEQ_NO: NonZeroU64 = NonZeroU64::MAX;

    /// Persist the given event and optional tag for the given entity ID and return the
    /// sequence number for the persisted event. The given last sequence number is used for
    /// optimistic locking, i.e. it must match the current last sequence number of the event log.
    fn persist<E, ToBytes, ToBytesError>(
        &mut self,
        type_name: &'static str,
        id: &Self::Id,
        last_seq_no: Option<NonZeroU64>,
        evt: &E,
        to_bytes: &ToBytes,
    ) -> impl Future<Output = Result<NonZeroU64, Self::Error>> + Send
    where
        E: Sync,
        ToBytes: Fn(&E) -> Result<Bytes, ToBytesError> + Sync,
        ToBytesError: StdError + Send + Sync + 'static;

    /// Get the last sequence number for the given type name and entity ID.
    fn last_seq_no(
        &self,
        type_name: &'static str,
        id: &Self::Id,
    ) -> impl Future<Output = Result<Option<NonZeroU64>, Self::Error>> + Send;

    /// Get the events for the given type name and entity ID starting at the given sequence
    /// number.
    fn evts_by_id<E, FromBytes, FromBytesError>(
        &self,
        type_name: &'static str,
        id: &Self::Id,
        seq_no: NonZeroU64,
        from_bytes: FromBytes,
    ) -> impl Future<
        Output = Result<
            impl Stream<Item = Result<(NonZeroU64, E), Self::Error>> + Send,
            Self::Error,
        >,
    > + Send
    where
        E: Send,
        FromBytes: Fn(Bytes) -> Result<E, FromBytesError> + Copy + Send + Sync + 'static,
        FromBytesError: StdError + Send + Sync + 'static;

    /// Get the events for the given entity type starting at the given sequence number.
    fn evts_by_type<E, FromBytes, FromBytesError>(
        &self,
        type_name: &'static str,
        seq_no: NonZeroU64,
        from_bytes: FromBytes,
    ) -> impl Future<
        Output = Result<
            impl Stream<Item = Result<(NonZeroU64, E), Self::Error>> + Send,
            Self::Error,
        >,
    > + Send
    where
        E: Send,
        FromBytes: Fn(Bytes) -> Result<E, FromBytesError> + Copy + Send + Sync + 'static,
        FromBytesError: StdError + Send + Sync + 'static;
}
