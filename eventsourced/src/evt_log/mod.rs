//! Persistence for events.

use crate::Metadata;
use bytes::Bytes;
use futures::Stream;
use std::{
    error::Error as StdError,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use uuid::Uuid;

/// Persistence for events.
pub trait EvtLog: Send + Sync + 'static {
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
    fn last_seq_no<'a>(
        &'a self,
        id: Uuid,
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send + 'a;

    /// Get the events for the given ID in the given closed range of sequence numbers.
    fn evts_by_id<'a, E, EvtFromBytes, EvtFromBytesError>(
        &'a self,
        id: Uuid,
        from_seq_no: u64,
        to_seq_no: u64,
        meta: Metadata,
        evt_from_bytes: EvtFromBytes,
    ) -> impl Future<
        Output = Result<
            EvtStream<E, impl Stream<Item = Result<(u64, E), Self::Error>> + Send, Self::Error>,
            Self::Error,
        >,
    > + Send
           + 'a
    where
        E: Send + 'a,
        EvtFromBytes: Fn(Bytes) -> Result<E, EvtFromBytesError> + Copy + Send + Sync + 'static,
        EvtFromBytesError: StdError + Send + Sync + 'static;
}

pub struct EvtStream<E, S, R>(S)
where
    S: Stream<Item = Result<(u64, E), R>>;

impl<E, S, R> From<S> for EvtStream<E, S, R>
where
    S: Stream<Item = Result<(u64, E), R>>,
{
    fn from(stream: S) -> Self {
        EvtStream(stream)
    }
}

impl<E, S, R> Stream for EvtStream<E, S, R>
where
    S: Stream<Item = Result<(u64, E), R>> + Send,
{
    type Item = Result<(u64, E), R>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.0) };
        let poll = inner.poll_next(cx);
        poll
    }
}
