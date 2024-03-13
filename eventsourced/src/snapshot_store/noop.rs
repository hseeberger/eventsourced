//! A [SnapshotStore] implementation that does nothing.

use crate::snapshot_store::{Snapshot, SnapshotStore};
use bytes::Bytes;
use std::{
    convert::Infallible, error::Error as StdError, fmt::Debug, marker::PhantomData, num::NonZeroU64,
};

/// A [SnapshotStore] implementation that does nothing.
#[derive(Debug, Clone, Copy)]
pub struct NoopSnapshotStore<I>(PhantomData<I>);

impl<I> NoopSnapshotStore<I> {
    pub fn new() -> Self {
        Default::default()
    }
}

impl<I> Default for NoopSnapshotStore<I> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<I> SnapshotStore for NoopSnapshotStore<I>
where
    I: Debug + Clone + Send + Sync + 'static,
{
    type Id = I;

    type Error = Infallible;

    async fn save<S, ToBytes, ToBytesError>(
        &mut self,
        _id: &Self::Id,
        _seq_no: NonZeroU64,
        _state: &S,
        _to_bytes: &ToBytes,
    ) -> Result<(), Self::Error>
    where
        S: Send + Sync,
        ToBytes: Fn(&S) -> Result<Bytes, ToBytesError> + Sync,
        ToBytesError: StdError,
    {
        Ok(())
    }

    async fn load<S, FromBytes, FromBytesError>(
        &self,
        _id: &Self::Id,
        _from_bytes: FromBytes,
    ) -> Result<Option<Snapshot<S>>, Self::Error>
    where
        FromBytes: Fn(Bytes) -> Result<S, FromBytesError> + Send,
        FromBytesError: StdError + Send,
    {
        Ok(None)
    }
}
