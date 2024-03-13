use crate::snapshot_store::{Snapshot, SnapshotStore};
use bytes::Bytes;
use error_ext::BoxError;
use std::{
    collections::HashMap, error::Error as StdError, fmt::Debug, hash::Hash, num::NonZeroU64,
};
use thiserror::Error;

/// An in-memory implementation of [SnapshotStore] for testing purposes.
#[derive(Debug, Default, Clone)]
pub struct TestSnapshotStore<I> {
    snapshots: HashMap<I, (NonZeroU64, Bytes)>,
}

impl<I> SnapshotStore for TestSnapshotStore<I>
where
    I: Debug + Clone + Eq + Hash + Send + Sync + 'static,
{
    type Id = I;

    type Error = Error;

    async fn save<S, ToBytes, ToBytesError>(
        &mut self,
        id: &Self::Id,
        seq_no: NonZeroU64,
        state: &S,
        to_bytes: &ToBytes,
    ) -> Result<(), Self::Error>
    where
        S: Send + Sync,
        ToBytes: Fn(&S) -> Result<Bytes, ToBytesError> + Sync,
        ToBytesError: StdError + Send + Sync + 'static,
    {
        let bytes = to_bytes(state).map_err(|error| Error(error.into()))?;
        self.snapshots.insert(id.to_owned(), (seq_no, bytes));
        Ok(())
    }

    async fn load<S, FromBytes, FromBytesError>(
        &self,
        id: &Self::Id,
        from_bytes: FromBytes,
    ) -> Result<Option<Snapshot<S>>, Self::Error>
    where
        FromBytes: Fn(Bytes) -> Result<S, FromBytesError> + Send,
        FromBytesError: StdError + Send + Sync + 'static,
    {
        self.snapshots
            .get(id)
            .map(|(seq_no, bytes)| {
                from_bytes(bytes.to_owned())
                    .map_err(|error| Error(error.into()))
                    .map(|state| Snapshot::new(*seq_no, state))
            })
            .transpose()
    }
}

#[derive(Debug, Error)]
#[error(transparent)]
pub struct Error(BoxError);

#[cfg(all(test, feature = "serde_json"))]
mod tests {
    use super::*;
    use crate::binarize::serde_json::*;
    use assert_matches::assert_matches;

    #[tokio::test]
    async fn test() {
        let mut snapshot_store = TestSnapshotStore::<u64>::default();

        let result = snapshot_store.load::<String, _, _>(&0, from_bytes).await;
        assert_matches!(result, Ok(None));

        let result = snapshot_store
            .save(&0, 42.try_into().unwrap(), &"42".to_string(), &to_bytes)
            .await;
        assert!(result.is_ok());

        let result = snapshot_store.load::<String, _, _>(&0, from_bytes).await;
        assert_matches!(
            result,
            Ok(Some(Snapshot { seq_no, state }))
                if seq_no == 42.try_into().unwrap() && state == "42"
        );

        let result = snapshot_store.load::<String, _, _>(&1, from_bytes).await;
        assert_matches!(result, Ok(None));
    }
}
