use crate::EvtLog;
use bytes::Bytes;
use error_ext::BoxError;
use futures::{stream, Stream};
use std::{
    collections::HashMap, error::Error as StdError, fmt::Debug, hash::Hash, iter, num::NonZeroU64,
    sync::Arc,
};
use thiserror::Error;
use tokio::sync::RwLock;

type Evts<I> = HashMap<&'static str, HashMap<I, Vec<(NonZeroU64, Bytes)>>>;

/// An in-memory implementation of [EvtLog] for testing purposes.
#[derive(Debug, Clone)]
pub struct TestEvtLog<I> {
    seq_no: Arc<RwLock<NonZeroU64>>,
    evts: Arc<RwLock<Evts<I>>>,
}

impl<I> Default for TestEvtLog<I> {
    fn default() -> Self {
        Self {
            seq_no: Arc::new(RwLock::new(NonZeroU64::MIN)),
            evts: Default::default(),
        }
    }
}

impl<I> EvtLog for TestEvtLog<I>
where
    I: Debug + Clone + Eq + Hash + Send + Sync + 'static,
{
    type Id = I;
    type Error = Error;

    async fn persist<E, ToBytes, ToBytesError>(
        &mut self,
        type_name: &'static str,
        id: &Self::Id,
        _last_seq_no: Option<NonZeroU64>,
        evt: &E,
        to_bytes: &ToBytes,
    ) -> Result<NonZeroU64, Self::Error>
    where
        E: Sync,
        ToBytes: Fn(&E) -> Result<Bytes, ToBytesError> + Sync,
        ToBytesError: StdError + Send + Sync + 'static,
    {
        let bytes = to_bytes(evt).map_err(|error| Error(error.into()))?;

        let mut seq_no = self.seq_no.write().await;
        self.evts
            .write()
            .await
            .entry(type_name)
            .and_modify(|evts| {
                evts.entry(id.to_owned())
                    .and_modify(|evts| {
                        evts.push((*seq_no, bytes.clone()));
                    })
                    .or_insert(vec![(*seq_no, bytes.clone())]);
            })
            .or_insert(HashMap::from_iter(iter::once((
                id.to_owned(),
                vec![(*seq_no, bytes)],
            ))));

        let this_seq_no = *seq_no;
        *seq_no = seq_no.saturating_add(1);

        Ok(this_seq_no)
    }

    async fn last_seq_no(
        &self,
        type_name: &'static str,
        id: &Self::Id,
    ) -> Result<Option<NonZeroU64>, Self::Error> {
        let seq_no = self
            .evts
            .read()
            .await
            .get(type_name)
            .and_then(|evts| evts.get(id))
            .and_then(|evts| evts.last())
            .map(|(seq_no, _)| seq_no)
            .copied();

        Ok(seq_no)
    }

    async fn evts_by_id<E, FromBytes, FromBytesError>(
        &self,
        type_name: &'static str,
        id: &Self::Id,
        seq_no: NonZeroU64,
        from_bytes: FromBytes,
    ) -> Result<impl Stream<Item = Result<(NonZeroU64, E), Self::Error>> + Send, Self::Error>
    where
        E: Send,
        FromBytes: Fn(Bytes) -> Result<E, FromBytesError> + Copy + Send + Sync,
        FromBytesError: StdError + Send + Sync + 'static,
    {
        let evts = self
            .evts
            .read()
            .await
            .get(type_name)
            .and_then(|evts| evts.get(id).cloned())
            .unwrap_or_default()
            .into_iter()
            .skip_while(move |(this_seq_no, _)| *this_seq_no < seq_no)
            .map(move |(seq_no, bytes)| {
                from_bytes(bytes.to_owned())
                    .map_err(|error| Error(error.into()))
                    .map(|evt| (seq_no, evt))
            });

        Ok(stream::iter(evts))
    }

    async fn evts_by_type<E, FromBytes, FromBytesError>(
        &self,
        type_name: &'static str,
        seq_no: NonZeroU64,
        from_bytes: FromBytes,
    ) -> Result<impl Stream<Item = Result<(NonZeroU64, E), Self::Error>> + Send, Self::Error>
    where
        E: Send,
        FromBytes: Fn(Bytes) -> Result<E, FromBytesError> + Copy + Send + Sync,
        FromBytesError: StdError + Send + Sync + 'static,
    {
        let evts = self
            .evts
            .read()
            .await
            .get(type_name)
            .cloned()
            .unwrap_or_default()
            .into_values()
            .flatten()
            .filter(move |(this_seq_no, _)| *this_seq_no >= seq_no)
            .map(move |(seq_no, bytes)| {
                from_bytes(bytes.to_owned())
                    .map_err(|error| Error(error.into()))
                    .map(|evt| (seq_no, evt))
            })
            .collect::<Vec<_>>();

        Ok(stream::iter(evts))
    }
}

#[derive(Debug, Error)]
#[error(transparent)]
pub struct Error(BoxError);

#[cfg(all(test, feature = "serde_json"))]
mod tests {
    use crate::{
        binarize::serde_json::*,
        evt_log::{test::TestEvtLog, EvtLog},
    };
    use assert_matches::assert_matches;
    use futures::TryStreamExt;
    use std::num::NonZeroU64;

    #[tokio::test]
    async fn test() {
        let mut evt_log = TestEvtLog::<u64>::default();

        let evts = evt_log
            .evts_by_id::<String, _, _>("type-1", &0, NonZeroU64::MIN, from_bytes)
            .await;
        assert!(evts.is_ok());
        let evts = evts.unwrap().try_collect::<Vec<_>>().await;
        assert_matches!(evts, Ok(evts) if evts.is_empty());

        let result = evt_log
            .persist("type-0", &0, None, &"type-0-0-A".to_string(), &to_bytes)
            .await;
        assert_matches!(result, Ok(seq_no) if seq_no.get() == 1);
        let result = evt_log
            .persist("type-0", &0, None, &"type-0-0-B".to_string(), &to_bytes)
            .await;
        assert_matches!(result, Ok(seq_no) if seq_no.get() == 2);
        let result = evt_log
            .persist("type-0", &1, None, &"type-0-1-A".to_string(), &to_bytes)
            .await;
        assert_matches!(result, Ok(seq_no) if seq_no.get() == 3);
        let result = evt_log
            .persist("type-1", &0, None, &"type-1-0-A".to_string(), &to_bytes)
            .await;
        assert_matches!(result, Ok(seq_no) if seq_no.get() == 4);
        let result = evt_log
            .persist("type-0", &0, None, &"type-0-0-C".to_string(), &to_bytes)
            .await;
        assert_matches!(result, Ok(seq_no) if seq_no.get() == 5);

        let evts = evt_log
            .evts_by_id::<String, _, _>("type-0", &0, NonZeroU64::MIN, from_bytes)
            .await;
        assert!(evts.is_ok());
        let evts = evts.unwrap().try_collect::<Vec<_>>().await;
        assert_matches!(evts, Ok(evts) if evts == vec![
            (1.try_into().unwrap(), "type-0-0-A".to_string()),
            (2.try_into().unwrap(), "type-0-0-B".to_string()),
            (5.try_into().unwrap(), "type-0-0-C".to_string()),
        ]);

        let evts = evt_log
            .evts_by_id::<String, _, _>("type-0", &0, 3.try_into().unwrap(), from_bytes)
            .await;
        assert!(evts.is_ok());
        let evts = evts.unwrap().try_collect::<Vec<_>>().await;
        assert_matches!(evts, Ok(evts) if evts == vec![
            (5.try_into().unwrap(), "type-0-0-C".to_string())
        ]);

        let evts = evt_log
            .evts_by_type::<String, _, _>("type-0", 2.try_into().unwrap(), from_bytes)
            .await;
        assert!(evts.is_ok());
        let evts = evts.unwrap().try_collect::<Vec<_>>().await;
        assert!(evts.is_ok());
        let evts = evts.unwrap();
        assert_eq!(evts.len(), 3);
        assert!(evts.contains(&(2.try_into().unwrap(), "type-0-0-B".to_string())));
        assert!(evts.contains(&(3.try_into().unwrap(), "type-0-1-A".to_string())));
        assert!(evts.contains(&(5.try_into().unwrap(), "type-0-0-C".to_string())));
    }
}
