//! Conversion to [Bytes] for any type that implements [Serialize] and from any type that implements
//! [Deserialize] based upon [serde_json](https://docs.rs/serde_json/latest/serde_json).

use crate::binarize::Binarize;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::{from_slice, to_value, Error};

#[derive(Debug, Clone, Copy)]
pub struct SerdeJsonBinarize;

impl<E, S> Binarize<E, S> for SerdeJsonBinarize
where
    for<'de> E: Serialize + Deserialize<'de>,
    for<'de> S: Serialize + Deserialize<'de>,
{
    type EventToBytesError = serde_json::Error;
    type EventFromBytesError = serde_json::Error;

    type StateToBytesError = serde_json::Error;
    type StateFromBytesError = serde_json::Error;

    fn event_to_bytes(&self, event: &E) -> Result<Bytes, Self::EventToBytesError> {
        to_bytes(event)
    }

    fn state_to_bytes(&self, state: &S) -> Result<Bytes, Self::StateToBytesError> {
        to_bytes(state)
    }

    fn event_from_bytes(&self, bytes: Bytes) -> Result<E, Self::EventFromBytesError> {
        from_bytes(bytes)
    }

    fn state_from_bytes(&self, bytes: Bytes) -> Result<S, Self::StateFromBytesError> {
        from_bytes(bytes)
    }
}

pub fn to_bytes<T>(value: &T) -> Result<Bytes, Error>
where
    T: Serialize,
{
    to_value(value).map(|value| value.to_string().into())
}

pub fn from_bytes<T>(bytes: Bytes) -> Result<T, Error>
where
    for<'de> T: Deserialize<'de>,
{
    from_slice::<T>(&bytes)
}
