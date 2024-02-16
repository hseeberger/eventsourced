//! Conversion to [Bytes] for any type that implements [Serialize] and from any type that implements
//! [Deserialize] based upon [serde_json](https://docs.rs/serde_json/latest/serde_json).

use crate::convert::Convert;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::{from_slice, to_value, Error};

#[derive(Debug, Clone, Copy)]
pub struct SerdeJsonConvert;

impl<E, S> Convert<E, S> for SerdeJsonConvert
where
    for<'de> E: Serialize + Deserialize<'de>,
    for<'de> S: Serialize + Deserialize<'de>,
{
    type EvtToBytesError = serde_json::Error;
    type EvtFromBytesError = serde_json::Error;

    type StateToBytesError = serde_json::Error;
    type StateFromBytesError = serde_json::Error;

    fn evt_to_bytes(&self, evt: &E) -> Result<Bytes, Self::EvtToBytesError> {
        to_bytes(evt)
    }

    fn state_to_bytes(&self, state: &S) -> Result<Bytes, Self::StateToBytesError> {
        to_bytes(state)
    }

    fn evt_from_bytes(&self, bytes: Bytes) -> Result<E, Self::EvtFromBytesError> {
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
