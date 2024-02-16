//! Conversion to and from [Bytes] for any type that implements [Message] based upon
//! [prost](https://github.com/tokio-rs/prost).

use crate::convert::Convert;
use bytes::{Bytes, BytesMut};
use prost::{DecodeError, EncodeError, Message};

#[derive(Debug, Clone, Copy)]
pub struct ProstConvert;

impl<E, S> Convert<E, S> for ProstConvert
where
    E: Message + Default,
    S: Message + Default,
{
    type EvtToBytesError = EncodeError;
    type EvtFromBytesError = DecodeError;

    type StateToBytesError = EncodeError;
    type StateFromBytesError = DecodeError;

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

pub fn to_bytes<T>(value: &T) -> Result<Bytes, EncodeError>
where
    T: Message,
{
    let mut bytes = BytesMut::new();
    value.encode(&mut bytes)?;
    Ok(bytes.into())
}

pub fn from_bytes<T>(bytes: Bytes) -> Result<T, DecodeError>
where
    T: Message + Default,
{
    T::decode(bytes)
}
