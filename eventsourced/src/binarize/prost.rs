//! Conversion to and from [Bytes] for any type that implements [Message] based upon
//! [prost](https://github.com/tokio-rs/prost).

use crate::binarize::Binarize;
use bytes::{Bytes, BytesMut};
use prost::{DecodeError, EncodeError, Message};

#[derive(Debug, Clone, Copy)]
pub struct ProstBinarize;

impl<E, S> Binarize<E, S> for ProstBinarize
where
    E: Message + Default,
    S: Message + Default,
{
    type EventToBytesError = EncodeError;
    type EventFromBytesError = DecodeError;

    type StateToBytesError = EncodeError;
    type StateFromBytesError = DecodeError;

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
