use super::{TryFromBytes, TryIntoBytes};
use bytes::{Bytes, BytesMut};
use prost::{DecodeError, EncodeError, Message};

impl<T> TryIntoBytes for T
where
    T: Message,
{
    type Error = EncodeError;

    fn try_into_bytes(&self) -> Result<Bytes, Self::Error> {
        let mut bytes = BytesMut::new();
        self.encode(&mut bytes)?;
        Ok(bytes.into())
    }
}

impl<T> TryFromBytes for T
where
    T: Message + Default,
{
    type Error = DecodeError;

    fn try_from_bytes(bytes: Bytes) -> Result<Self, Self::Error> {
        T::decode(Bytes::from(bytes))
    }
}
