//! [TryIntoBytes] and [TryFromBytes] implementations for any type that implements [prost::Message]
//! based upon [prost](https://github.com/tokio-rs/prost).

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
        T::decode(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string() {
        // Prost comes with `Message` implementations for basic types like `String`.
        let s = "test".to_string();

        let bytes = s.try_into_bytes();
        assert!(bytes.is_ok());
        let bytes = bytes.unwrap();

        let s_2 = <String as TryFromBytes>::try_from_bytes(bytes);
        assert!(s_2.is_ok());
        let s_2 = s_2.unwrap();
        assert_eq!(s_2, s);
    }
}
