//! Conversion to and from [Bytes](bytes::Bytes) for any type that implements
//! [Message](prost::Message) based upon [prost](https://github.com/tokio-rs/prost).

use crate::Binarizer;
use bytes::{Bytes, BytesMut};
use prost::{DecodeError, EncodeError, Message};

pub fn binarizer<E, S>() -> Binarizer<
    for<'a> fn(&'a E) -> Result<bytes::Bytes, EncodeError>,
    fn(bytes::Bytes) -> Result<E, DecodeError>,
    for<'a> fn(&'a S) -> Result<bytes::Bytes, EncodeError>,
    fn(bytes::Bytes) -> Result<S, DecodeError>,
>
where
    E: Message + Default,
    S: Message + Default,
{
    Binarizer {
        evt_to_bytes: to_bytes::<E>,
        evt_from_bytes: from_bytes::<E>,
        state_to_bytes: to_bytes::<S>,
        state_from_bytes: from_bytes::<S>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_prost() {
        // Prost comes with `Message` implementations for basic types like `String`.
        let s = "test".to_string();

        let bytes = to_bytes(&s);
        assert!(bytes.is_ok());
        let bytes = bytes.unwrap();

        let s_2 = from_bytes::<String>(bytes);
        assert!(s_2.is_ok());
        let s_2 = s_2.unwrap();
        assert_eq!(s_2, s);
    }
}
