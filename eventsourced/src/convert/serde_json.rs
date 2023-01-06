//! Conversion to [Bytes](bytes::Bytes) for any type that implements [Serialize](serde::Serialize)
//! and from any type that implements [DeserializeOwned](serde::de::DeserializeOwned) based upon
//! [serde_json](https://docs.rs/serde_json/latest/serde_json).

use crate::Binarizer;
use bytes::Bytes;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{from_slice, to_value, Error};

pub fn binarizer<E, S>() -> Binarizer<
    for<'a> fn(&'a E) -> Result<bytes::Bytes, Error>,
    fn(bytes::Bytes) -> Result<E, Error>,
    for<'a> fn(&'a S) -> Result<bytes::Bytes, Error>,
    fn(bytes::Bytes) -> Result<S, Error>,
>
where
    E: Serialize + DeserializeOwned,
    S: Serialize + DeserializeOwned,
{
    Binarizer {
        evt_to_bytes: to_bytes::<E>,
        evt_from_bytes: from_bytes::<E>,
        state_to_bytes: to_bytes::<S>,
        state_from_bytes: from_bytes::<S>,
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
    T: DeserializeOwned,
{
    from_slice::<T>(&bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    struct Foo(u64);

    #[test]
    fn test_convert_serde_json() {
        let foo = Foo(42);

        let bytes = to_bytes(&foo);
        assert!(bytes.is_ok());
        let bytes = bytes.unwrap();

        let bar = from_bytes::<Foo>(bytes);
        assert!(bar.is_ok());
        let bar = bar.unwrap();
        assert_eq!(bar, foo);
    }
}
