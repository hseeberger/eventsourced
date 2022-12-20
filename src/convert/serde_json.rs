//! [Binarize] implementation for any type that implements [Serialize] and [Debinarize]
//! implementation for any type that implements [DeserializeOwned] based upon
//! [serde_json](https://docs.rs/serde_json/latest/serde_json).

use super::{TryFromBytes, TryIntoBytes};
use bytes::Bytes;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{to_value, Error};

impl<T> TryIntoBytes for T
where
    T: Serialize + ?Sized,
{
    type Error = Error;

    fn try_into_bytes(&self) -> Result<Bytes, Self::Error> {
        to_value(self).map(|value| value.to_string().into())
    }
}

impl<T> TryFromBytes for T
where
    T: DeserializeOwned,
{
    type Error = Error;

    fn try_from_bytes(bytes: Bytes) -> Result<Self, Self::Error> {
        serde_json::from_slice::<Self>(&bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    struct Foo(u64);

    #[test]
    fn test() {
        let foo = Foo(42);

        let bytes = foo.try_into_bytes();
        assert!(bytes.is_ok());
        let bytes = bytes.unwrap();

        let bar = <Foo as TryFromBytes>::try_from_bytes(bytes);
        assert!(bar.is_ok());
        let bar = bar.unwrap();
        assert_eq!(bar, foo);
    }
}
