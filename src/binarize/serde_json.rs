//! [Binarize] implementation for any type that implements [Serialize] and [Debinarize]
//! implementation for any type that implements [DeserializeOwned] based upon
//! [serde_json](https://docs.rs/serde_json/latest/serde_json).

use super::{Binarize, Debinarize};
use bytes::Bytes;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{to_value, Error};

impl<T> Binarize for T
where
    T: Serialize + ?Sized,
{
    type Error = Error;

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        to_value(self).map(|value| value.to_string().into())
    }
}

impl<T> Debinarize for T
where
    T: DeserializeOwned,
{
    type Error = Error;

    fn from_bytes(bytes: Bytes) -> Result<Self, Self::Error> {
        serde_json::from_slice::<Self>(&bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn test_string() {
        let s = "test".to_string();

        let bytes = s.to_bytes();
        assert!(bytes.is_ok());
        let bytes = bytes.unwrap();

        let value = serde_json::from_slice::<Value>(&bytes);
        assert!(value.is_ok());
        let value = value.unwrap();
        assert_eq!(value, Value::String("test".to_string()));

        let s_2 = <String as Debinarize>::from_bytes(bytes);
        assert!(s_2.is_ok());
        let s_2 = s_2.unwrap();
        assert_eq!(s_2, s);
    }

    #[test]
    fn test_vec() {
        let numbers = vec![1, 2, 3];

        let bytes = numbers.to_bytes();
        assert!(bytes.is_ok());
        let bytes = bytes.unwrap();

        let numbers_2 = <Vec<i32> as Debinarize>::from_bytes(bytes);
        assert!(numbers_2.is_ok());
        let numbers_2 = numbers_2.unwrap();
        assert_eq!(numbers_2, numbers);
    }
}
