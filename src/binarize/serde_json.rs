//! [Binarize] implementation for any type that implements [Serialize] and [Debinarize]
//! implementation for any type that implements [DeserializeOwned].

use super::{Binarize, Debinarize};
use bytes::Bytes;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{to_value, Error};

trait SerdeJsonBinarize {
    fn to_bytes_serde_json(&self) -> Result<Bytes, Error>;
}

impl<T> SerdeJsonBinarize for T
where
    T: Serialize + ?Sized,
{
    fn to_bytes_serde_json(&self) -> Result<Bytes, Error> {
        to_value(self).map(|value| value.to_string().into())
    }
}

impl<T> Binarize for T
where
    T: SerdeJsonBinarize + ?Sized,
{
    type Error = Error;

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        self.to_bytes_serde_json()
    }
}

pub trait SerdeJsonDebinarize {
    type Ok;

    fn from_bytes_serde_json(bytes: Bytes) -> Result<Self::Ok, Error>;
}

impl<T> SerdeJsonDebinarize for T
where
    T: DeserializeOwned,
{
    type Ok = T;

    fn from_bytes_serde_json(bytes: Bytes) -> Result<Self::Ok, Error> {
        serde_json::from_slice::<Self::Ok>(&bytes)
    }
}

impl<T> Debinarize for T
where
    T: SerdeJsonDebinarize,
{
    type Ok = T::Ok;

    type Error = Error;

    fn from_bytes(bytes: Bytes) -> Result<Self::Ok, Self::Error> {
        <T as SerdeJsonDebinarize>::from_bytes_serde_json(bytes)
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
