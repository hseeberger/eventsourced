//! [Binarize] implementation for any type that implements [Serialize] and [Debinarize]
//! implementation for any type that implements [DeserializeOwned] based upon
//! [Flexbuffers](https://docs.rs/flexbuffers/latest/flexbuffers/index.html).

use super::{Binarize, Debinarize};
use bytes::Bytes;
use flexbuffers::{DeserializationError, FlexbufferSerializer, Reader, SerializationError};
use serde::{de::DeserializeOwned, Serialize};

trait FlexbuffersBinarize {
    fn to_bytes_flexbuffers(&self) -> Result<Bytes, SerializationError>;
}

impl<T> FlexbuffersBinarize for T
where
    T: Serialize + ?Sized,
{
    fn to_bytes_flexbuffers(&self) -> Result<Bytes, SerializationError> {
        let mut serializer = FlexbufferSerializer::new();
        self.serialize(&mut serializer)?;
        Ok(serializer.take_buffer().into())
    }
}

impl<T> Binarize for T
where
    T: FlexbuffersBinarize + ?Sized,
{
    type Error = SerializationError;

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        self.to_bytes_flexbuffers()
    }
}

pub trait FlexbuffersDebinarize {
    type Ok;

    fn from_bytes_flexbuffers(bytes: Bytes) -> Result<Self::Ok, DeserializationError>;
}

impl<T> FlexbuffersDebinarize for T
where
    T: DeserializeOwned,
{
    type Ok = T;

    fn from_bytes_flexbuffers(bytes: Bytes) -> Result<Self::Ok, DeserializationError> {
        let reader = Reader::get_root(bytes.as_ref())?;
        Self::Ok::deserialize(reader)
    }
}

impl<T> Debinarize for T
where
    T: FlexbuffersDebinarize,
{
    type Ok = T::Ok;

    type Error = DeserializationError;

    fn from_bytes(bytes: Bytes) -> Result<Self::Ok, Self::Error> {
        <T as FlexbuffersDebinarize>::from_bytes_flexbuffers(bytes)
    }
}
