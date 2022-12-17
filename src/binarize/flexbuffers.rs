//! [Binarize] implementation for any type that implements [Serialize] and [Debinarize]
//! implementation for any type that implements [DeserializeOwned] based upon
//! [Flexbuffers](https://docs.rs/flexbuffers/latest/flexbuffers/index.html).

use super::{Binarize, Debinarize};
use bytes::Bytes;
use flexbuffers::{DeserializationError, FlexbufferSerializer, Reader, SerializationError};
use serde::{de::DeserializeOwned, Serialize};

impl<T> Binarize for T
where
    T: Serialize + ?Sized,
{
    type Error = SerializationError;

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let mut serializer = FlexbufferSerializer::new();
        self.serialize(&mut serializer)?;
        Ok(serializer.take_buffer().into())
    }
}

impl<T> Debinarize for T
where
    T: DeserializeOwned,
{
    type Ok = T;

    type Error = DeserializationError;

    fn from_bytes(bytes: Bytes) -> Result<Self::Ok, Self::Error> {
        let reader = Reader::get_root(bytes.as_ref())?;
        Self::Ok::deserialize(reader)
    }
}
