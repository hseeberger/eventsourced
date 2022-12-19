//! [Binarize] implementation for any type that implements [Serialize] and [Debinarize]
//! implementation for any type that implements [DeserializeOwned] based upon
//! [Flexbuffers](https://docs.rs/flexbuffers/latest/flexbuffers/index.html).

use super::{TryFromBytes, TryIntoBytes};
use flexbuffers::{DeserializationError, FlexbufferSerializer, Reader, SerializationError};
use serde::{de::DeserializeOwned, Serialize};

impl<T> TryIntoBytes for T
where
    T: Serialize + ?Sized,
{
    type Error = SerializationError;

    fn try_into_bytes(&self) -> Result<Vec<u8>, Self::Error> {
        let mut serializer = FlexbufferSerializer::new();
        self.serialize(&mut serializer)?;
        Ok(serializer.take_buffer().into())
    }
}

impl<T> TryFromBytes for T
where
    T: DeserializeOwned,
{
    type Error = DeserializationError;

    fn try_from_bytes(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        let reader = Reader::get_root(bytes.as_ref())?;
        Self::deserialize(reader)
    }
}
