/// An event and an optional tag. Typically not used direcly, but via [IntoTaggedEvt] or [EvtExt].
pub struct TaggedEvt<E> {
    pub(crate) evt: E,
    pub(crate) tag: Option<String>,
}

/// Used in an [super::EventSourced] command handler as impl trait in return position. Together with
/// its blanket implementation for any event allows for returning plain events without boilerplate.
pub trait IntoTaggedEvt<E>: Send {
    fn into_tagged_evt(self) -> TaggedEvt<E>;
}

impl<E> IntoTaggedEvt<E> for E
where
    E: Send,
{
    fn into_tagged_evt(self) -> TaggedEvt<E> {
        TaggedEvt {
            evt: self,
            tag: None,
        }
    }
}

impl<E> IntoTaggedEvt<E> for TaggedEvt<E>
where
    E: Send,
{
    fn into_tagged_evt(self) -> TaggedEvt<E> {
        self
    }
}

/// Provide `with_tag` extension method for events. Together with its blanket implementation for any
/// event allows for calling `with_tag` on any event type.
pub trait EvtExt: Sized {
    /// Create a [TaggedEvt] with the given tag.
    fn with_tag<T>(self, tag: T) -> TaggedEvt<Self>
    where
        T: Into<String>;
}

impl<E> EvtExt for E {
    fn with_tag<T>(self, tag: T) -> TaggedEvt<E>
    where
        T: Into<String>,
    {
        TaggedEvt {
            evt: self,
            tag: Some(tag.into()),
        }
    }
}
