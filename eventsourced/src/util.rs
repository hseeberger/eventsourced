use futures::Stream;
use pin_project::pin_project;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

pub trait StreamExt
where
    Self: Sized,
{
    fn take_until_predicate<P>(self, predicate: P) -> TakeUntil<Self, P>
    where
        Self: Stream,
        P: Fn(&Self::Item) -> bool,
    {
        TakeUntil {
            inner: self,
            predicate,
            done: false,
        }
    }
}

impl<S> StreamExt for S where S: Stream {}

#[pin_project]
pub struct TakeUntil<S, P> {
    #[pin]
    inner: S,
    predicate: P,
    done: bool,
}

impl<S, P> Stream for TakeUntil<S, P>
where
    S: Stream,
    P: Fn(&S::Item) -> bool,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        if *this.done {
            return Poll::Ready(None);
        }

        match this.inner.poll_next(cx) {
            Poll::Ready(Some(item)) => {
                if (this.predicate)(&item) {
                    *this.done = true;
                }
                Poll::Ready(Some(item))
            }

            Poll::Ready(None) => Poll::Ready(None),

            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::StreamExt as EventSourcedStreamExt;
    use futures::{stream, StreamExt};

    #[tokio::test]
    async fn test_take_until_predicate() {
        let numbers = stream::iter(vec![1, 2, 3, -1, -2, -3]);
        let numbers = numbers
            .take_until_predicate(|n: &i32| *n < 1)
            .collect::<Vec<_>>()
            .await;
        assert_eq!(numbers, vec![1, 2, 3, -1]);
    }
}
