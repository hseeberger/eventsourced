use std::{
    fmt::{self, Display},
    num::NonZeroU64,
};
use thiserror::Error;

/// Sequence number used for events by event log and snapshot store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SeqNo(pub(crate) NonZeroU64);

impl SeqNo {
    #[allow(missing_docs)]
    pub const MIN: SeqNo = Self(unsafe { NonZeroU64::new_unchecked(1) });

    #[allow(missing_docs)]
    pub const fn new(value: NonZeroU64) -> Self {
        Self(value)
    }

    #[allow(missing_docs)]
    pub const fn as_u64(&self) -> u64 {
        self.0.get()
    }

    /// Get the successor of this sequence number.
    pub fn succ(&self) -> Self {
        let seq_no = self.0.checked_add(1).expect("overflow");
        Self(seq_no)
    }
}

impl TryFrom<u64> for SeqNo {
    type Error = ZeroSeqNoError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        NonZeroU64::new(value).ok_or(ZeroSeqNoError).map(Self::new)
    }
}

/// Error signaling that a sequence number must not be zero.
#[derive(Debug, Error)]
#[error("SeqNo must not be zero")]
pub struct ZeroSeqNoError;

impl Display for SeqNo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}
