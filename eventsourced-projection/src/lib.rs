pub mod postgres;

use anyhow::anyhow;
use std::error::Error as StdError;

/// Format the given error with its whole error chain, implemented by using `anyhow`.
fn error_chain<E>(error: E) -> String
where
    E: StdError + Send + Sync + 'static,
{
    format!("{:#}", anyhow!(error))
}
