#![feature(impl_trait_in_assoc_type)]

mod compute;
mod executor;
mod runner;
mod stage;

use common_error::DaftError;
pub use runner::{run_local_async, run_local_sync};
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error joining spawned task: {}", source))]
    JoinError { source: tokio::task::JoinError },
    #[snafu(display(
        "Sender of OneShot Channel Dropped before sending data over: {}",
        source
    ))]
    OneShotRecvError {
        source: tokio::sync::oneshot::error::RecvError,
    },
}

impl From<Error> for DaftError {
    fn from(err: Error) -> DaftError {
        match err {
            _ => DaftError::External(err.into()),
        }
    }
}
