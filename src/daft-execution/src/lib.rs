#![feature(return_position_impl_trait_in_trait)]
#![feature(async_fn_in_trait)]
#![feature(impl_trait_in_assoc_type)]

mod compute;
mod executor;
mod runner;
mod stage;

use common_error::DaftError;
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

type Result<T, E = Error> = std::result::Result<T, E>;

// #[cfg(feature = "python")]
// use execution_plan::PyExecutor;
// #[cfg(feature = "python")]
// use pyo3::prelude::*;

// #[cfg(feature = "python")]
// pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
//     parent.add_class::<PyExecutor>()?;

//     Ok(())
// }
