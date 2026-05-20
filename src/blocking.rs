// Copyright 2025-2026 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

//! Shared infrastructure for the ``*_blocking`` PyO3 method surface.
//!
//! Every blocking entry point goes through :func:`run_blocking`, which:
//!
//! 1. Rejects calls made from inside a running asyncio event loop (the
//!    user should use the async variant + ``await`` instead).
//! 2. Releases the GIL and drives the supplied future on the global
//!    Tokio runtime, returning when it completes.
//!
//! Moving these helpers into their own module keeps them reachable from
//! both ``lib.rs`` (client / op blocking methods) and ``tasks.rs`` (task
//! blocking methods) without bumping their visibility past ``pub(crate)``.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3_stub_gen::derive::gen_stub_pyfunction;

use crate::errors::RustClientError;
use crate::policies::ClientPolicy;
use crate::Client;

/// Reject a blocking call invoked from inside a running asyncio loop.
///
/// Async-context misuse (running a blocking method from within
/// ``asyncio.run`` or similar) is rejected up front so we never end up
/// calling ``block_on`` from a thread that already owns an asyncio loop —
/// which would deadlock the loop or starve the caller's await.
pub(crate) fn check_not_in_async_context(py: Python<'_>) -> PyResult<()> {
    let asyncio = py.import("asyncio")?;
    if asyncio.call_method0("get_running_loop").is_ok() {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(
            "Cannot call a blocking method from within an async context \
             (a running asyncio event loop was detected). Use the async \
             method and `await` it instead.",
        ));
    }
    Ok(())
}

/// Run a ``Send`` future to completion on the global Tokio runtime while
/// the GIL is released.
///
/// The future is moved in, polled by ``block_on`` on the calling thread;
/// aerospike-core dispatches I/O to its workers internally. The caller
/// wraps any pyclass returns post-detach (the GIL is reacquired
/// automatically on return).
pub(crate) fn run_blocking<Fut, T>(py: Python<'_>, fut: Fut) -> PyResult<T>
where
    Fut: std::future::Future<Output = PyResult<T>> + Send,
    T: Send,
{
    check_not_in_async_context(py)?;
    let rt = pyo3_async_runtimes::tokio::get_runtime();
    py.detach(move || rt.block_on(fut))
}

/// Synchronously create and connect a Client.
///
/// Unlike :func:`new_client`, this function does not require a running
/// asyncio event loop. The returned :class:`Client` can only be used with
/// the ``_blocking`` method variants — calling an async method on a client
/// built this way raises ``RuntimeError`` because the completion bridge
/// (which captures an asyncio loop at construction time) is not initialized.
#[gen_stub_pyfunction(module = "_aerospike_async_native")]
#[pyfunction]
pub(crate) fn new_client_blocking(
    py: Python<'_>,
    policy: ClientPolicy,
    seeds: String,
) -> PyResult<Client> {
    let as_policy = policy._as.clone();
    let as_seeds = seeds.clone();
    let raw = run_blocking(py, async move {
        log::debug!(target: "aerospike_async", "connecting (blocking) to {}", as_seeds);
        aerospike_core::Client::new(&as_policy, &as_seeds)
            .await
            .map_err(|e| PyErr::from(RustClientError(e)))
    })?;
    log::debug!(target: "aerospike_async", "connected (blocking) to {}", seeds);
    Ok(Client {
        _as: Arc::new(raw),
        seeds,
        bridge: None,
    })
}
