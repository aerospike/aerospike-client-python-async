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

//! Batched completion bridge for async Python futures.
//!
//! Instead of each Rust future completion independently calling
//! `spawn_blocking` + `Python::attach` + `call_soon_threadsafe`,
//! completions push results into a shared queue and a single
//! `call_soon_threadsafe` per batch wakes the event loop to drain them.
//!
//! Under 32-way concurrency this collapses 32 GIL acquisitions per
//! completion burst into 1, eliminating the "completion storm".

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};

use futures::FutureExt;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;

use pyo3_async_runtimes::tokio as pyo3_asyncio;

type Converter = Box<dyn FnOnce(Python<'_>) -> PyResult<Py<PyAny>> + Send>;

struct PendingResult {
    future: Py<PyAny>,
    result: Result<Converter, PyErr>,
}

// SAFETY: Py<PyAny> is Send+Sync; Converter is Send; PyErr is Send.
unsafe impl Send for PendingResult {}

static QUEUE: Mutex<Vec<PendingResult>> = Mutex::new(Vec::new());
static DRAIN_SCHEDULED: AtomicBool = AtomicBool::new(false);
static DRAINER: OnceLock<Py<CompletionDrainer>> = OnceLock::new();

/// Python-callable object that drains the completion queue.
/// Registered once at module init; invoked on the event loop thread
/// via `call_soon_threadsafe`.
#[pyclass]
pub(crate) struct CompletionDrainer;

#[pymethods]
impl CompletionDrainer {
    fn __call__(&self, py: Python<'_>) -> PyResult<()> {
        drain(py);
        Ok(())
    }
}

fn drain(py: Python<'_>) {
    // Allow new batches to schedule while we process.
    DRAIN_SCHEDULED.store(false, Ordering::Release);
    let pending: Vec<PendingResult> = std::mem::take(&mut *QUEUE.lock().unwrap());
    for pr in pending {
        let future = pr.future.bind(py);
        // Skip the cancelled() check: set_result/set_exception on a cancelled
        // future raises InvalidStateError, which the `let _` already discards.
        // Avoiding the check saves 3 Python dispatches per completion on the
        // hot path (getattr + call0 + is_truthy), which at 25k TPS is
        // ~75k redundant Python calls/sec eliminated.
        match pr.result {
            Ok(converter) => match converter(py) {
                Ok(val) => {
                    let _ = future.call_method1(pyo3::intern!(py, "set_result"), (val,));
                }
                Err(e) => {
                    let _ = future.call_method1(pyo3::intern!(py, "set_exception"), (e,));
                }
            },
            Err(e) => {
                let _ = future.call_method1(pyo3::intern!(py, "set_exception"), (e,));
            }
        }
    }
}

fn enqueue(event_loop: Py<PyAny>, pr: PendingResult) {
    QUEUE.lock().unwrap().push(pr);
    if !DRAIN_SCHEDULED.swap(true, Ordering::AcqRel) {
        // First completion in this batch — schedule a drain callback.
        // This is the only GIL acquisition from the Tokio side per batch.
        Python::attach(|py| {
            let drainer = DRAINER.get().expect("completion bridge not initialized");
            let el = event_loop.bind(py);
            let _ = el.call_method1(
                pyo3::intern!(py, "call_soon_threadsafe"),
                (drainer.bind(py),),
            );
        });
    }
}

/// Initialize the bridge.  Call once from the module init function.
pub(crate) fn init(py: Python<'_>) -> PyResult<()> {
    let drainer = Py::new(py, CompletionDrainer)?;
    let _ = DRAINER.set(drainer);
    Ok(())
}

/// Batched replacement for `pyo3_asyncio::future_into_py`.
///
/// Creates an asyncio `Future`, spawns the Rust future on Tokio, and on
/// completion enqueues the result for batch delivery to the event loop.
/// No `spawn_blocking` is used on the completion path.
pub(crate) fn batched_future_into_py<'py, F, T>(
    py: Python<'py>,
    fut: F,
) -> PyResult<Bound<'py, PyAny>>
where
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: for<'a> IntoPyObject<'a> + Send + 'static,
{
    let locals = pyo3_asyncio::get_current_locals(py)?;
    let event_loop = locals.event_loop(py);
    let py_fut = event_loop.call_method0(pyo3::intern!(py, "create_future"))?;

    let future_ref: Py<PyAny> = py_fut.clone().unbind();
    let event_loop_ref: Py<PyAny> = event_loop.clone().unbind();

    pyo3_asyncio::get_runtime().spawn(async move {
        let pr = match AssertUnwindSafe(fut).catch_unwind().await {
            Ok(Ok(val)) => PendingResult {
                future: future_ref,
                result: Ok(Box::new(move |py| val.into_py_any(py))),
            },
            Ok(Err(e)) => PendingResult {
                future: future_ref,
                result: Err(e),
            },
            Err(panic) => {
                let msg = if let Some(s) = panic.downcast_ref::<String>() {
                    s.clone()
                } else if let Some(s) = panic.downcast_ref::<&str>() {
                    s.to_string()
                } else {
                    "Rust panic in async task".to_string()
                };
                PendingResult {
                    future: future_ref,
                    result: Err(pyo3::exceptions::PyRuntimeError::new_err(msg)),
                }
            }
        };
        enqueue(event_loop_ref, pr);
    });

    Ok(py_fut)
}
