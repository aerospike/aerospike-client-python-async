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

//! Process-global Tokio runtime configuration for `pyo3-async-runtimes`.
//!
//! All async PAC operations dispatch through the runtime returned by
//! `pyo3_async_runtimes::tokio::get_runtime()`.  By default that's a Tokio
//! `Builder::new_multi_thread()` runtime with `enable_all()` and a worker
//! count equal to `available_parallelism()` (CPU count on most systems).
//!
//! [`init`] replaces that default with an explicitly configured builder:
//!
//! 1. **`enable_io() + enable_time()` instead of `enable_all()`.**  `enable_all`
//!    also enables Tokio's signal driver, which fights Python's own signal
//!    handling (Ctrl-C delivery in particular) on the affected process.
//!    We only need I/O + timers, so opt in narrowly.
//!
//! 2. **Worker-count knob via `AEROSPIKE_PAC_RUNTIME_WORKERS` env var.**
//!    On free-threaded multi-loop deployments (an `AsyncPool` with N event
//!    loops on N OS threads) the process already runs N pool threads doing
//!    Python work.  pyo3-async-runtimes' default of `cpu_count` Tokio
//!    workers adds another N threads on top, giving roughly 2N threads
//!    contending for N cores — the kernel-overhead bill shows up as
//!    elevated `sys` CPU and context-switch volume.  Lowering Tokio worker
//!    count to a small constant (e.g. 2–4) often improves throughput at
//!    high loop counts even though it reduces I/O parallelism, because the
//!    aerospike-core async stack is already well-pipelined per worker.
//!
//!    Default behavior (no env var): unchanged — Tokio picks
//!    `available_parallelism()` workers.  Set the env var to override.
//!
//! [`init`] must be called from the module init function *before* any code
//! path that calls `future_into_py` / `batched_future_into_py` (i.e. before
//! the first `await` on a PAC awaitable).  pyo3-async-runtimes builds the
//! runtime lazily on the first `get_runtime()` call, so as long as `init`
//! runs at module import time the override sticks.

use log::{info, warn};
use tokio::runtime::Builder;

const ENV_VAR: &str = "AEROSPIKE_PAC_RUNTIME_WORKERS";
const MAX_WORKERS: usize = 32;

/// Read the configured worker count from the env var, or `None` if unset /
/// unparsable.  Values are clamped to `[1, MAX_WORKERS]`.
fn configured_workers() -> Option<usize> {
    let raw = std::env::var(ENV_VAR).ok()?;
    let parsed: usize = raw.parse().ok()?;
    let clamped = parsed.clamp(1, MAX_WORKERS);
    if clamped != parsed {
        warn!(
            "{ENV_VAR}={parsed} out of range [1, {MAX_WORKERS}], using {clamped}"
        );
    }
    Some(clamped)
}

/// Install the customized Tokio runtime builder.
pub(crate) fn init() {
    let mut builder = Builder::new_multi_thread();
    builder.enable_io().enable_time();

    if let Some(workers) = configured_workers() {
        info!(
            "Tokio runtime worker_threads={workers} (from {ENV_VAR})"
        );
        builder.worker_threads(workers);
    }

    pyo3_async_runtimes::tokio::init(builder);
}
