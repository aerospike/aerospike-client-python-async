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

//! Per-`Client` Tokio runtime.
//!
//! The default PAC topology is one global multi-thread Tokio runtime feeding
//! all Clients via `pyo3_async_runtimes::tokio::get_runtime()`. Under
//! AsyncPool with N event loops, all N loops contend on that shared runtime's
//! scheduler — work stealing across loops, completions on any worker dispatch
//! to any loop. Empirically this collapses past ~4 loops on an 8-core box.
//!
//! Solution: give each `Client` its own dedicated Tokio runtime. Loops become
//! fully independent — no shared scheduler, no cross-loop stealing. The hot
//! path still goes through the existing batched `CompletionBridge` (which
//! already amortizes `call_soon_threadsafe` over batches), only the spawn
//! target changes.
//!
//! Opt-in per Client via `ClientPolicy.per_client_runtime_workers = Some(N)`.
//! PSDK's AsyncPool sets this automatically based on `pool_loops` and
//! `os.cpu_count()`. Single-Client users keep the default (`None`) and get
//! the global-runtime behavior.

use tokio::runtime::{Builder, Handle, Runtime};

/// A self-contained Tokio runtime owned by one `Client`.
///
/// Built with `worker_threads(N)` — N dedicated Tokio worker threads are
/// auto-spawned when the runtime is constructed, and torn down when it's
/// dropped. Using multi-thread mode (rather than current-thread) keeps the
/// I/O driver pattern PAC already relies on and avoids the asyncio-thread-
/// ownership obstacle (asyncio's `epoll_wait` owns its thread; a
/// current-thread Tokio runtime can't share it).
pub(crate) struct ClientRuntime {
    // Order matters: `handle` is just a borrow; `_rt` must outlive any
    // spawned tasks. Dropping the runtime stops the worker threads.
    handle: Handle,
    _rt: Runtime,
}

impl ClientRuntime {
    /// Create a new per-Client runtime with the given worker thread count.
    /// `workers` must be >= 1.
    pub(crate) fn new(workers: usize) -> std::io::Result<Self> {
        let workers = workers.max(1);
        let rt = Builder::new_multi_thread()
            .worker_threads(workers)
            .enable_all()
            .thread_name("pac-client-rt")
            .build()?;
        let handle = rt.handle().clone();
        Ok(ClientRuntime { handle, _rt: rt })
    }

    pub(crate) fn handle(&self) -> &Handle {
        &self.handle
    }
}
