// Copyright 2023-2026 Aerospike, Inc.
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

use pyo3::prelude::*;

use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyclass_enum, gen_stub_pymethods};

use crate::blocking::run_blocking;
use crate::errors::RustClientError;

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  TaskStatus
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(from_py_object, module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum TaskStatus {
        #[pyo3(name = "NOT_FOUND")]
        NotFound,
        #[pyo3(name = "IN_PROGRESS")]
        InProgress,
        #[pyo3(name = "COMPLETE")]
        Complete,
    }

    impl From<aerospike_core::task::Status> for TaskStatus {
        fn from(status: aerospike_core::task::Status) -> Self {
            match status {
                aerospike_core::task::Status::NotFound => TaskStatus::NotFound,
                aerospike_core::task::Status::InProgress => TaskStatus::InProgress,
                aerospike_core::task::Status::Complete => TaskStatus::Complete,
            }
        }
    }

    #[pymethods]
    impl TaskStatus {
        fn __richcmp__(&self, other: &TaskStatus, op: pyo3::class::basic::CompareOp) -> pyo3::PyResult<bool> {
            match op {
                pyo3::class::basic::CompareOp::Eq => Ok(self == other),
                pyo3::class::basic::CompareOp::Ne => Ok(self != other),
                _ => Err(pyo3::exceptions::PyNotImplementedError::new_err("Only == and != comparisons are supported")),
            }
        }

        fn __hash__(&self) -> u64 {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            self.hash(&mut hasher);
            hasher.finish()
        }
    }

    // Helper function for wait_till_complete implementation
    async fn wait_till_complete_impl<T: aerospike_core::task::Task>(
        task: T,
        sleep_time: f64,
        max_attempts: u32,
    ) -> Result<bool, PyErr> {
        use tokio::time::sleep;
        use std::time::Duration;

        for _attempt in 0..max_attempts {
            let status: aerospike_core::task::Status = task
                .query_status()
                .await
                .map_err(|e| PyErr::from(RustClientError(e)))?;

            match status {
                aerospike_core::task::Status::Complete | aerospike_core::task::Status::NotFound => {
                    return Ok(true);
                }
                aerospike_core::task::Status::InProgress => {
                    sleep(Duration::from_secs_f64(sleep_time)).await;
                }
            }
        }
        Ok(false)
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  RegisterTask
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(from_py_object, subclass, freelist = 1, module = "_aerospike_async_native")]
    pub struct RegisterTask {
        pub(crate) _as: aerospike_core::RegisterTask,
        // Some when built from an async Client method; None when built from
        // the sync `*_blocking` sibling. Async polling routes through the
        // bridge for loop-affinity + per-Client runtime isolation.
        pub(crate) bridge: Option<crate::completion::CompletionBridge>,
    }

    // Manual Clone: CompletionBridge holds a `Py<>` (the drainer) which
    // requires a Python token to clone (`Py::clone_ref(py)`). `derive(Clone)`
    // has signature `fn clone(&self) -> Self` — no token — so it can't drive
    // `clone_ref`. The same constraint holds under GIL (non-atomic refcount
    // protected by exclusive interpreter access) and free-threading
    // (interpreter-attachment lifetime, even though refcount itself is now
    // atomic). Clones drop the bridge; only the original PyClass needs it
    // for async iteration. `from_py_object` (which Clone serves) doesn't
    // iterate, so dropping the bridge there is safe.
    impl Clone for RegisterTask {
        fn clone(&self) -> Self {
            RegisterTask {
                _as: self._as.clone(),
                bridge: None,
            }
        }
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl RegisterTask {
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[TaskStatus]", imports=("typing")))]
        pub fn query_status<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
            let bridge = self.bridge.as_ref().ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "Cannot await query_status() on a task returned from a \
                     `*_blocking` method. Use the `query_status_blocking()` \
                     sibling or rebuild via the async Client method.",
                )
            })?;
            let task = self._as.clone();
            crate::completion::batched_future_into_py(bridge, py, async move {
                use aerospike_core::task::Task;
                let status: aerospike_core::task::Status = task.query_status().await.map_err(|e| PyErr::from(RustClientError(e)))?;
                Ok(TaskStatus::from(status))
            })
        }

        /// Wait for the task to complete, polling status until COMPLETE or NOT_FOUND.
        ///
        /// Args:
        ///     sleep_time: Time to sleep between status checks (seconds). Default: 0.25
        ///     max_attempts: Maximum number of attempts before giving up. Default: 80 (20 seconds)
        ///
        /// Returns:
        ///     True if task completed, False if max attempts reached
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[bool]", imports=("typing")))]
        #[pyo3(signature = (sleep_time = 0.25, max_attempts = 80))]
        pub fn wait_till_complete<'a>(
            &self,
            sleep_time: f64,
            max_attempts: u32,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let bridge = self.bridge.as_ref().ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "Cannot await wait_till_complete() on a task returned from \
                     a `*_blocking` method. Use the `wait_till_complete_blocking()` \
                     sibling or rebuild via the async Client method.",
                )
            })?;
            let task = self._as.clone();
            crate::completion::batched_future_into_py(bridge, py, async move {
                wait_till_complete_impl(task, sleep_time, max_attempts).await
            })
        }

        /// Blocking sibling of :meth:`query_status` — returns the current
        /// status without spinning up an asyncio loop. Rejects calls made
        /// from inside a running asyncio loop.
        pub fn query_status_blocking(&self, py: Python<'_>) -> PyResult<TaskStatus> {
            let task = self._as.clone();
            run_blocking(py, async move {
                use aerospike_core::task::Task;
                let status: aerospike_core::task::Status = task
                    .query_status()
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;
                Ok(TaskStatus::from(status))
            })
        }

        /// Blocking sibling of :meth:`wait_till_complete` — polls status
        /// until COMPLETE or NOT_FOUND without needing an asyncio loop.
        /// Rejects calls made from inside a running asyncio loop.
        ///
        /// Args:
        ///     sleep_time: Time to sleep between status checks (seconds). Default: 0.25
        ///     max_attempts: Maximum number of attempts before giving up. Default: 80 (20 seconds)
        ///
        /// Returns:
        ///     True if task completed, False if max attempts reached
        #[pyo3(signature = (sleep_time = 0.25, max_attempts = 80))]
        pub fn wait_till_complete_blocking(
            &self,
            sleep_time: f64,
            max_attempts: u32,
            py: Python<'_>,
        ) -> PyResult<bool> {
            let task = self._as.clone();
            run_blocking(py, async move {
                wait_till_complete_impl(task, sleep_time, max_attempts).await
            })
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  UdfRemoveTask
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(from_py_object, subclass, freelist = 1, module = "_aerospike_async_native")]
    pub struct UdfRemoveTask {
        pub(crate) _as: aerospike_core::UdfRemoveTask,
        pub(crate) bridge: Option<crate::completion::CompletionBridge>,
    }

    impl Clone for UdfRemoveTask {
        fn clone(&self) -> Self {
            UdfRemoveTask {
                _as: self._as.clone(),
                bridge: None,
            }
        }
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl UdfRemoveTask {
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[TaskStatus]", imports=("typing")))]
        pub fn query_status<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
            let bridge = self.bridge.as_ref().ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "Cannot await query_status() on a task returned from a \
                     `*_blocking` method. Use the `query_status_blocking()` \
                     sibling or rebuild via the async Client method.",
                )
            })?;
            let task = self._as.clone();
            crate::completion::batched_future_into_py(bridge, py, async move {
                use aerospike_core::task::Task;
                let status: aerospike_core::task::Status = task.query_status().await.map_err(|e| PyErr::from(RustClientError(e)))?;
                Ok(TaskStatus::from(status))
            })
        }

        /// Wait for the task to complete, polling status until COMPLETE or NOT_FOUND.
        ///
        /// Args:
        ///     sleep_time: Time to sleep between status checks (seconds). Default: 0.25
        ///     max_attempts: Maximum number of attempts before giving up. Default: 80 (20 seconds)
        ///
        /// Returns:
        ///     True if task completed, False if max attempts reached
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[bool]", imports=("typing")))]
        #[pyo3(signature = (sleep_time = 0.25, max_attempts = 80))]
        pub fn wait_till_complete<'a>(
            &self,
            sleep_time: f64,
            max_attempts: u32,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let bridge = self.bridge.as_ref().ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "Cannot await wait_till_complete() on a task returned from \
                     a `*_blocking` method. Use the `wait_till_complete_blocking()` \
                     sibling or rebuild via the async Client method.",
                )
            })?;
            let task = self._as.clone();
            crate::completion::batched_future_into_py(bridge, py, async move {
                wait_till_complete_impl(task, sleep_time, max_attempts).await
            })
        }

        /// Blocking sibling of :meth:`query_status` — returns the current
        /// status without spinning up an asyncio loop. Rejects calls made
        /// from inside a running asyncio loop.
        pub fn query_status_blocking(&self, py: Python<'_>) -> PyResult<TaskStatus> {
            let task = self._as.clone();
            run_blocking(py, async move {
                use aerospike_core::task::Task;
                let status: aerospike_core::task::Status = task
                    .query_status()
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;
                Ok(TaskStatus::from(status))
            })
        }

        /// Blocking sibling of :meth:`wait_till_complete` — polls status
        /// until COMPLETE or NOT_FOUND without needing an asyncio loop.
        /// Rejects calls made from inside a running asyncio loop.
        ///
        /// Args:
        ///     sleep_time: Time to sleep between status checks (seconds). Default: 0.25
        ///     max_attempts: Maximum number of attempts before giving up. Default: 80 (20 seconds)
        ///
        /// Returns:
        ///     True if task completed, False if max attempts reached
        #[pyo3(signature = (sleep_time = 0.25, max_attempts = 80))]
        pub fn wait_till_complete_blocking(
            &self,
            sleep_time: f64,
            max_attempts: u32,
            py: Python<'_>,
        ) -> PyResult<bool> {
            let task = self._as.clone();
            run_blocking(py, async move {
                wait_till_complete_impl(task, sleep_time, max_attempts).await
            })
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  IndexTask
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(from_py_object, subclass, freelist = 1, module = "_aerospike_async_native")]
    pub struct IndexTask {
        pub(crate) _as: aerospike_core::IndexTask,
        pub(crate) bridge: Option<crate::completion::CompletionBridge>,
    }

    impl Clone for IndexTask {
        fn clone(&self) -> Self {
            IndexTask {
                _as: self._as.clone(),
                bridge: None,
            }
        }
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl IndexTask {
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[TaskStatus]", imports=("typing")))]
        pub fn query_status<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
            let bridge = self.bridge.as_ref().ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "Cannot await query_status() on a task returned from a \
                     `*_blocking` method. Use the `query_status_blocking()` \
                     sibling or rebuild via the async Client method.",
                )
            })?;
            let task = self._as.clone();
            crate::completion::batched_future_into_py(bridge, py, async move {
                use aerospike_core::task::Task;
                let status: aerospike_core::task::Status =
                    task.query_status().await.map_err(|e| PyErr::from(RustClientError(e)))?;
                Ok(TaskStatus::from(status))
            })
        }

        #[gen_stub(override_return_type(type_repr="typing.Awaitable[bool]", imports=("typing")))]
        #[pyo3(signature = (sleep_time = 0.25, max_attempts = 80))]
        pub fn wait_till_complete<'a>(
            &self,
            sleep_time: f64,
            max_attempts: u32,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let bridge = self.bridge.as_ref().ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "Cannot await wait_till_complete() on a task returned from \
                     a `*_blocking` method. Use the `wait_till_complete_blocking()` \
                     sibling or rebuild via the async Client method.",
                )
            })?;
            let task = self._as.clone();
            crate::completion::batched_future_into_py(bridge, py, async move {
                wait_till_complete_impl(task, sleep_time, max_attempts).await
            })
        }

        /// Blocking sibling of :meth:`query_status` — returns the current
        /// status without spinning up an asyncio loop. Rejects calls made
        /// from inside a running asyncio loop.
        pub fn query_status_blocking(&self, py: Python<'_>) -> PyResult<TaskStatus> {
            let task = self._as.clone();
            run_blocking(py, async move {
                use aerospike_core::task::Task;
                let status: aerospike_core::task::Status = task
                    .query_status()
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;
                Ok(TaskStatus::from(status))
            })
        }

        /// Blocking sibling of :meth:`wait_till_complete` — polls status
        /// until COMPLETE or NOT_FOUND without needing an asyncio loop.
        /// Rejects calls made from inside a running asyncio loop.
        ///
        /// Args:
        ///     sleep_time: Time to sleep between status checks (seconds). Default: 0.25
        ///     max_attempts: Maximum number of attempts before giving up. Default: 80 (20 seconds)
        ///
        /// Returns:
        ///     True if task completed, False if max attempts reached
        #[pyo3(signature = (sleep_time = 0.25, max_attempts = 80))]
        pub fn wait_till_complete_blocking(
            &self,
            sleep_time: f64,
            max_attempts: u32,
            py: Python<'_>,
        ) -> PyResult<bool> {
            let task = self._as.clone();
            run_blocking(py, async move {
                wait_till_complete_impl(task, sleep_time, max_attempts).await
            })
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  DropIndexTask
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(from_py_object, subclass, freelist = 1, module = "_aerospike_async_native")]
    pub struct DropIndexTask {
        pub(crate) _as: aerospike_core::DropIndexTask,
        pub(crate) bridge: Option<crate::completion::CompletionBridge>,
    }

    impl Clone for DropIndexTask {
        fn clone(&self) -> Self {
            DropIndexTask {
                _as: self._as.clone(),
                bridge: None,
            }
        }
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl DropIndexTask {
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[TaskStatus]", imports=("typing")))]
        pub fn query_status<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
            let bridge = self.bridge.as_ref().ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "Cannot await query_status() on a task returned from a \
                     `*_blocking` method. Use the `query_status_blocking()` \
                     sibling or rebuild via the async Client method.",
                )
            })?;
            let task = self._as.clone();
            crate::completion::batched_future_into_py(bridge, py, async move {
                use aerospike_core::task::Task;
                let status: aerospike_core::task::Status =
                    task.query_status().await.map_err(|e| PyErr::from(RustClientError(e)))?;
                Ok(TaskStatus::from(status))
            })
        }

        #[gen_stub(override_return_type(type_repr="typing.Awaitable[bool]", imports=("typing")))]
        #[pyo3(signature = (sleep_time = 0.25, max_attempts = 80))]
        pub fn wait_till_complete<'a>(
            &self,
            sleep_time: f64,
            max_attempts: u32,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let bridge = self.bridge.as_ref().ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "Cannot await wait_till_complete() on a task returned from \
                     a `*_blocking` method. Use the `wait_till_complete_blocking()` \
                     sibling or rebuild via the async Client method.",
                )
            })?;
            let task = self._as.clone();
            crate::completion::batched_future_into_py(bridge, py, async move {
                wait_till_complete_impl(task, sleep_time, max_attempts).await
            })
        }

        /// Blocking sibling of :meth:`query_status` — returns the current
        /// status without spinning up an asyncio loop. Rejects calls made
        /// from inside a running asyncio loop.
        pub fn query_status_blocking(&self, py: Python<'_>) -> PyResult<TaskStatus> {
            let task = self._as.clone();
            run_blocking(py, async move {
                use aerospike_core::task::Task;
                let status: aerospike_core::task::Status = task
                    .query_status()
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;
                Ok(TaskStatus::from(status))
            })
        }

        /// Blocking sibling of :meth:`wait_till_complete` — polls status
        /// until COMPLETE or NOT_FOUND without needing an asyncio loop.
        /// Rejects calls made from inside a running asyncio loop.
        ///
        /// Args:
        ///     sleep_time: Time to sleep between status checks (seconds). Default: 0.25
        ///     max_attempts: Maximum number of attempts before giving up. Default: 80 (20 seconds)
        ///
        /// Returns:
        ///     True if task completed, False if max attempts reached
        #[pyo3(signature = (sleep_time = 0.25, max_attempts = 80))]
        pub fn wait_till_complete_blocking(
            &self,
            sleep_time: f64,
            max_attempts: u32,
            py: Python<'_>,
        ) -> PyResult<bool> {
            let task = self._as.clone();
            run_blocking(py, async move {
                wait_till_complete_impl(task, sleep_time, max_attempts).await
            })
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  ExecuteTask
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(from_py_object, subclass, freelist = 1, module = "_aerospike_async_native")]
    pub struct ExecuteTask {
        pub(crate) _as: aerospike_core::ExecuteTask,
        pub(crate) bridge: Option<crate::completion::CompletionBridge>,
    }

    impl Clone for ExecuteTask {
        fn clone(&self) -> Self {
            ExecuteTask {
                _as: self._as.clone(),
                bridge: None,
            }
        }
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl ExecuteTask {
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[TaskStatus]", imports=("typing")))]
        pub fn query_status<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
            let bridge = self.bridge.as_ref().ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "Cannot await query_status() on a task returned from a \
                     `*_blocking` method. Use the `query_status_blocking()` \
                     sibling or rebuild via the async Client method.",
                )
            })?;
            let task = self._as.clone();
            crate::completion::batched_future_into_py(bridge, py, async move {
                use aerospike_core::task::Task;
                let status: aerospike_core::task::Status =
                    task.query_status().await.map_err(|e| PyErr::from(RustClientError(e)))?;
                Ok(TaskStatus::from(status))
            })
        }

        #[gen_stub(override_return_type(type_repr="typing.Awaitable[bool]", imports=("typing")))]
        #[pyo3(signature = (sleep_time = 0.25, max_attempts = 80))]
        pub fn wait_till_complete<'a>(
            &self,
            sleep_time: f64,
            max_attempts: u32,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let bridge = self.bridge.as_ref().ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "Cannot await wait_till_complete() on a task returned from \
                     a `*_blocking` method. Use the `wait_till_complete_blocking()` \
                     sibling or rebuild via the async Client method.",
                )
            })?;
            let task = self._as.clone();
            crate::completion::batched_future_into_py(bridge, py, async move {
                wait_till_complete_impl(task, sleep_time, max_attempts).await
            })
        }

        /// Blocking sibling of :meth:`query_status` — returns the current
        /// status without spinning up an asyncio loop. Rejects calls made
        /// from inside a running asyncio loop.
        pub fn query_status_blocking(&self, py: Python<'_>) -> PyResult<TaskStatus> {
            let task = self._as.clone();
            run_blocking(py, async move {
                use aerospike_core::task::Task;
                let status: aerospike_core::task::Status = task
                    .query_status()
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;
                Ok(TaskStatus::from(status))
            })
        }

        /// Blocking sibling of :meth:`wait_till_complete` — polls status
        /// until COMPLETE or NOT_FOUND without needing an asyncio loop.
        /// Rejects calls made from inside a running asyncio loop.
        ///
        /// Args:
        ///     sleep_time: Time to sleep between status checks (seconds). Default: 0.25
        ///     max_attempts: Maximum number of attempts before giving up. Default: 80 (20 seconds)
        ///
        /// Returns:
        ///     True if task completed, False if max attempts reached
        #[pyo3(signature = (sleep_time = 0.25, max_attempts = 80))]
        pub fn wait_till_complete_blocking(
            &self,
            sleep_time: f64,
            max_attempts: u32,
            py: Python<'_>,
        ) -> PyResult<bool> {
            let task = self._as.clone();
            run_blocking(py, async move {
                wait_till_complete_impl(task, sleep_time, max_attempts).await
            })
        }
    }
