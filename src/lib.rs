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

#![deny(warnings)]
extern crate pyo3;

use std::collections::HashMap;
use std::sync::Arc;

use pyo3::exceptions::{PyException, PyValueError};
use pyo3::exceptions::PyTypeError;
use pyo3::types::PyDict;
use pyo3::{prelude::*, IntoPyObjectExt};

use pyo3_async_runtimes::tokio as pyo3_asyncio;
use pyo3_stub_gen::{
    define_stub_info_gatherer, derive::gen_stub_pyclass, derive::gen_stub_pyfunction,
    derive::gen_stub_pymethods,
};

use tokio::sync::Mutex;

use aerospike_core::errors::Error;


mod completion;
mod enums;
mod errors;
mod tasks;
mod tls;
mod record;
mod cdt;
mod expressions;
mod filter;
mod operations;
mod policies;
mod cluster;

pub use enums::*;
pub use errors::*;
pub use tasks::*;
pub use record::*;
pub use cdt::*;
pub use expressions::*;
pub use filter::*;
pub use operations::*;
pub use policies::*;
pub use cluster::*;
pub use tls::*;

define_stub_info_gatherer!(stub_info);

use crate::cdt::ctx_to_vec;
use crate::operations::{
    bins_flag, convert_ops_with_ctx_to_core, convert_scalar_ops_to_core, extract_py_ops,
    extract_py_ops_with_ctx,
};

    /**********************************************************************************
     *
     * Client
     *
     **********************************************************************************/
    #[gen_stub_pyfunction(module = "_aerospike_async_native")]
    #[pyfunction]
    #[gen_stub(override_return_type(type_repr="typing.Awaitable[Client]", imports=("typing")))]
    pub fn new_client(py: Python, policy: ClientPolicy, seeds: String) -> PyResult<Py<PyAny>> {
        let as_policy = policy._as.clone();
        let as_seeds = seeds.clone();

        Ok(pyo3_asyncio::future_into_py(py, async move {
            log::debug!(target: "aerospike_async", "connecting to {}", as_seeds);
            let c = aerospike_core::Client::new(&as_policy, &as_seeds)
                .await
                .map_err(|e| PyErr::from(RustClientError(e)))?;

            log::debug!(target: "aerospike_async", "connected to {}", seeds);
            let res = Client {
                _as: Arc::new(c),
                seeds: seeds.clone(),
            };

            Ok(res)
        })?
        .into())
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  Txn
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Multi-Record Transaction handle.
    ///
    /// Pass a `Txn` instance to record operations to group them into a single
    /// atomic transaction. Call :meth:`Client.commit` or :meth:`Client.abort`
    /// to finalize the transaction.
    ///
    /// Example::
    ///
    ///     txn = aerospike_async.Txn()
    ///     # use txn in put/get policy.txn field
    ///     status = await client.commit(txn)
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(module = "_aerospike_async_native")]
    #[derive(Clone)]
    pub struct Txn {
        pub(crate) _as: Arc<aerospike_core::Txn>,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl Txn {
        /// Create a new multi-record transaction.
        #[new]
        pub fn new() -> Self {
            Txn { _as: Arc::new(aerospike_core::Txn::new()) }
        }

        /// Unique transaction ID assigned by the client.
        #[getter]
        pub fn id(&self) -> i64 {
            self._as.id()
        }

        /// Current state of the transaction.
        #[getter]
        pub fn state(&self) -> TxnState {
            self._as.state().into()
        }

        /// Force the transaction into a given state.
        ///
        /// Primarily useful for testing error paths that depend on a
        /// non-``OPEN`` transaction (commands must be issued against an
        /// ``OPEN`` transaction). Production code should let
        /// :meth:`Client.commit` / :meth:`Client.abort` drive state
        /// transitions.
        #[setter]
        pub fn set_state(&self, state: TxnState) {
            let core_state = match state {
                TxnState::Open => aerospike_core::TxnState::Open,
                TxnState::Verified => aerospike_core::TxnState::Verified,
                TxnState::Committed => aerospike_core::TxnState::Committed,
                TxnState::Aborted => aerospike_core::TxnState::Aborted,
            };
            self._as.set_state(core_state);
        }

        /// Transaction timeout in seconds. Zero means use the server default.
        #[getter]
        pub fn timeout(&self) -> u32 {
            self._as.timeout().as_secs() as u32
        }

        /// Set the transaction timeout in seconds.
        ///
        /// Must be set before the transaction is shared with a policy or
        /// operation. After the underlying ``Arc<Txn>`` has been cloned into
        /// a policy / builder the timeout is frozen and this raises
        /// :class:`ValueError`. In a transactional session, set the timeout
        /// immediately after entering the ``async with`` block (before the
        /// first ``execute()``).
        #[setter]
        pub fn set_timeout(&mut self, timeout: u32) -> PyResult<()> {
            Arc::get_mut(&mut self._as)
                .ok_or_else(|| PyValueError::new_err(
                    "Cannot mutate Txn.timeout after the transaction has been \
                     shared with a policy or operation; set the timeout before \
                     the first operation in the transactional session."
                ))?
                .set_timeout(std::time::Duration::from_secs(timeout as u64));
            Ok(())
        }

        /// Namespace in use by this transaction, if one has been set.
        #[getter]
        pub fn namespace(&self) -> Option<String> {
            self._as.namespace()
        }

        fn __repr__(&self) -> String {
            format!("Txn(id={}, state={:?}, namespace={:?})", self._as.id(), self._as.state(), self._as.namespace())
        }
    }

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(subclass, freelist = 1)]
    #[derive(Clone)]
    pub struct Client {
        _as: Arc<aerospike_core::Client>,
        seeds: String,
    }

    // Helper function to check if a key exists (internal use, shared by exists() and exists_legacy())
    impl Client {
        async fn exists_internal(
            client: Arc<aerospike_core::Client>,
            policy: aerospike_core::ReadPolicy,
            key: aerospike_core::Key,
        ) -> Result<bool, Error> {
            client.exists(&policy, &key).await
        }
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl Client {
        #[new]
        pub fn new() -> PyResult<Self> {
            // This is a placeholder constructor - actual initialization should be done via new_client function
            Err(PyException::new_err("Use new_client() function to create a Client instance"))
        }

        pub fn seeds(&self) -> &str {
            &self.seeds
        }

        /// Closes the connection to the Aerospike cluster.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        pub fn close<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();

            pyo3_asyncio::future_into_py(py, async move {
                client
                    .close()
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;
                Ok(None::<bool>)
            })
        }

        /// Returns true if the client is connected to any cluster nodes.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[bool]", imports=("typing")))]
        pub fn is_connected<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();

            pyo3_asyncio::future_into_py(py, async move {
                Ok(client
                    .is_connected())
            })
        }

        /// Write record bin(s). The policy specifies the transaction timeout, record expiration and
        /// how the transaction is handled when the record already exists.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        pub fn put<'a>(
            &self,
            policy: &WritePolicy,
            key: &Key,
            bins: &Bound<'a, PyDict>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let policy = policy._as.clone();
            let key = key._as.clone();
            let client = self._as.clone();

            // Convert PyDict to Vec<Bin>, validating that all keys are strings
            let mut bin_vec = Vec::new();
            for (py_key, py_val) in bins.iter() {
                // Validate that the key is a string
                let name = py_key.extract::<String>().map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "A bin name must be a string or unicode string"
                    )
                })?;

                let val: PythonValue = py_val.extract()?;
                bin_vec.push(aerospike_core::Bin::new(name, val.into()));
            }

            completion::batched_future_into_py(py, async move {
                client
                    .put(&policy, &key, &bin_vec)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(None::<bool>)
            })
        }

        /// Read record for the specified key. Depending on the bins value provided, all record bins,
        /// only selected record bins or only the record headers will be returned. The policy can be
        /// used to specify timeouts.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        #[pyo3(signature = (policy, key, bins = None))]
        pub fn get<'a>(
            &self,
            policy: &ReadPolicy,
            key: &Key,
            bins: Option<Vec<String>>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let has_filter_expression = policy._as.base_policy.filter_expression.is_some();
            let policy = policy._as.clone();
            let key = key._as.clone();
            let client = self._as.clone();

            completion::batched_future_into_py(py, async move {
                let res = client
                    .get(&policy, &key, bins_flag(bins))
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                // Check if filter expression didn't match
                // When a filter expression doesn't match, Aerospike returns an empty record
                if res.bins.is_empty() && has_filter_expression {
                    return Err(PyException::new_err("Filter expression did not match any records"));
                }

                Ok(Record { _as: res, cached_bins: None })
            })
        }

        /// Execute multiple operations atomically on a single record.
        ///
        /// The policy specifies the transaction timeout, record expiration and how the transaction
        /// is handled when the record already exists.
        ///
        /// Args:
        ///     policy: The write policy for the operation.
        ///     key: The key of the record to operate on.
        ///     operations: A list of Operation objects to execute.
        ///
        /// Returns:
        ///     A Record containing the results of the operations.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[Record]", imports=("typing", "aerospike_async")))]
        pub fn operate<'a>(
            &self,
            policy: &WritePolicy,
            key: &Key,
            operations: Vec<Py<PyAny>>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let policy = policy._as.clone();
            let key = key._as.clone();
            let client = self._as.clone();

            let rust_ops = extract_py_ops_with_ctx(py, &operations)?;

            completion::batched_future_into_py(py, async move {
                let (core_ops, _) = convert_ops_with_ctx_to_core(&rust_ops, false)?;
                let res = client
                    .operate(&policy, &key, &core_ops)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(Record { _as: res, cached_bins: None })
            })
        }


        /// Add integer bin values to existing record bin values. The policy specifies the transaction
        /// timeout, record expiration and how the transaction is handled when the record already
        /// exists. This call only works for integer values.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        pub fn add<'a>(
            &self,
            policy: &WritePolicy,
            key: &Key,
            bins: HashMap<String, PythonValue>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let policy = policy._as.clone();
            let key = key._as.clone();
            let client = self._as.clone();

            let bins: Vec<aerospike_core::Bin> = bins
                .into_iter()
                .map(|(name, val)| aerospike_core::Bin::new(name, val.into()))
                .collect();

            completion::batched_future_into_py(py, async move {
                client
                    .add(&policy, &key, &bins)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(None::<bool>)
            })
        }

        /// Append bin string values to existing record bin values. The policy specifies the
        /// transaction timeout, record expiration and how the transaction is handled when the record
        /// already exists. This call only works for string values.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        pub fn append<'a>(
            &self,
            policy: &WritePolicy,
            key: &Key,
            bins: HashMap<String, PythonValue>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let policy = policy._as.clone();
            let key = key._as.clone();
            let client = self._as.clone();

            let bins: Vec<aerospike_core::Bin> = bins
                .into_iter()
                .map(|(name, val)| aerospike_core::Bin::new(name, val.into()))
                .collect();

            completion::batched_future_into_py(py, async move {
                client
                    .append(&policy, &key, &bins)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(None::<bool>)
            })
        }

        /// Prepend bin string values to existing record bin values. The policy specifies the
        /// transaction timeout, record expiration and how the transaction is handled when the record
        /// already exists. This call only works for string values.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        pub fn prepend<'a>(
            &self,
            policy: &WritePolicy,
            key: &Key,
            bins: HashMap<String, PythonValue>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let policy = policy._as.clone();
            let key = key._as.clone();
            let client = self._as.clone();

            let bins: Vec<aerospike_core::Bin> = bins
                .into_iter()
                .map(|(name, val)| aerospike_core::Bin::new(name, val.into()))
                .collect();

            completion::batched_future_into_py(py, async move {
                client
                    .prepend(&policy, &key, &bins)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(None::<bool>)
            })
        }

        /// Delete record for specified key. The policy specifies the transaction timeout.
        /// The call returns `true` if the record existed on the server before deletion.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        pub fn delete<'a>(
            &self,
            policy: &WritePolicy,
            key: &Key,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let policy = policy._as.clone();
            let key = key._as.clone();
            let client = self._as.clone();

            completion::batched_future_into_py(py, async move {
                let res = client
                    .delete(&policy, &key)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(res)
            })
        }

        /// Reset record's time to expiration using the policy's expiration. Fail if the record does
        /// not exist.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        pub fn touch<'a>(
            &self,
            policy: &WritePolicy,
            key: &Key,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let policy = policy._as.clone();
            let key = key._as.clone();
            let client = self._as.clone();

            completion::batched_future_into_py(py, async move {
                client
                    .touch(&policy, &key)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(None::<bool>)
            })
        }

        /// Read multiple records for specified keys in one batch call.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.List[BatchRecord]]", imports=("typing")))]
        #[pyo3(signature = (batch_policy, read_policy, keys, bins = None))]
        pub fn batch_read<'a>(
            &self,
            batch_policy: Option<&BatchPolicy>,
            read_policy: Option<&BatchReadPolicy>,
            keys: Vec<PyRef<Key>>,
            bins: Option<Vec<String>>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let batch_policy = batch_policy.map(|p| p._as.clone()).unwrap_or_default();
            let read_policy = read_policy.map(|p| p._as.clone()).unwrap_or_default();

            let rust_keys: Vec<aerospike_core::Key> =
                keys.iter().map(|k| k._as.clone()).collect();
            let client = self._as.clone();

            completion::batched_future_into_py(py, async move {
                use aerospike_core::BatchOperation;

                let bf = bins_flag(bins);
                let mut batch_ops = Vec::with_capacity(rust_keys.len());
                for key in rust_keys {
                    batch_ops.push(BatchOperation::read(&read_policy, key, bf.clone()));
                }

                let results = client
                    .batch(&batch_policy, &batch_ops)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(results
                    .into_iter()
                    .map(|br| BatchRecord { _as: br })
                    .collect::<Vec<BatchRecord>>())
            })
        }

        /// Write multiple records for specified keys in one batch call.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.List[BatchRecord]]", imports=("typing")))]
        pub fn batch_write<'a>(
            &self,
            batch_policy: Option<&BatchPolicy>,
            write_policy: Option<&BatchWritePolicy>,
            keys: Vec<PyRef<Key>>,
            bins_list: Vec<Py<PyAny>>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            if keys.len() != bins_list.len() {
                return Err(PyValueError::new_err("keys and bins_list must have the same length"));
            }

            let batch_policy = batch_policy.map(|p| p._as.clone()).unwrap_or_default();
            let write_policy = write_policy.map(|p| p._as.clone()).unwrap_or_default();
            let client = self._as.clone();

            let rust_keys: Vec<aerospike_core::Key> =
                keys.iter().map(|k| k._as.clone()).collect();

            let mut bins_vecs = Vec::with_capacity(bins_list.len());
            for bins_obj in &bins_list {
                let bins_dict = bins_obj.bind(py).cast::<pyo3::types::PyDict>()?;
                let mut bin_vec = Vec::new();
                for (py_key, py_val) in bins_dict.iter() {
                    let name = py_key.extract::<String>().map_err(|_| {
                        PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                            "A bin name must be a string or unicode string"
                        )
                    })?;
                    let val: PythonValue = py_val.extract()?;
                    bin_vec.push(aerospike_core::Bin::new(name, val.into()));
                }
                bins_vecs.push(bin_vec);
            }

            completion::batched_future_into_py(py, async move {
                use aerospike_core::BatchOperation;
                use aerospike_core::operations;

                let mut batch_ops = Vec::with_capacity(rust_keys.len());
                for (key, bins) in rust_keys.into_iter().zip(bins_vecs.iter()) {
                    let ops: Vec<aerospike_core::operations::Operation> = bins
                        .iter()
                        .map(|bin| operations::put(bin))
                        .collect();
                    batch_ops.push(BatchOperation::write(&write_policy, key, ops));
                }

                let results = client
                    .batch(&batch_policy, &batch_ops)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(results
                    .into_iter()
                    .map(|br| BatchRecord { _as: br })
                    .collect::<Vec<BatchRecord>>())
            })
        }

        /// Perform read/write operations on multiple keys in one batch call.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.List[BatchRecord]]", imports=("typing")))]
        pub fn batch_operate<'a>(
            &self,
            batch_policy: Option<&BatchPolicy>,
            write_policy: Option<&BatchWritePolicy>,
            keys: Vec<PyRef<Key>>,
            operations_list: Vec<Vec<Py<PyAny>>>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            if keys.len() != operations_list.len() {
                return Err(PyValueError::new_err("keys and operations_list must have the same length"));
            }

            let batch_policy = batch_policy.map(|p| p._as.clone()).unwrap_or_default();
            let write_policy = write_policy.map(|p| p._as.clone()).unwrap_or_default();
            let client = self._as.clone();

            let rust_keys: Vec<aerospike_core::Key> =
                keys.iter().map(|k| k._as.clone()).collect();

            let mut rust_ops_list = Vec::with_capacity(operations_list.len());
            for operations in operations_list {
                rust_ops_list.push(extract_py_ops_with_ctx(py, &operations)?);
            }

            completion::batched_future_into_py(py, async move {
                use aerospike_core::BatchOperation;

                let read_policy = aerospike_core::BatchReadPolicy::default();

                let mut batch_ops = Vec::with_capacity(rust_keys.len());
                for (key, ops) in rust_keys.into_iter().zip(rust_ops_list.into_iter()) {
                    let (core_ops, has_write_op) = convert_ops_with_ctx_to_core(&ops, true)?;
                    let batch_op = if has_write_op {
                        BatchOperation::write(&write_policy, key, core_ops)
                    } else {
                        BatchOperation::read_ops(&read_policy, key, core_ops)
                    };
                    batch_ops.push(batch_op);
                }

                let results = client
                    .batch(&batch_policy, &batch_ops)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(results
                    .into_iter()
                    .map(|br| BatchRecord { _as: br })
                    .collect::<Vec<BatchRecord>>())
            })
        }

        /// Delete multiple records for specified keys in one batch call.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.List[BatchRecord]]", imports=("typing")))]
        pub fn batch_delete<'a>(
            &self,
            batch_policy: Option<&BatchPolicy>,
            delete_policy: Option<&BatchDeletePolicy>,
            keys: Vec<PyRef<Key>>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let batch_policy = batch_policy.map(|p| p._as.clone()).unwrap_or_default();
            let delete_policy = delete_policy.map(|p| p._as.clone()).unwrap_or_default();
            let client = self._as.clone();

            let rust_keys: Vec<aerospike_core::Key> =
                keys.iter().map(|k| k._as.clone()).collect();

            completion::batched_future_into_py(py, async move {
                use aerospike_core::BatchOperation;

                let mut batch_ops = Vec::with_capacity(rust_keys.len());
                for key in rust_keys {
                    batch_ops.push(BatchOperation::delete(&delete_policy, key));
                }

                let results = client
                    .batch(&batch_policy, &batch_ops)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(results
                    .into_iter()
                    .map(|br| BatchRecord { _as: br })
                    .collect::<Vec<BatchRecord>>())
            })
        }

        /// Check if multiple record keys exist in one batch call.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.List[builtins.bool]]", imports=("typing", "builtins")))]
        pub fn batch_exists<'a>(
            &self,
            batch_policy: Option<&BatchPolicy>,
            read_policy: Option<&BatchReadPolicy>,
            keys: Vec<PyRef<Key>>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let batch_policy = batch_policy.map(|p| p._as.clone()).unwrap_or_default();
            let read_policy = read_policy.map(|p| p._as.clone()).unwrap_or_default();
            let client = self._as.clone();

            let rust_keys: Vec<aerospike_core::Key> =
                keys.iter().map(|k| k._as.clone()).collect();

            completion::batched_future_into_py(py, async move {
                use aerospike_core::BatchOperation;
                use aerospike_core::Bins;

                let mut batch_ops = Vec::with_capacity(rust_keys.len());
                for key in rust_keys {
                    batch_ops.push(BatchOperation::read(&read_policy, key, Bins::None));
                }

                let results = client
                    .batch(&batch_policy, &batch_ops)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(results
                    .into_iter()
                    .map(|br| br.record.is_some())
                    .collect::<Vec<bool>>())
            })
        }

        /// Read multiple record headers (metadata only, no bin data) for specified keys in one batch call.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.List[typing.Optional[Record]]]", imports=("typing")))]
        pub fn batch_get_header<'a>(
            &self,
            batch_policy: Option<&BatchPolicy>,
            read_policy: Option<&BatchReadPolicy>,
            keys: Vec<PyRef<Key>>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let batch_policy = batch_policy.map(|p| p._as.clone()).unwrap_or_default();
            let read_policy = read_policy.map(|p| p._as.clone()).unwrap_or_default();
            let client = self._as.clone();

            let rust_keys: Vec<aerospike_core::Key> =
                keys.iter().map(|k| k._as.clone()).collect();

            completion::batched_future_into_py(py, async move {
                use aerospike_core::BatchOperation;
                use aerospike_core::Bins;

                let mut batch_ops = Vec::with_capacity(rust_keys.len());
                for key in rust_keys {
                    batch_ops.push(BatchOperation::read(&read_policy, key, Bins::None));
                }

                let results = client
                    .batch(&batch_policy, &batch_ops)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(results
                    .into_iter()
                    .map(|br| br.record.map(|r| Record { _as: r, cached_bins: None }))
                    .collect::<Vec<Option<Record>>>())
            })
        }

        /// Apply UDF operations on multiple keys in one batch call.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.List[BatchRecord]]", imports=("typing")))]
        pub fn batch_apply<'a>(
            &self,
            batch_policy: Option<&BatchPolicy>,
            udf_policy: Option<&BatchUDFPolicy>,
            keys: Vec<PyRef<Key>>,
            udf_name: String,
            function_name: String,
            args: Option<Vec<PythonValue>>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let batch_policy = batch_policy.map(|p| p._as.clone()).unwrap_or_default();
            let udf_policy = udf_policy.map(|p| p._as.clone()).unwrap_or_default();
            let client = self._as.clone();

            let rust_keys: Vec<aerospike_core::Key> =
                keys.iter().map(|k| k._as.clone()).collect();

            let rust_args = args.map(|args| {
                args.into_iter()
                    .map(|v| v.into())
                    .collect::<Vec<aerospike_core::Value>>()
            });

            completion::batched_future_into_py(py, async move {
                use aerospike_core::BatchOperation;

                let mut batch_ops = Vec::with_capacity(rust_keys.len());
                for key in rust_keys {
                    let rust_args_owned = rust_args.as_ref().map(|a| a.to_vec());
                    batch_ops.push(BatchOperation::udf(&udf_policy, key, &udf_name, &function_name, rust_args_owned));
                }

                let results = client
                    .batch(&batch_policy, &batch_ops)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(results
                    .into_iter()
                    .map(|br| BatchRecord { _as: br })
                    .collect::<Vec<BatchRecord>>())
            })
        }

        /// Execute a mixed batch of read, write, and delete operations in a single server call.
        ///
        /// Each operation is specified via :class:`BatchReadOp`, :class:`BatchWriteOp`,
        /// or :class:`BatchDeleteOp`, each carrying its own key and per-record policy.
        ///
        /// Args:
        ///     batch_policy: Optional :class:`BatchPolicy` for the entire batch.
        ///     ops: List of :class:`BatchReadOp`, :class:`BatchWriteOp`, and/or
        ///          :class:`BatchDeleteOp` objects.
        ///
        /// Returns:
        ///     A list of :class:`BatchRecord` results in the same order as the input ops.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Sequence[BatchRecord]]", imports=("typing")))]
        pub fn batch<'a>(
            &self,
            batch_policy: Option<&BatchPolicy>,
            ops: Vec<Py<PyAny>>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let batch_policy = batch_policy.map(|p| p._as.clone()).unwrap_or_default();
            let client = self._as.clone();

            #[derive(Clone)]
            enum ExtractedOp {
                Read {
                    key: aerospike_core::Key,
                    policy: aerospike_core::BatchReadPolicy,
                    bins: Option<Vec<String>>,
                    ops: Vec<OpWithCtx>,
                },
                Write {
                    key: aerospike_core::Key,
                    policy: aerospike_core::BatchWritePolicy,
                    ops: Vec<OpWithCtx>,
                },
                Delete {
                    key: aerospike_core::Key,
                    policy: aerospike_core::BatchDeletePolicy,
                },
            }

            let mut extracted: Vec<ExtractedOp> = Vec::with_capacity(ops.len());
            for op_obj in &ops {
                if let Ok(read_op) = op_obj.extract::<PyRef<BatchReadOp>>(py) {
                    extracted.push(ExtractedOp::Read {
                        key: read_op.key.clone(),
                        policy: read_op.policy.clone(),
                        bins: read_op.bins.clone(),
                        ops: read_op.ops.clone(),
                    });
                } else if let Ok(write_op) = op_obj.extract::<PyRef<BatchWriteOp>>(py) {
                    extracted.push(ExtractedOp::Write {
                        key: write_op.key.clone(),
                        policy: write_op.policy.clone(),
                        ops: write_op.ops.clone(),
                    });
                } else if let Ok(delete_op) = op_obj.extract::<PyRef<BatchDeleteOp>>(py) {
                    extracted.push(ExtractedOp::Delete {
                        key: delete_op.key.clone(),
                        policy: delete_op.policy.clone(),
                    });
                } else {
                    return Err(PyTypeError::new_err(
                        "Each op must be a BatchReadOp, BatchWriteOp, or BatchDeleteOp"
                    ));
                }
            }

            completion::batched_future_into_py(py, async move {
                use aerospike_core::BatchOperation;

                let mut batch_ops = Vec::with_capacity(extracted.len());
                for ext in &extracted {
                    match ext {
                        ExtractedOp::Read { key, policy, bins, ops } if ops.is_empty() => {
                            batch_ops.push(
                                BatchOperation::read(policy, key.clone(), bins_flag(bins.clone()))
                            );
                        }
                        ExtractedOp::Read { key, policy, bins: _, ops } => {
                            let (core_ops, _has_write) = convert_ops_with_ctx_to_core(ops, false)?;
                            batch_ops.push(
                                BatchOperation::read_ops(policy, key.clone(), core_ops)
                            );
                        }
                        ExtractedOp::Write { key, policy, ops } => {
                            let (core_ops, _) = convert_ops_with_ctx_to_core(ops, false)?;
                            batch_ops.push(
                                BatchOperation::write(policy, key.clone(), core_ops)
                            );
                        }
                        ExtractedOp::Delete { key, policy } => {
                            batch_ops.push(
                                BatchOperation::delete(policy, key.clone())
                            );
                        }
                    }
                }

                let results = client
                    .batch(&batch_policy, &batch_ops)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(results
                    .into_iter()
                    .map(|br| BatchRecord { _as: br })
                    .collect::<Vec<BatchRecord>>())
            })
        }

        /// Execute a UDF (User Defined Function) on a single record.
        ///
        /// Args:
        ///     policy: WritePolicy for the operation.
        ///     key: The key of the record to execute the UDF on.
        ///     server_path: Server path to the UDF module (e.g., "example.lua").
        ///     function_name: Name of the function to execute within the UDF module.
        ///     args: Optional list of arguments to pass to the UDF function.
        ///
        /// Returns:
        ///     Optional Value containing the UDF result, or None if the UDF returns no value.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Optional[typing.Any]]", imports=("typing")))]
        pub fn execute_udf<'a>(
            &self,
            policy: &WritePolicy,
            key: &Key,
            server_path: String,
            function_name: String,
            args: Option<Vec<PythonValue>>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let policy = policy._as.clone();
            let key = key._as.clone();
            let client = self._as.clone();

            // Convert args before moving into async block
            let rust_args = args.map(|args| {
                args.into_iter()
                    .map(|v| v.into())
                    .collect::<Vec<aerospike_core::Value>>()
            });

            pyo3_asyncio::future_into_py(py, async move {
                let rust_args_ref = rust_args.as_ref().map(|a| a.as_slice());
                let result = client
                    .execute_udf(&policy, &key, &server_path, &function_name, rust_args_ref)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|_py| {
                    match result {
                        Some(value) => {
                            let pv: PythonValue = value.into();
                            Ok(Some(pv.into_pyobject(_py)?.unbind()))
                        }
                        None => Ok(None),
                    }
                })
            })
        }

        /// Execute a query/scan and apply write operations to matching records (background job).
        /// Returns an ExecuteTask to poll for completion. Supports scalar and expression write
        /// operations (put, add, delete, touch, append, prepend, ExpOperation.write).
        /// List/map/bit/HLL operations are not supported for background query.
        ///
        /// Args:
        ///     write_policy: WritePolicy for the background operation.
        ///     statement: Statement (namespace, set, optional filters).
        ///     operations: List of Operation objects (e.g. Operation.put, Operation.add, Operation.delete, Operation.touch).
        ///
        /// Returns:
        ///     ExecuteTask to monitor completion (query_status, wait_till_complete).
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[ExecuteTask]", imports=("typing")))]
        pub fn query_operate<'a>(
            &self,
            write_policy: &WritePolicy,
            statement: &Statement,
            operations: Vec<Py<PyAny>>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let policy = write_policy._as.clone();
            let client = self._as.clone();
            let core_statement = statement._as.clone();

            let rust_ops = extract_py_ops(py, &operations)?;
            let (core_ops, _) = convert_scalar_ops_to_core(&rust_ops).map_err(|e| {
                PyValueError::new_err(format!(
                    "query_operate supports scalar and expression operations (put, add, delete, touch, append, prepend, ExpOperation.write). List/map/bit/HLL operations are not supported for background query. {}",
                    e.to_string()
                ))
            })?;

            pyo3_asyncio::future_into_py(py, async move {
                let task = client
                    .query_operate(&policy, core_statement, &core_ops)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;
                Ok(ExecuteTask { _as: task })
            })
        }

        /// Apply a UDF to records matching the statement filter (background job).
        /// Returns an ExecuteTask to poll for completion. Records are not returned.
        /// If the statement has no filter, the UDF is applied to all records in the namespace/set.
        ///
        /// Args:
        ///     write_policy: WritePolicy for the background operation.
        ///     statement: Statement (namespace, set, optional filters).
        ///     package_name: Server-side UDF package name.
        ///     function_name: UDF function to invoke.
        ///     args: Optional arguments to pass to the UDF.
        ///
        /// Returns:
        ///     ExecuteTask to monitor completion (query_status, wait_till_complete).
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[ExecuteTask]", imports=("typing")))]
        pub fn query_execute_udf<'a>(
            &self,
            write_policy: &WritePolicy,
            statement: &Statement,
            package_name: String,
            function_name: String,
            args: Option<Vec<PythonValue>>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let policy = write_policy._as.clone();
            let client = self._as.clone();
            let mut core_statement = statement._as.clone();
            let rust_args = args.map(|a| a.into_iter().map(|v| v.into()).collect::<Vec<aerospike_core::Value>>());
            core_statement.set_aggregate_function(&package_name, &function_name, rust_args.as_deref());

            pyo3_asyncio::future_into_py(py, async move {
                let args_ref = rust_args.as_deref();
                let task = client
                    .query_execute_udf(&policy, core_statement, &package_name, &function_name, args_ref)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;
                Ok(ExecuteTask { _as: task })
            })
        }

        /// Register a UDF (User Defined Function) module on the server from bytes.
        ///
        /// Args:
        ///     policy: AdminPolicy for the operation.
        ///     udf_body: The UDF module content as bytes.
        ///     server_path: Server path where the UDF will be stored (e.g., "example.lua").
        ///     language: UDF language (UDFLang.LUA).
        ///
        /// Returns:
        ///     RegisterTask that can be used to wait for registration completion.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[RegisterTask]", imports=("typing")))]
        pub fn register_udf<'a>(
            &self,
            policy: Option<AdminPolicy>,
            udf_body: Vec<u8>,
            server_path: String,
            language: UDFLang,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();
            let admin_policy = policy.map(|p| p._as).unwrap_or_else(|| aerospike_core::AdminPolicy::default());
            let lang: aerospike_core::UDFLang = language.into();

            pyo3_asyncio::future_into_py(py, async move {
                let task = client
                    .register_udf(&admin_policy, &udf_body, &server_path, lang)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|_py| {
                    Ok(RegisterTask { _as: task })
                })
            })
        }

        /// Register a UDF (User Defined Function) module on the server from a file.
        ///
        /// Args:
        ///     policy: AdminPolicy for the operation.
        ///     client_path: Local file path to the UDF module.
        ///     server_path: Server path where the UDF will be stored (e.g., "example.lua").
        ///     language: UDF language (UDFLang.LUA).
        ///
        /// Returns:
        ///     RegisterTask that can be used to wait for registration completion.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[RegisterTask]", imports=("typing")))]
        pub fn register_udf_from_file<'a>(
            &self,
            policy: Option<AdminPolicy>,
            client_path: String,
            server_path: String,
            language: UDFLang,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();
            let admin_policy = policy.map(|p| p._as).unwrap_or_else(|| aerospike_core::AdminPolicy::default());
            let lang: aerospike_core::UDFLang = language.into();

            pyo3_asyncio::future_into_py(py, async move {
                let task = client
                    .register_udf_from_file(&admin_policy, &client_path, &server_path, lang)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|_py| {
                    Ok(RegisterTask { _as: task })
                })
            })
        }

        /// Remove a UDF (User Defined Function) module from the server.
        ///
        /// Args:
        ///     policy: AdminPolicy for the operation.
        ///     server_path: Server path to the UDF module to remove (e.g., "example.lua").
        ///
        /// Returns:
        ///     UdfRemoveTask that can be used to wait for removal completion.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[UdfRemoveTask]", imports=("typing")))]
        pub fn remove_udf<'a>(
            &self,
            policy: Option<AdminPolicy>,
            server_path: String,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();
            let admin_policy = policy.map(|p| p._as).unwrap_or_else(|| aerospike_core::AdminPolicy::default());

            pyo3_asyncio::future_into_py(py, async move {
                let task = client
                    .remove_udf(&admin_policy, &server_path)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|_py| {
                    Ok(UdfRemoveTask { _as: task })
                })
            })
        }

        /// Determine if a record key exists. The policy can be used to specify timeouts.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        pub fn exists<'a>(
            &self,
            policy: &ReadPolicy,
            key: &Key,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let policy = policy._as.clone();
            let key = key._as.clone();
            let client = self._as.clone();

            completion::batched_future_into_py(py, async move {
                let res = Self::exists_internal(client, policy, key)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(res)
            })
        }

        /// Determine if a record key exists (legacy contract). Returns (key, meta) where meta=None if record not found.
        /// This matches the legacy Python client contract.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Tuple[Key, typing.Optional[typing.Any]]]", imports=("typing")))]
        pub fn exists_legacy<'a>(
            &self,
            policy: &ReadPolicy,
            key: &Key,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let policy = policy._as.clone();
            let key = key._as.clone();
            let client = self._as.clone();

            pyo3_asyncio::future_into_py(py, async move {
                // Reuse the same logic as exists() but return (key, meta) tuple
                let exists = Self::exists_internal(client.clone(), policy.clone(), key.clone())
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                // Return (key, meta) tuple where meta=None if record doesn't exist
                // If record exists, get metadata (generation, ttl)
                let meta_record = if exists {
                    // Get metadata by calling get() with empty bins
                    let read_policy = aerospike_core::ReadPolicy::default();
                    Some(client
                        .get(&read_policy, &key, aerospike_core::Bins::None)
                        .await
                        .map_err(|e| PyErr::from(RustClientError(e)))?)
                } else {
                    None
                };

                // This matches the legacy Python client contract
                Python::attach(|py| {
                    let key_obj = Py::new(py, Key { _as: key })?;
                    let meta = if let Some(record) = meta_record {
                        // Create a dict with generation and ttl metadata
                        let meta_dict = pyo3::types::PyDict::new(py);
                        meta_dict.set_item("gen", record.generation)?;
                        if let Some(ttl) = record.time_to_live() {
                            meta_dict.set_item("ttl", ttl.as_secs() as u32)?;
                        } else {
                            meta_dict.set_item("ttl", py.None())?;
                        }
                        meta_dict.into()
                    } else {
                        py.None()
                    };
                    let tuple = pyo3::types::PyTuple::new(py, [key_obj.into(), meta])?;
                    Ok(tuple.unbind())
                })
            })
        }

        /// Removes all records in the specified namespace/set efficiently.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        #[pyo3(signature = (namespace, set_name, before_nanos = None, *, policy = None))]
        pub fn truncate<'a>(
            &self,
            namespace: String,
            set_name: String,
            before_nanos: Option<i64>,
            policy: Option<AdminPolicy>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();
            let admin_policy = policy.map(|p| p._as).unwrap_or_else(|| aerospike_core::AdminPolicy::default());

            let before_nanos = before_nanos.unwrap_or_default();

            pyo3_asyncio::future_into_py(py, async move {
                client
                    .truncate(&admin_policy, &namespace, &set_name, before_nanos)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(None::<bool>)
            })
        }

        /// Create a secondary index on a bin containing scalar values. This asynchronous server call
        /// returns before the command is complete.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        #[pyo3(signature = (namespace, set_name, bin_name, index_name, index_type, cit = None, ctx = None, *, policy = None))]
        pub fn create_index<'a>(
            &self,
            namespace: String,
            set_name: String,
            bin_name: String,
            index_name: String,
            index_type: IndexType,
            cit: Option<CollectionIndexType>,
            ctx: Option<Vec<CTX>>,
            policy: Option<AdminPolicy>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();
            let admin_policy = policy.map(|p| p._as).unwrap_or_else(|| aerospike_core::AdminPolicy::default());

            let cit = (&cit.unwrap_or(CollectionIndexType::Default)).into();
            let index_type = (&index_type).into();
            let ctx_core = ctx.map(|c| ctx_to_vec(&c));

            pyo3_asyncio::future_into_py(py, async move {
                client
                    .create_index_on_bin(
                        &admin_policy,
                        &namespace,
                        &set_name,
                        &bin_name,
                        &index_name,
                        index_type,
                        cit,
                        ctx_core.as_deref(),
                    )
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(None::<bool>)
            })
        }

        /// Drop a secondary index. Returns a DropIndexTask to track completion.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[DropIndexTask]", imports=("typing")))]
        #[pyo3(signature = (namespace, set_name, index_name, *, policy = None))]
        pub fn drop_index<'a>(
            &self,
            namespace: String,
            set_name: String,
            index_name: String,
            policy: Option<AdminPolicy>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();
            let admin_policy = policy.map(|p| p._as).unwrap_or_else(|| aerospike_core::AdminPolicy::default());

            pyo3_asyncio::future_into_py(py, async move {
                let task = client
                    .drop_index(&admin_policy, &namespace, &set_name, &index_name)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;
                Ok(DropIndexTask { _as: task })
            })
        }

        /// Create a secondary index using an expression. Returns an IndexTask to wait for completion.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[IndexTask]", imports=("typing")))]
        #[pyo3(signature = (namespace, set_name, index_name, index_type, expression, cit = None, *, policy = None))]
        pub fn create_index_using_expression<'a>(
            &self,
            namespace: String,
            set_name: String,
            index_name: String,
            index_type: IndexType,
            expression: &FilterExpression,
            cit: Option<CollectionIndexType>,
            policy: Option<AdminPolicy>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();
            let admin_policy =
                policy.map(|p| p._as).unwrap_or_else(|| aerospike_core::AdminPolicy::default());
            let expr = expression._as.clone();
            let cit = (&cit.unwrap_or(CollectionIndexType::Default)).into();
            let index_type = (&index_type).into();

            pyo3_asyncio::future_into_py(py, async move {
                let task = client
                    .create_index_using_expression(
                        &admin_policy,
                        &namespace,
                        &set_name,
                        &index_name,
                        index_type,
                        cit,
                        &expr,
                    )
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;
                Ok(IndexTask { _as: task })
            })
        }

        /// Execute a query on all server nodes and return a record iterator. The query executor puts
        /// records on a queue in separate threads. The calling thread concurrently pops records off
        /// the queue through the record iterator.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        pub fn query<'a>(
            &self,
            policy: &QueryPolicy,
            partition_filter: PartitionFilter,
            statement: &Statement,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let policy = policy._as.clone();
            let client = self._as.clone();
            let stmt = statement.clone()._as;

            pyo3_asyncio::future_into_py(py, async move {
                let res = client
                    .query(&policy, partition_filter._as, stmt)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(Recordset {
                    _as: res,
                    _stream: Arc::new(Mutex::new(None)),
                })
            })
        }

        /// Creates a new user with password and roles. Clear-text password will be hashed using bcrypt
        /// before sending to server.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        #[pyo3(signature = (user, password, roles, *, policy = None))]
        pub fn create_user<'a>(
            &self,
            user: String,
            password: String,
            roles: Vec<String>,
            policy: Option<AdminPolicy>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();
            let admin_policy = policy.map(|p| p._as).unwrap_or_else(|| aerospike_core::AdminPolicy::default());

            pyo3_asyncio::future_into_py(py, async move {
                let roles: Vec<&str> = roles.iter().map(|r| &**r).collect();
                client
                    .create_user(&admin_policy, &user, &password, &roles)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(None::<bool>)
            })
        }

        /// Creates a PKI user with roles. PKI users authenticate via TLS client certificate (no password).
        /// Supported by Aerospike Server v8.1+ Enterprise.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        #[pyo3(signature = (user, roles, *, policy = None))]
        pub fn create_pki_user<'a>(
            &self,
            user: String,
            roles: Vec<String>,
            policy: Option<AdminPolicy>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();
            let admin_policy = policy.map(|p| p._as).unwrap_or_else(|| aerospike_core::AdminPolicy::default());

            pyo3_asyncio::future_into_py(py, async move {
                let roles: Vec<&str> = roles.iter().map(|r| &**r).collect();
                client
                    .create_pki_user(&admin_policy, &user, &roles)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(None::<bool>)
            })
        }

        /// Removes a user from the cluster.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        #[pyo3(signature = (user, *, policy = None))]
        pub fn drop_user<'a>(&self, user: String, policy: Option<AdminPolicy>, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();
            let admin_policy = policy.map(|p| p._as).unwrap_or_else(|| aerospike_core::AdminPolicy::default());

            pyo3_asyncio::future_into_py(py, async move {
                client
                    .drop_user(&admin_policy, &user)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(None::<bool>)
            })
        }

        /// Changes a user's password. Clear-text password will be hashed using bcrypt before sending to server.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        #[pyo3(signature = (user, password, *, policy = None))]
        pub fn change_password<'a>(
            &self,
            user: String,
            password: String,
            policy: Option<AdminPolicy>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();
            let admin_policy = policy.map(|p| p._as).unwrap_or_else(|| aerospike_core::AdminPolicy::default());

            pyo3_asyncio::future_into_py(py, async move {
                client
                    .change_password(&admin_policy, &user, &password)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(None::<bool>)
            })
        }

        /// Adds roles to user's list of roles.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        #[pyo3(signature = (user, roles, *, policy = None))]
        pub fn grant_roles<'a>(
            &self,
            user: String,
            roles: Vec<String>,
            policy: Option<AdminPolicy>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();
            let admin_policy = policy.map(|p| p._as).unwrap_or_else(|| aerospike_core::AdminPolicy::default());

            pyo3_asyncio::future_into_py(py, async move {
                let roles: Vec<&str> = roles.iter().map(|r| &**r).collect();
                client
                    .grant_roles(&admin_policy, &user, &roles)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(None::<bool>)
            })
        }

        /// Removes roles from user's list of roles.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        #[pyo3(signature = (user, roles, *, policy = None))]
        pub fn revoke_roles<'a>(
            &self,
            user: String,
            roles: Vec<String>,
            policy: Option<AdminPolicy>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();
            let admin_policy = policy.map(|p| p._as).unwrap_or_else(|| aerospike_core::AdminPolicy::default());

            pyo3_asyncio::future_into_py(py, async move {
                let roles: Vec<&str> = roles.iter().map(|r| &**r).collect();
                client
                    .revoke_roles(&admin_policy, &user, &roles)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(None::<bool>)
            })
        }

        /// Retrieves users and their roles.
        /// If None is passed for the user argument, all users will be returned.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        #[pyo3(signature = (user = None, *, policy = None))]
        pub fn query_users<'a>(
            &self,
            user: Option<String>,
            policy: Option<AdminPolicy>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();
            let admin_policy = policy.map(|p| p._as).unwrap_or_else(|| aerospike_core::AdminPolicy::default());

            pyo3_asyncio::future_into_py(py, async move {
                let user = user.as_deref();
                let res = client
                    .query_users(&admin_policy, user)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                let res: Vec<User> = res.iter().map(|u| User { _as: u.clone() }).collect();
                Ok(res)
            })
        }

        /// Retrieves roles and their privileges.
        /// If None is passed for the role argument, all roles will be returned.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        #[pyo3(signature = (role = None, *, policy = None))]
        pub fn query_roles<'a>(
            &self,
            role: Option<String>,
            policy: Option<AdminPolicy>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();
            let admin_policy = policy.map(|p| p._as).unwrap_or_else(|| aerospike_core::AdminPolicy::default());

            pyo3_asyncio::future_into_py(py, async move {
                let role: Option<&str> = role.as_deref();
                let res = client
                    .query_roles(&admin_policy, role)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                let res: Vec<Role> = res.iter().map(|r| Role { _as: r.clone() }).collect();
                Ok(res)
            })
        }

        /// Creates a user-defined role.
        /// Quotas require server security configuration "enable-quotas" to be set to true.
        /// Pass 0 for quota values for no limit.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        #[pyo3(signature = (role_name, privileges, allowlist, read_quota, write_quota, *, policy = None))]
        pub fn create_role<'a>(
            &self,
            role_name: String,
            privileges: Vec<Privilege>,
            allowlist: Vec<String>,
            read_quota: u32,
            write_quota: u32,
            policy: Option<AdminPolicy>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();
            let admin_policy = policy.map(|p| p._as).unwrap_or_else(|| aerospike_core::AdminPolicy::default());

            pyo3_asyncio::future_into_py(py, async move {
                let allowlist: Vec<&str> = allowlist.iter().map(|al| &**al).collect();
                let privileges: Vec<aerospike_core::Privilege> =
                    privileges.iter().map(|r| r._as.clone()).collect();
                client
                    .create_role(&admin_policy, &role_name, &privileges, &allowlist, read_quota, write_quota)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(None::<bool>)
            })
        }

        /// Removes a user-defined role.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        #[pyo3(signature = (role_name, *, policy = None))]
        pub fn drop_role<'a>(
            &self,
            role_name: String,
            policy: Option<AdminPolicy>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();
            let admin_policy = policy.map(|p| p._as).unwrap_or_else(|| aerospike_core::AdminPolicy::default());

            pyo3_asyncio::future_into_py(py, async move {
                client
                    .drop_role(&admin_policy, &role_name)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(None::<bool>)
            })
        }

        /// Grants privileges to a user-defined role.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        #[pyo3(signature = (role_name, privileges, *, policy = None))]
        pub fn grant_privileges<'a>(
            &self,
            role_name: String,
            privileges: Vec<Privilege>,
            policy: Option<AdminPolicy>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();
            let admin_policy = policy.map(|p| p._as).unwrap_or_else(|| aerospike_core::AdminPolicy::default());

            pyo3_asyncio::future_into_py(py, async move {
                let privileges: Vec<aerospike_core::Privilege> =
                    privileges.iter().map(|p| p._as.clone()).collect();
                client
                    .grant_privileges(&admin_policy, &role_name, &privileges)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(None::<bool>)
            })
        }

        /// Revokes privileges from a user-defined role.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        #[pyo3(signature = (role_name, privileges, *, policy = None))]
        pub fn revoke_privileges<'a>(
            &self,
            role_name: String,
            privileges: Vec<Privilege>,
            policy: Option<AdminPolicy>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();
            let admin_policy = policy.map(|p| p._as).unwrap_or_else(|| aerospike_core::AdminPolicy::default());

            pyo3_asyncio::future_into_py(py, async move {
                let privileges: Vec<aerospike_core::Privilege> =
                    privileges.iter().map(|p| p._as.clone()).collect();
                client
                    .revoke_privileges(&admin_policy, &role_name, &privileges)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(None::<bool>)
            })
        }

        /// Sets IP address allowlist for a role.
        /// If allowlist is nil or empty, it removes existing allowlist from role.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        #[pyo3(signature = (role_name, allowlist, *, policy = None))]
        pub fn set_allowlist<'a>(
            &self,
            role_name: String,
            allowlist: Vec<String>,
            policy: Option<AdminPolicy>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();
            let admin_policy = policy.map(|p| p._as).unwrap_or_else(|| aerospike_core::AdminPolicy::default());

            pyo3_asyncio::future_into_py(py, async move {
                let allowlist: Vec<&str> = allowlist.iter().map(|al| &**al).collect();
                client
                    .set_allowlist(&admin_policy, &role_name, &allowlist)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(None::<bool>)
            })
        }

        /// Sets maximum reads/writes per second limits for a role.
        /// If a quota is zero, the limit is removed.
        /// Quotas require server security configuration "enable-quotas" to be set to true.
        /// Pass 0 for quota values for no limit.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        #[pyo3(signature = (role_name, read_quota, write_quota, *, policy = None))]
        pub fn set_quotas<'a>(
            &self,
            role_name: String,
            read_quota: u32,
            write_quota: u32,
            policy: Option<AdminPolicy>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();
            let admin_policy = policy.map(|p| p._as).unwrap_or_else(|| aerospike_core::AdminPolicy::default());

            pyo3_asyncio::future_into_py(py, async move {
                client
                    .set_quotas(&admin_policy, &role_name, read_quota, write_quota)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(None::<bool>)
            })
        }

        fn __str__(&self) -> PyResult<String> {
            Ok(self.seeds.to_string())
        }

        fn __repr__(&self) -> PyResult<String> {
            let s = self.__str__()?;
            Ok(format!("Client('{}')", s))
        }

        pub fn __copy__(&self) -> Self {
            self.clone()
        }

        pub fn __deepcopy__(&self, _memo: &Bound<PyDict>) -> Self {
            // fast bitwise copy instead of python's pickling process
            self.clone()
        }

        /// Returns a list of the names of the active server nodes in the cluster.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.List[str]]", imports=("typing")))]
        pub fn node_names<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();

            pyo3_asyncio::future_into_py(py, async move {
                let node_names = client
                    .node_names();

                Ok(node_names)
            })
        }

        /// Return node given its name.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[Node]", imports=("typing")))]
        pub fn get_node<'a>(&self, name: String, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();

            pyo3_asyncio::future_into_py(py, async move {
                let node = client
                    .get_node(&name)
                    .map_err(|e| PyErr::from(RustClientError(e)))?;
                Ok(Node { _as: node })
            })
        }

        /// Returns a list of all active server nodes in the cluster.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.List[Node]]", imports=("typing")))]
        pub fn nodes<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();

            pyo3_asyncio::future_into_py(py, async move {
                let nodes = client
                    .nodes();

                let py_nodes: Vec<Node> = nodes.into_iter().map(|n| Node { _as: n }).collect();
                Ok(py_nodes)
            })
        }

        /// Commit a multi-record transaction.
        ///
        /// Verifies all transaction record versions, then applies all writes
        /// atomically. Returns a :class:`CommitStatus` indicating the outcome.
        /// Raises :exc:`aerospike_async.exceptions.CommitFailedError` if the
        /// commit fails part-way through.
        ///
        /// Args:
        ///     txn: The transaction to commit.
        ///
        /// Returns:
        ///     CommitStatus: The outcome of the commit.
        ///
        /// Raises:
        ///     CommitFailedError: If the transaction could not be committed.
        ///
        /// Example::
        ///
        ///     status = await client.commit(txn)
        ///     assert status == CommitStatus.OK
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[CommitStatus]", imports=("typing")))]
        pub fn commit<'a>(&self, txn: &Txn, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();
            let txn_arc = txn._as.clone();
            pyo3_asyncio::future_into_py(py, async move {
                let status = client
                    .commit(&txn_arc)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;
                Ok(CommitStatus::from(status))
            })
        }

        /// Abort a multi-record transaction, rolling back all writes.
        ///
        /// Args:
        ///     txn: The transaction to abort.
        ///
        /// Returns:
        ///     AbortStatus: The outcome of the abort.
        ///
        /// Raises:
        ///     AerospikeError: If the abort itself encounters a server error.
        ///
        /// Example::
        ///
        ///     status = await client.abort(txn)
        ///     assert status == AbortStatus.OK
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[AbortStatus]", imports=("typing")))]
        pub fn abort<'a>(&self, txn: &Txn, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();
            let txn_arc = txn._as.clone();
            pyo3_asyncio::future_into_py(py, async move {
                let status = client
                    .abort(&txn_arc)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;
                Ok(AbortStatus::from(status))
            })
        }

        /// Execute an info command on a random cluster node.
        ///
        /// Args:
        ///     command: The info command to execute (e.g., "namespaces", "statistics", "build").
        ///
        /// Returns:
        ///     A dictionary containing the info command response as key-value pairs.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Dict[str, str]]", imports=("typing")))]
        pub fn info<'a>(&self, command: String, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();

            pyo3_asyncio::future_into_py(py, async move {
                let node = client
                    .cluster
                    .get_random_node()
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                let policy = aerospike_core::AdminPolicy::default();
                let response = node
                    .info(&policy, &[&command])
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(response)
            })
        }

        /// Execute an info command on all cluster nodes.
        ///
        /// Args:
        ///     command: The info command to execute (e.g., "namespaces", "statistics", "build").
        ///
        /// Returns:
        ///     A dictionary mapping node names to their info command responses.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Dict[str, typing.Dict[str, str]]]", imports=("typing")))]
        pub fn info_on_all_nodes<'a>(&self, command: String, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();

            pyo3_asyncio::future_into_py(py, async move {
                let nodes = client
                    .nodes();

                let mut results: HashMap<String, HashMap<String, String>> = HashMap::new();

                for node in nodes {
                    let node_name = node.name().to_string();
                    let policy = aerospike_core::AdminPolicy::default();
                    match node.info(&policy, &[&command]).await {
                        Ok(response) => {
                            results.insert(node_name, response);
                        }
                        Err(e) => {
                            // Log error but continue with other nodes
                            // We could also collect errors, but for now just skip failed nodes
                            eprintln!("Failed to get info from node {}: {}", node_name, e);
                        }
                    }
                }

                Ok(results)
            })
        }

        /// Sets XDR filter for given datacenter and namespace. Pass None as filter to remove.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        #[pyo3(signature = (datacenter, namespace, filter_expression = None, *, policy = None))]
        pub fn set_xdr_filter<'a>(
            &self,
            datacenter: String,
            namespace: String,
            filter_expression: Option<FilterExpression>,
            policy: Option<AdminPolicy>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();
            let admin_policy =
                policy.map(|p| p._as).unwrap_or_else(|| aerospike_core::AdminPolicy::default());
            let expr = filter_expression.clone();

            pyo3_asyncio::future_into_py(py, async move {
                client
                    .set_xdr_filter(
                        &admin_policy,
                        &datacenter,
                        &namespace,
                        expr.as_ref().map(|e| &e._as),
                    )
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;
                Ok(None::<bool>)
            })
        }
    }

/// Return a null value for use in Aerospike operations.
/// This is equivalent to Python None but represents an Aerospike null value.
/// Matches the legacy client's aerospike.null() function.
#[pyfunction]
#[gen_stub_pyfunction(module = "_aerospike_async_native")]
pub fn null(py: Python) -> Bound<PyAny> {
    py.None().into_bound(py)
}

/// Convert a GeoJSON string or coordinate pair to a GeoJSON object.
/// This matches the legacy client's aerospike.geojson() function.
///
/// Accepts:
/// - GeoJSON JSON string: '{"type": "Point", "coordinates": [-122.0, 37.0]}'
/// - Coordinate pair string: "-122.0, 37.5" (longitude, latitude)
#[pyfunction]
#[gen_stub_pyfunction(module = "_aerospike_async_native")]
pub fn geojson<'a>(py: Python<'a>, geo_str: &str) -> PyResult<GeoJSON> {
    // First, try to parse as GeoJSON JSON string
    // Check if it looks like JSON (starts with '{' and contains "type")
    if geo_str.trim_start().starts_with('{') && geo_str.contains("\"type\"") {
        // Try to parse as JSON and create GeoJSON from it
        let json_module = PyModule::import(py, "json")?;
        let json_loads = json_module.getattr("loads")?;
        let geo_dict = json_loads.call1((geo_str,))?;

        // Use GeoJSON constructor which accepts dict
        return GeoJSON::new(py, geo_dict.into_bound_py_any(py)?.as_any());
    }

    // Otherwise, try to parse as coordinate pair string like "122.0, 37.5"
    let parts: Vec<&str> = geo_str.split(',').map(|s| s.trim()).collect();
    if parts.len() != 2 {
        return Err(PyValueError::new_err(
            format!("Invalid input: '{}'. Expected GeoJSON JSON string or coordinate pair 'longitude, latitude'", geo_str)
        ));
    }

    let lng: f64 = parts[0].parse()
        .map_err(|_| PyValueError::new_err(format!("Invalid longitude: '{}'", parts[0])))?;
    let lat: f64 = parts[1].parse()
        .map_err(|_| PyValueError::new_err(format!("Invalid latitude: '{}'", parts[1])))?;

    // Create GeoJSON Point structure
    let point_dict = PyDict::new(py);
    point_dict.set_item("type", "Point")?;
    // Create coordinates list [lng, lat]
    let coords_vec = vec![lng, lat];
    point_dict.set_item("coordinates", coords_vec)?;

    // Use GeoJSON constructor to create from dict
    GeoJSON::new(py, point_dict.as_any())
}

/// Logger that forwards to Python's `logging` via pyo3-log when the interpreter
/// is available, falling back to stderr for messages emitted on background tokio
/// threads after Python shutdown.
struct ResilientPyLogger {
    inner: pyo3_log::Logger,
}

impl log::Log for ResilientPyLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        self.inner.enabled(metadata)
    }

    fn log(&self, record: &log::Record) {
        if unsafe { pyo3::ffi::Py_IsInitialized() } != 0 {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.inner.log(record);
            }));
        }
        // Silently drop messages when Python is unavailable (shutdown).
    }

    fn flush(&self) {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.inner.flush();
        }));
    }
}

#[pymodule]
fn _aerospike_async_native(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Bridge Rust `log` records to Python's `logging` module.
    // Rust module paths become Python logger names, e.g.
    //   aerospike_core::cluster -> logging.getLogger("aerospike_core.cluster")
    // Uses ResilientPyLogger to avoid panics on background threads during shutdown.
    let inner = pyo3_log::Logger::new(py, pyo3_log::Caching::LoggersAndLevels)?;
    let logger = ResilientPyLogger { inner };
    log::set_max_level(log::LevelFilter::Debug);
    let _ = log::set_logger(Box::leak(Box::new(logger)));
    log::debug!(target: "aerospike_async", "pyo3-log bridge active");

    // Add all main classes to the top level for easy importing
    m.add_class::<Client>()?;
    m.add_class::<QueryDuration>()?;
    m.add_class::<Replica>()?;
    m.add_class::<Expiration>()?;
    m.add_class::<CommitLevel>()?;
    m.add_class::<TxnState>()?;
    m.add_class::<CommitStatus>()?;
    m.add_class::<AbortStatus>()?;
    m.add_class::<LoopVarPart>()?;
    m.add_class::<SelectFlags>()?;
    m.add_class::<ModifyFlags>()?;
    m.add_class::<Txn>()?;
    m.add_class::<ReadModeAP>()?;
    m.add_class::<ReadModeSC>()?;
    m.add_class::<RecordExistsAction>()?;
    m.add_class::<GenerationPolicy>()?;
    m.add_class::<IndexType>()?;
    m.add_class::<CollectionIndexType>()?;
    m.add_class::<PrivilegeCode>()?;
    m.add_class::<Privilege>()?;
    m.add_class::<ResultCode>()?;

    m.add_class::<List>()?;
    m.add_class::<Map>()?;
    m.add_class::<Blob>()?;
    m.add_class::<GeoJSON>()?;
    m.add_class::<HLL>()?;

    m.add_class::<Key>()?;
    m.add_class::<Record>()?;
    m.add_class::<Recordset>()?;
    m.add_class::<Filter>()?;
    m.add_class::<Statement>()?;
    m.add_class::<ExpType>()?;
    m.add_class::<FilterExpression>()?;
    m.add_class::<ServerError>()?;
    m.add_class::<Operation>()?;
    m.add_class::<ListOperation>()?;
    m.add_class::<MapOperation>()?;
    m.add_class::<CTX>()?;
    m.add_class::<ListReturnType>()?;
    m.add_class::<ListSortFlags>()?;
    m.add_class::<BitOperation>()?;
    m.add_class::<HllOperation>()?;
    m.add_class::<HLLWriteFlags>()?;
    m.add_class::<HLLPolicy>()?;
    m.add_class::<ExpOperation>()?;
    m.add_class::<ExpWriteFlags>()?;
    m.add_class::<ExpReadFlags>()?;
    m.add_class::<RegexFlag>()?;
    m.add_class::<CdtOperation>()?;

    m.add_class::<BasePolicy>()?;
    m.add_class::<AdminPolicy>()?;
    m.add_class::<ReadPolicy>()?;

    // Add helper functions
    m.add_function(wrap_pyfunction!(null, m)?)?;
    m.add_function(wrap_pyfunction!(geojson, m)?)?;
    m.add_class::<AuthMode>()?;
    m.add_class::<ClientPolicy>()?;
    m.add_class::<WritePolicy>()?;
    m.add_class::<QueryPolicy>()?;
    m.add_class::<BatchRecord>()?;
    m.add_class::<BatchPolicy>()?;
    m.add_class::<BatchReadPolicy>()?;
    m.add_class::<BatchWritePolicy>()?;
    m.add_class::<BatchDeletePolicy>()?;
    m.add_class::<BatchUDFPolicy>()?;
    m.add_class::<BatchReadOp>()?;
    m.add_class::<BatchWriteOp>()?;
    m.add_class::<BatchDeleteOp>()?;
    m.add_class::<ListOrderType>()?;
    m.add_class::<ListWriteFlags>()?;
    m.add_class::<ListPolicy>()?;
    m.add_class::<MapOrder>()?;
    m.add_class::<MapWriteMode>()?;
    m.add_class::<MapWriteFlags>()?;
    m.add_class::<MapReturnType>()?;
    m.add_class::<SpecialValue>()?;
    m.add_class::<MapPolicy>()?;
    m.add_class::<BitwiseResizeFlags>()?;
    m.add_class::<BitWriteFlags>()?;
    m.add_class::<BitwiseOverflowActions>()?;
    m.add_class::<BitPolicy>()?;
    m.add_class::<PartitionStatus>()?;
    m.add_class::<PartitionFilter>()?;
    m.add_class::<UDFLang>()?;
    m.add_class::<TaskStatus>()?;
    m.add_class::<RegisterTask>()?;
    m.add_class::<UdfRemoveTask>()?;
    m.add_class::<IndexTask>()?;
    m.add_class::<DropIndexTask>()?;
    m.add_class::<ExecuteTask>()?;
    m.add_class::<Version>()?;
    m.add_class::<Node>()?;
    #[cfg(feature = "tls")]
    m.add_class::<TlsConfig>()?;

    m.add_function(wrap_pyfunction!(new_client, m)?)?;
    m.add_class::<completion::CompletionDrainer>()?;
    completion::init(py)?;

    // Create and register the exceptions submodule
    // Exceptions are only available via aerospike_async.exceptions submodule
    // They are not exposed at the top level to avoid namespace pollution
    let exceptions_module = PyModule::new(py, "exceptions")?;
    exceptions_module.add("AerospikeError", py.get_type::<AerospikeError>())?;
    exceptions_module.add("ServerError", py.get_type::<ServerError>())?;
    exceptions_module.add("UDFBadResponse", py.get_type::<UDFBadResponse>())?;
    exceptions_module.add("TimeoutError", py.get_type::<TimeoutError>())?;
    exceptions_module.add("BadResponse", py.get_type::<BadResponse>())?;
    exceptions_module.add("ConnectionError", py.get_type::<ConnectionError>())?;
    exceptions_module.add("InvalidNodeError", py.get_type::<InvalidNodeError>())?;
    exceptions_module.add("InvalidNamespaceError", py.get_type::<InvalidNamespaceError>())?;
    exceptions_module.add("NoMoreConnections", py.get_type::<NoMoreConnections>())?;
    exceptions_module.add("RecvError", py.get_type::<RecvError>())?;
    exceptions_module.add("Base64DecodeError", py.get_type::<Base64DecodeError>())?;
    exceptions_module.add("InvalidUTF8", py.get_type::<InvalidUTF8>())?;
    exceptions_module.add("ParseAddressError", py.get_type::<ParseAddressError>())?;
    exceptions_module.add("ParseIntError", py.get_type::<ParseIntError>())?;
    exceptions_module.add("ValueError", py.get_type::<ValueError>())?;
    exceptions_module.add("IoError", py.get_type::<IoError>())?;
    exceptions_module.add("PasswordHashError", py.get_type::<PasswordHashError>())?;
    exceptions_module.add("InvalidRustClientArgs", py.get_type::<InvalidRustClientArgs>())?;
    exceptions_module.add("ClientError", py.get_type::<ClientError>())?;
    exceptions_module.add("CommitFailedError", py.get_type::<CommitFailedError>())?;
    exceptions_module.add("MaxErrorRate", py.get_type::<MaxErrorRate>())?;
    exceptions_module.add("ResultCode", py.get_type::<ResultCode>())?;
    m.add_submodule(&exceptions_module)?;

    Ok(())
}
