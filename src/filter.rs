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

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;

use pyo3::exceptions::{PyKeyError, PyValueError};
use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::types::{PyBool, PyList};
use pyo3::{prelude::*, IntoPyObjectExt};

use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

use parking_lot::Mutex as PartitionMutex;
use tokio::sync::Mutex;

use aerospike_core::query::RecordStream;

use crate::CollectionIndexType;
use crate::cdt::CTX;
use crate::errors::RustClientError;
use crate::expressions::FilterExpression;
use crate::operations::bins_flag;
use crate::record::{Key, PythonValue, Record};

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  PartitionStatus
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(name = "PartitionStatus", module = "_aerospike_async_native")]
    #[derive(Debug)]
    pub struct PartitionStatus {
        pub(crate) _as: aerospike_core::query::PartitionStatus,
    }

    // Note: We can't derive Clone because PartitionStatus has private fields
    // If cloning is needed, we'd need to add a method in the Rust core

    // Note: PartitionStatus can be constructed from Python using PartitionStatus(id)
    // Users typically get PartitionStatus instances from query/scan operations,
    // but can also create new instances manually when needed.

    #[gen_stub_pymethods]
    #[pymethods]
    impl PartitionStatus {
        /// Create a new PartitionStatus with the specified partition ID.
        ///
        /// The `retry` field defaults to `true`, and other fields can be set via setters.
        #[new]
        pub fn new(id: u16) -> Self {
            PartitionStatus {
                _as: aerospike_core::query::PartitionStatus {
                    id,
                    retry: true,
                    bval: None,
                    digest: None,
                    node: None,
                    sequence: None,
                },
            }
        }

        #[getter]
        pub fn get_bval(&self) -> Option<u64> {
            self._as.bval
        }

        #[setter]
        pub fn set_bval(&mut self, bval: Option<u64>) {
            self._as.bval = bval;
        }

        #[getter]
        pub fn get_id(&self) -> u16 {
            self._as.id
        }

        #[getter]
        pub fn get_retry(&self) -> bool {
            self._as.retry
        }

        #[setter]
        pub fn set_retry(&mut self, retry: bool) {
            self._as.retry = retry;
        }

        #[getter]
        pub fn get_digest(&self) -> Option<String> {
            self._as.digest.map(hex::encode)
        }

        #[setter]
        pub fn set_digest(&mut self, digest: Option<String>) -> PyResult<()> {
            match digest {
                None => {
                    self._as.digest = None;
                }
                Some(hex_str) => {
                    let bytes = hex::decode(&hex_str)
                        .map_err(|e| PyValueError::new_err(format!("Invalid hex digest: {}", e)))?;
                    if bytes.len() != 20 {
                        return Err(PyValueError::new_err(format!(
                            "Digest must be exactly 20 bytes (40 hex chars), got {} bytes",
                            bytes.len()
                        )));
                    }
                    let mut digest_array = [0u8; 20];
                    digest_array.copy_from_slice(&bytes);
                    self._as.digest = Some(digest_array);
                }
            }
            Ok(())
        }


        /// Dictionary-style access for convenience (in addition to getters/setters).
        /// Supported keys: 'id', 'bval', 'retry', 'digest'
        /// Example: ps['id'], ps['bval'] = 123
        pub fn __getitem__(&self, key: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
            let py = key.py();
            let key_str = key.extract::<String>()?;
            match key_str.as_str() {
                "id" => Ok(self.get_id().into_pyobject(py).unwrap().into_any().into()),
                "bval" => match self.get_bval() {
                    Some(v) => Ok(v.into_pyobject(py).unwrap().into_any().into()),
                    None => Ok(py.None()),
                },
                "retry" => Ok(PyBool::new(py, self.get_retry()).into_bound_py_any(py).unwrap().into()),
                "digest" => match self.get_digest() {
                    Some(v) => Ok(v.into_pyobject(py).unwrap().into_any().into()),
                    None => Ok(py.None()),
                },
                _ => Err(PyKeyError::new_err(format!("Unknown key: '{}'. Valid keys: 'id', 'bval', 'retry', 'digest'", key_str))),
            }
        }

        /// Dictionary-style assignment for convenience (in addition to getters/setters).
        /// Supported keys: 'bval', 'retry', 'digest'
        /// Note: 'id' is read-only and cannot be set.
        pub fn __setitem__(&mut self, key: &Bound<'_, PyAny>, value: &Bound<'_, PyAny>) -> PyResult<()> {
            let key_str = key.extract::<String>()?;
            match key_str.as_str() {
                "id" => Err(PyValueError::new_err("'id' is read-only and cannot be set")),
                "bval" => {
                    let bval: Option<u64> = value.extract()?;
                    self.set_bval(bval);
                    Ok(())
                }
                "retry" => {
                    let retry: bool = value.extract()?;
                    self.set_retry(retry);
                    Ok(())
                }
                "digest" => {
                    let digest: Option<String> = value.extract()?;
                    self.set_digest(digest)?;
                    Ok(())
                }
                _ => Err(PyKeyError::new_err(format!("Unknown key: '{}'. Valid keys: 'bval', 'retry', 'digest'", key_str))),
            }
        }
    }

    //  PartitionFilter
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(from_py_object, 
        name = "PartitionFilter",
        module = "_aerospike_async_native",
        freelist = 1000
    )]
    #[derive(Debug, Clone)]
    pub struct PartitionFilter {
        pub(crate) _as: aerospike_core::query::PartitionFilter,
    }

    impl Default for PartitionFilter {
        fn default() -> Self {
            PartitionFilter {
                _as: aerospike_core::query::PartitionFilter::all(),
            }
        }
    }

    /// Trait implemented by most policy types; policies that implement this trait typically encompass
    /// an instance of `PartitionFilter`.
    #[gen_stub_pymethods]
    #[pymethods]
    impl PartitionFilter {
        #[new]
        pub fn new() -> Self {
            Self::default()
        }

        pub fn done(&self) -> bool {
            self._as.done()
        }

        #[staticmethod]
        pub fn all() -> Self {
            Self {
                _as: aerospike_core::query::PartitionFilter::all(),
            }
        }

        #[staticmethod]
        pub fn by_id(id: usize) -> Self {
            Self {
                _as: aerospike_core::query::PartitionFilter::by_id(id),
            }
        }

        #[staticmethod]
        pub fn by_key(key: &Key) -> Self {
            Self {
                _as: aerospike_core::query::PartitionFilter::by_key(&key._as),
            }
        }

        #[staticmethod]
        pub fn by_range(begin: usize, count: usize) -> Self {
            Self {
                _as: aerospike_core::query::PartitionFilter::by_range(begin, count),
            }
        }

        #[getter]
        pub fn get_begin(&self) -> usize {
            self._as.begin
        }

        #[setter]
        pub fn set_begin(&mut self, begin: usize) {
            self._as.begin = begin;
        }

        #[getter]
        pub fn get_count(&self) -> usize {
            self._as.count
        }

        #[setter]
        pub fn set_count(&mut self, count: usize) {
            self._as.count = count;
        }

        #[getter]
        pub fn get_digest(&self) -> Option<String> {
            self._as.digest.map(hex::encode)
        }

        #[setter]
        pub fn set_digest(&mut self, digest: Option<String>) -> PyResult<()> {
            match digest {
                None => {
                    self._as.digest = None;
                }
                Some(hex_str) => {
                    let bytes = hex::decode(&hex_str)
                        .map_err(|e| PyValueError::new_err(format!("Invalid hex digest: {}", e)))?;
                    if bytes.len() != 20 {
                        return Err(PyValueError::new_err(format!(
                            "Digest must be exactly 20 bytes (40 hex chars), got {} bytes",
                            bytes.len()
                        )));
                    }
                    let mut digest_array = [0u8; 20];
                    digest_array.copy_from_slice(&bytes);
                    self._as.digest = Some(digest_array);
                }
            }
            Ok(())
        }

        #[getter]
        pub fn get_partitions(&self, py: Python) -> PyResult<Py<PyAny>> {
            match &self._as.partitions {
                None => Ok(py.None()),
                Some(partitions) => {
                    let mut py_partitions = Vec::new();
                    for arc_mutex_status in partitions.iter() {
                        // parking_lot Mutex: a synchronous lock, so no Tokio runtime
                        // handle is needed — works from a Python asyncio context.
                        let status = arc_mutex_status.lock();
                        let py_status = PartitionStatus {
                            _as: aerospike_core::query::PartitionStatus {
                                id: status.id,
                                retry: status.retry,
                                bval: status.bval,
                                digest: status.digest,
                                node: status.node.clone(),
                                sequence: status.sequence,
                            },
                        };
                        py_partitions.push(Py::new(py, py_status)?);
                    }
                    let list = PyList::empty(py);
                    for item in py_partitions {
                        list.append(item)?;
                    }
                    Ok(list.into())
                }
            }
        }

        #[setter]
        pub fn set_partitions(&mut self, partitions: Option<Bound<'_, PyList>>) -> PyResult<()> {
            match partitions {
                None => {
                    self._as.partitions = None;
                }
                Some(py_partitions) => {
                    let mut rust_partitions = Vec::new();
                    for item in py_partitions.iter() {
                        let status: PyRef<PartitionStatus> = item.extract()?;
                        rust_partitions.push(PartitionMutex::new(
                            aerospike_core::query::PartitionStatus {
                                id: status._as.id,
                                retry: status._as.retry,
                                bval: status._as.bval,
                                digest: status._as.digest,
                                node: None,
                                sequence: None,
                            },
                        ));
                    }
                    self._as.partitions = Some(Arc::new(rust_partitions));
                }
            }
            Ok(())
        }
    }
    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  Statement
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Query statement parameters.
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(from_py_object, 
        name = "Statement",
        module = "_aerospike_async_native",
        subclass,
        freelist = 1000
    )]
    #[derive(Clone)]
    pub struct Statement {
        pub(crate) _as: aerospike_core::Statement,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl Statement {
        #[new]
        #[pyo3(signature = (namespace, set_name = None, bins = None))]
        pub fn __construct(
            namespace: &str,
            set_name: Option<&str>,
            bins: Option<Vec<String>>,
        ) -> Self {
            let set_name_str = set_name.unwrap_or("");
            Statement {
                _as: aerospike_core::Statement::new(namespace, set_name_str, bins_flag(bins)),
            }
        }

        #[getter]
        pub fn get_filters(&self) -> Option<Vec<Filter>> {
            self._as
                .filters
                .as_ref()
                .map(|filters| filters.iter().map(|f| Filter { _as: f.clone() }).collect())
        }

        #[setter]
        pub fn set_filters(&mut self, filters: Option<Vec<Filter>>) {
            match filters {
                None => self._as.filters = None,
                Some(filters) => {
                    self._as.filters = Some(filters.iter().map(|qf| qf._as.clone()).collect());
                }
            };
        }

        #[getter]
        pub fn get_set_name(&self) -> Option<String> {
            if self._as.set_name.is_empty() {
                None
            } else {
                Some(self._as.set_name.clone())
            }
        }

        #[setter]
        pub fn set_set_name(&mut self, set_name: Option<String>) {
            self._as.set_name = set_name.unwrap_or_default();
        }

        /// Set Lua aggregation function parameters for query aggregation.
        ///
        /// Args:
        ///     package_name: Name of the Lua package/module containing the aggregation function.
        ///     function_name: Name of the Lua aggregation function.
        ///     function_args: Optional list of arguments to pass to the function.
        #[pyo3(signature = (package_name, function_name, function_args = None))]
        pub fn set_aggregate_function(
            &mut self,
            package_name: &str,
            function_name: &str,
            function_args: Option<Vec<PythonValue>>,
        ) {
            let args: Option<Vec<aerospike_core::Value>> = function_args
                .map(|args| args.into_iter().map(|v| v.into()).collect());
            self._as.set_aggregate_function(
                package_name,
                function_name,
                args.as_deref(),
            );
        }

        /// Attach an ops projection. The server returns the result of these
        /// operations for each matching record instead of the bin set
        /// configured via ``bins``. Mutually exclusive with ``bins`` (the
        /// server uses ``operations`` if both are set).
        ///
        /// Foreground queries accept only read ops. Server versions before
        /// 8.1.2 only accept the basic ``Read`` op here; 8.1.2+ also accepts
        /// CDT, expression, bit, and HLL reads.
        pub fn set_operations(&mut self, py: Python<'_>, ops: Vec<Py<PyAny>>) -> PyResult<()> {
            let py_ops_with_ctx = crate::operations::extract_py_ops_with_ctx(py, &ops)?;
            let (core_ops, _has_write) =
                crate::operations::convert_ops_with_ctx_to_core(&py_ops_with_ctx, false)?;
            self._as.set_operations(core_ops);
            Ok(())
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  Filter
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Query filter definition. Currently, only one filter is allowed in a Statement, and must target a
    /// bin that has a secondary index (or use `*_by_index` with the index name).
    ///
    /// Build filters from the class static methods, for example `equal`, `range`, `contains`,
    /// `contains_range`, `within_region`, `within_radius`, `regions_containing_point`, and the
    /// corresponding `equal_by_index`, `range_by_index`, `contains_by_index`, `contains_range_by_index`,
    /// `within_region_by_index`, `within_radius_by_index`, and `regions_containing_point_by_index`.
    ///
    /// Use instance methods `context` and `expression` to attach a CDT path or expression-based index
    /// to a filter (for example `Filter.equal("bin", 1).context([CTX.list_index(0)])`).
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(from_py_object, 
        name = "Filter",
        module = "_aerospike_async_native",
        subclass,
        freelist = 1000
    )]
    #[derive(Clone, Debug)]
    pub struct Filter {
        pub(crate) _as: aerospike_core::query::Filter,
    }

    impl fmt::Display for Filter {
        fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
            write!(f, "Filter({:?})", self._as)
        }
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl Filter {
        fn __str__(&self) -> PyResult<String> {
            Ok(format!("{}", self))
        }

        fn __repr__(&self) -> PyResult<String> {
            Ok(format!("Filter({:?})", self._as))
        }

        /// Attach a CDT context path for a secondary index on a nested list or map element.
        pub fn context(&self, ctx: Vec<CTX>) -> Self {
            let core_ctx = crate::cdt::ctx_to_vec(&ctx);
            Filter {
                _as: self._as.clone().context(core_ctx),
            }
        }

        /// Attach the expression used when the secondary index was created with
        /// `create_index_using_expression`.
        pub fn expression(&self, exp: &FilterExpression) -> Self {
            Filter {
                _as: self._as.clone().expression(exp._as.clone()),
            }
        }

        #[staticmethod]
        pub fn equal(bin_name: &str, value: PythonValue) -> Self {
            Filter {
                _as: aerospike_core::query::Filter::equal(
                    bin_name,
                    aerospike_core::Value::from(value),
                ),
            }
        }

        #[staticmethod]
        pub fn equal_by_index(index_name: &str, value: PythonValue) -> Self {
            Filter {
                _as: aerospike_core::query::Filter::equal_by_index(
                    index_name,
                    aerospike_core::Value::from(value),
                ),
            }
        }

        #[staticmethod]
        pub fn range(bin_name: &str, begin: PythonValue, end: PythonValue) -> Self {
            Filter {
                _as: aerospike_core::query::Filter::range(
                    bin_name,
                    aerospike_core::Value::from(begin),
                    aerospike_core::Value::from(end),
                ),
            }
        }

        #[staticmethod]
        pub fn range_by_index(index_name: &str, begin: PythonValue, end: PythonValue) -> Self {
            Filter {
                _as: aerospike_core::query::Filter::range_by_index(
                    index_name,
                    aerospike_core::Value::from(begin),
                    aerospike_core::Value::from(end),
                ),
            }
        }

        #[staticmethod]
        pub fn contains(
            bin_name: &str,
            value: PythonValue,
            cit: Option<&CollectionIndexType>,
        ) -> Self {
            let default = CollectionIndexType::Default;
            let cit = cit.unwrap_or(&default);
            Filter {
                _as: aerospike_core::query::Filter::contains(
                    bin_name,
                    aerospike_core::Value::from(value),
                    aerospike_core::query::CollectionIndexType::from(cit),
                ),
            }
        }

        #[staticmethod]
        pub fn contains_by_index(
            index_name: &str,
            value: PythonValue,
            cit: Option<&CollectionIndexType>,
        ) -> Self {
            let default = CollectionIndexType::Default;
            let cit = cit.unwrap_or(&default);
            Filter {
                _as: aerospike_core::query::Filter::contains_by_index(
                    index_name,
                    aerospike_core::Value::from(value),
                    aerospike_core::query::CollectionIndexType::from(cit),
                ),
            }
        }

        #[staticmethod]
        pub fn contains_range(
            bin_name: &str,
            begin: PythonValue,
            end: PythonValue,
            cit: Option<&CollectionIndexType>,
        ) -> Self {
            let default = CollectionIndexType::Default;
            let cit = cit.unwrap_or(&default);
            Filter {
                _as: aerospike_core::query::Filter::contains_range(
                    bin_name,
                    aerospike_core::Value::from(begin),
                    aerospike_core::Value::from(end),
                    aerospike_core::query::CollectionIndexType::from(cit),
                ),
            }
        }

        #[staticmethod]
        pub fn contains_range_by_index(
            index_name: &str,
            begin: PythonValue,
            end: PythonValue,
            cit: Option<&CollectionIndexType>,
        ) -> Self {
            let default = CollectionIndexType::Default;
            let cit = cit.unwrap_or(&default);
            Filter {
                _as: aerospike_core::query::Filter::contains_range_by_index(
                    index_name,
                    aerospike_core::Value::from(begin),
                    aerospike_core::Value::from(end),
                    aerospike_core::query::CollectionIndexType::from(cit),
                ),
            }
        }

        #[staticmethod]
        pub fn within_region(
            bin_name: &str,
            region: &str,
            cit: Option<&CollectionIndexType>,
        ) -> Self {
            let default = CollectionIndexType::Default;
            let cit = cit.unwrap_or(&default);
            if matches!(cit, CollectionIndexType::Default) {
                Filter {
                    _as: aerospike_core::query::Filter::geo_within_region(bin_name, region),
                }
            } else {
                Filter {
                    _as: aerospike_core::query::Filter::geo_within_region_cit(
                        bin_name,
                        region,
                        aerospike_core::query::CollectionIndexType::from(cit),
                    ),
                }
            }
        }

        #[staticmethod]
        pub fn within_region_by_index(
            index_name: &str,
            region: &str,
            cit: Option<&CollectionIndexType>,
        ) -> Self {
            let default = CollectionIndexType::Default;
            let cit = cit.unwrap_or(&default);
            if matches!(cit, CollectionIndexType::Default) {
                Filter {
                    _as: aerospike_core::query::Filter::geo_within_region_by_index(
                        index_name,
                        region,
                    ),
                }
            } else {
                Filter {
                    _as: aerospike_core::query::Filter::geo_within_region_by_index_cit(
                        index_name,
                        region,
                        aerospike_core::query::CollectionIndexType::from(cit),
                    ),
                }
            }
        }

        #[staticmethod]
        /// Public API uses (lng, lat) to match GeoJSON [longitude, latitude].
        pub fn within_radius(
            bin_name: &str,
            lng: f64,
            lat: f64,
            radius: f64,
            cit: Option<&CollectionIndexType>,
        ) -> Self {
            let default = CollectionIndexType::Default;
            let cit = cit.unwrap_or(&default);
            if matches!(cit, CollectionIndexType::Default) {
                Filter {
                    _as: aerospike_core::query::Filter::geo_within_radius(
                        bin_name,
                        lng,
                        lat,
                        radius,
                    ),
                }
            } else {
                Filter {
                    _as: aerospike_core::query::Filter::geo_within_radius_cit(
                        bin_name,
                        lng,
                        lat,
                        radius,
                        aerospike_core::query::CollectionIndexType::from(cit),
                    ),
                }
            }
        }

        #[staticmethod]
        pub fn within_radius_by_index(
            index_name: &str,
            lng: f64,
            lat: f64,
            radius: f64,
            cit: Option<&CollectionIndexType>,
        ) -> Self {
            let default = CollectionIndexType::Default;
            let cit = cit.unwrap_or(&default);
            if matches!(cit, CollectionIndexType::Default) {
                Filter {
                    _as: aerospike_core::query::Filter::geo_within_radius_by_index(
                        index_name,
                        lng,
                        lat,
                        radius,
                    ),
                }
            } else {
                Filter {
                    _as: aerospike_core::query::Filter::geo_within_radius_by_index_cit(
                        index_name,
                        lng,
                        lat,
                        radius,
                        aerospike_core::query::CollectionIndexType::from(cit),
                    ),
                }
            }
        }

        #[staticmethod]
        pub fn regions_containing_point(
            bin_name: &str,
            point: &str,
            cit: Option<&CollectionIndexType>,
        ) -> Self {
            let default = CollectionIndexType::Default;
            let cit = cit.unwrap_or(&default);
            if matches!(cit, CollectionIndexType::Default) {
                Filter {
                    _as: aerospike_core::query::Filter::geo_contains(bin_name, point),
                }
            } else {
                Filter {
                    _as: aerospike_core::query::Filter::geo_contains_cit(
                        bin_name,
                        point,
                        aerospike_core::query::CollectionIndexType::from(cit),
                    ),
                }
            }
        }

        #[staticmethod]
        pub fn regions_containing_point_by_index(
            index_name: &str,
            point: &str,
            cit: Option<&CollectionIndexType>,
        ) -> Self {
            let default = CollectionIndexType::Default;
            let cit = cit.unwrap_or(&default);
            if matches!(cit, CollectionIndexType::Default) {
                Filter {
                    _as: aerospike_core::query::Filter::geo_contains_by_index(
                        index_name,
                        point,
                    ),
                }
            } else {
                Filter {
                    _as: aerospike_core::query::Filter::geo_contains_by_index_cit(
                        index_name,
                        point,
                        aerospike_core::query::CollectionIndexType::from(cit),
                    ),
                }
            }
        }
    }
    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  Recordset
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Virtual collection of records retrieved through queries and scans. During a query/scan,
    /// multiple threads will retrieve records from the server nodes and put these records on an
    /// internal queue managed by the recordset. The single user thread consumes these records from the
    /// queue.
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(from_py_object, 
        name = "Recordset",
        module = "_aerospike_async_native",
        subclass,
        freelist = 1000
    )]
    pub struct Recordset {
        pub(crate) _as: Arc<aerospike_core::Recordset>,
        pub(crate) _stream: Arc<Mutex<Option<Pin<Box<RecordStream>>>>>,
        // Some when built from `Client::query` (async); None when built from
        // `Client::query_blocking`. Async iteration / `partition_filter()`
        // route through the bridge for loop-affinity + per-Client runtime;
        // a None bridge surfaces an explicit refusal rather than silently
        // routing through the global runtime.
        pub(crate) bridge: Option<crate::completion::CompletionBridge>,
    }

    impl Clone for Recordset {
        fn clone(&self) -> Self {
            // The bridge holds a Py<...> which needs the GIL to bump
            // refcount; PyO3 from_py_object expects a plain Clone. Drop the
            // bridge on clone — async paths read bridge from the original
            // (via PyRef in `__aiter__`), so this only loses bridge access
            // for from_py_object conversions which don't iterate.
            Recordset {
                _as: self._as.clone(),
                _stream: Arc::new(Mutex::new(None)),
                bridge: None,
            }
        }
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl Recordset {
        pub fn close(&self) {
            self._as.close();
        }

        #[getter]
        pub fn get_active(&self) -> bool {
            self._as.is_active()
        }

        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Optional[PartitionFilter]]", imports=("typing", "aerospike_async")))]
        pub fn partition_filter<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
            let bridge = self.bridge.as_ref().ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "Cannot await partition_filter() on a Recordset created via \
                     query_blocking. Use query() in an async context.",
                )
            })?;
            let recordset = self._as.clone();

            crate::completion::batched_future_into_py(bridge, py, async move {
                match recordset.partition_filter().await {
                    Some(pf) => Ok(Some(PartitionFilter { _as: pf })),
                    None => Ok(None),
                }
            })
        }

        pub fn partition_filter_sync(&self, py: Python<'_>) -> PyResult<Option<PartitionFilter>> {
            // Synchronous counterpart to `partition_filter()` for the blocking
            // query path. The async method returns an awaitable and needs a
            // CompletionBridge; a Recordset created via `query_blocking` has no
            // event loop to await on, so block on the per-thread runtime instead
            // — the same pattern `__next__` uses. Core's `partition_filter()`
            // only locks the tracker and clones out the cursor (no network IO),
            // so blocking here is cheap.
            let asyncio = py.import("asyncio")?;
            if asyncio.call_method0("get_running_loop").is_ok() {
                return Err(pyo3::exceptions::PyRuntimeError::new_err(
                    "Cannot call partition_filter_sync() from within an async \
                     context. Use `await partition_filter()` instead.",
                ));
            }
            let recordset = self._as.clone();
            let rt = pyo3_async_runtimes::tokio::get_runtime();
            let pf = py.detach(|| {
                rt.block_on(async move { recordset.partition_filter().await })
            });
            Ok(pf.map(|pf| PartitionFilter { _as: pf }))
        }

        fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
            slf
        }

        fn __anext__<'a>(&'a mut self, py: Python<'a>) -> PyResult<Py<PyAny>> {
            let bridge = self.bridge.as_ref().ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "Cannot async-iterate a Recordset created via query_blocking. \
                     Use sync iteration (`for record in recordset:`) or recreate \
                     the recordset via Client.query() in an async context.",
                )
            })?;
            let recordset = self._as.clone();
            let stream_mutex = self._stream.clone();

            crate::completion::batched_future_into_py(bridge, py, async move {
                // Initialize stream if needed, then poll
                let mut stream_opt = stream_mutex.lock().await;
                if stream_opt.is_none() {
                    *stream_opt = Some(Box::pin(recordset.clone().into_stream()));
                }

                if let Some(ref mut stream) = *stream_opt {
                    use futures::StreamExt;
                    // Return a plain (Send) Rust value; the CompletionBridge's
                    // converter builds the Python `Record` on the drainer/loop
                    // thread. Never `Python::attach` here — this runs on a Tokio
                    // worker, and registering a PyThreadState on a worker
                    // segfaults on free-threaded finalization teardown (see the
                    // invariant in waker.rs and the lazy pattern in errors.rs).
                    // `PyErr::from(RustClientError(..))` is lazy, so the error
                    // arm never attaches on the worker either.
                    match stream.as_mut().next().await {
                        Some(Ok(rec)) => {
                            Ok(Record { _as: rec, cached_bins: None, cached_results: None })
                        }
                        Some(Err(e)) => Err(PyErr::from(RustClientError(e))),
                        None => Err(PyStopAsyncIteration::new_err("Recordset iteration complete")),
                    }
                } else {
                    Err(PyStopAsyncIteration::new_err("Recordset iteration complete"))
                }
            })
            .map(|bound| bound.unbind())
        }

        // Blocking iteration — returned by `Client.query_blocking()`.  Same
        // shape as the async path: drive the underlying stream, but block
        // the Python thread (releasing the GIL via py.detach) instead of
        // returning an awaitable each step.  Raising the standard
        // `StopIteration` ends the loop the Pythonic way.
        fn __iter__(&self) -> Self {
            self.clone()
        }

        fn __next__(&mut self, py: Python<'_>) -> PyResult<Record> {
            // The async-context guard is checked here too, not just in
            // `query_blocking`: a user could legally create the Recordset
            // before entering an async context and then iterate it from
            // inside one.  That guard lives in lib.rs as
            // `check_not_in_async_context`; here we duplicate the same
            // behavior via the canonical asyncio probe.
            let asyncio = py.import("asyncio")?;
            if asyncio.call_method0("get_running_loop").is_ok() {
                return Err(pyo3::exceptions::PyRuntimeError::new_err(
                    "Cannot iterate a blocking Recordset from within an async \
                     context.  Use `async for record in recordset:` instead.",
                ));
            }

            let recordset = self._as.clone();
            let stream_mutex = self._stream.clone();
            let rt = pyo3_async_runtimes::tokio::get_runtime();

            let result = py.detach(|| {
                rt.block_on(async move {
                    let mut stream_opt = stream_mutex.lock().await;
                    if stream_opt.is_none() {
                        *stream_opt = Some(Box::pin(recordset.clone().into_stream()));
                    }
                    let stream = stream_opt.as_mut().unwrap();
                    use futures::StreamExt;
                    match stream.as_mut().next().await {
                        Some(Ok(rec)) => Ok(Some(rec)),
                        Some(Err(e)) => Err(PyErr::from(RustClientError(e))),
                        None => Ok(None),
                    }
                })
            })?;

            match result {
                Some(rec) => Ok(Record { _as: rec, cached_bins: None, cached_results: None }),
                None => Err(pyo3::exceptions::PyStopIteration::new_err(())),
            }
        }
    }
