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

use std::collections::HashMap;

use pyo3::types::PyDict;
use pyo3::prelude::*;

use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};



use crate::enums::*;
use crate::expressions::FilterExpression;
use crate::operations::{extract_py_ops_with_ctx, OpWithCtx};
use crate::record::{Key, Record};
#[cfg(feature = "tls")]
use crate::TlsConfig;

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  BasePolicy
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(
        name = "BasePolicy",
        subclass,
        freelist = 1000,
        module = "_aerospike_async_native"
    )]
    #[derive(Debug, Clone)]
    pub struct BasePolicy {
        pub(crate) _as: aerospike_core::policy::BasePolicy,
    }

    /// Trait implemented by most policy types; policies that implement this trait typically encompass
    /// an instance of `BasePolicy`.
    impl Default for BasePolicy {
        fn default() -> Self {
            Self::new()
        }
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl BasePolicy {
        #[new]
        pub fn new() -> Self {
            BasePolicy {
                _as: aerospike_core::policy::BasePolicy::default(),
            }
        }

        #[getter]
        pub fn get_consistency_level(&self) -> ConsistencyLevel {
            (&self._as.consistency_level).into()
        }

        #[setter]
        pub fn set_consistency_level(&mut self, consistency_level: ConsistencyLevel) {
            self._as.consistency_level = match consistency_level {
                ConsistencyLevel::ConsistencyOne => {
                    aerospike_core::ConsistencyLevel::ConsistencyOne
                }
                ConsistencyLevel::ConsistencyAll => {
                    aerospike_core::ConsistencyLevel::ConsistencyAll
                }
            };
        }

        #[getter]
        pub fn get_total_timeout(&self) -> u64 {
            self._as.total_timeout as u64
        }

        #[setter]
        pub fn set_total_timeout(&mut self, timeout_millis: u64) {
            self._as.total_timeout = timeout_millis as u32;
        }

        #[getter]
        pub fn get_max_retries(&self) -> usize {
            self._as.max_retries
        }

        #[setter]
        pub fn set_max_retries(&mut self, max_retries: usize) {
            self._as.max_retries = max_retries;
        }

        #[getter]
        pub fn get_sleep_between_retries(&self) -> u64 {
            self._as.sleep_between_retries as u64
        }

        #[setter]
        pub fn set_sleep_between_retries(&mut self, sleep_between_retries_millis: u64) {
            self._as.sleep_between_retries = sleep_between_retries_millis.min(u32::MAX as u64) as u32;
        }

        #[getter]
        pub fn get_filter_expression(&self) -> Option<FilterExpression> {
            self._as.filter_expression.as_ref().map(|fe| FilterExpression { _as: fe.clone() })
        }

        #[setter]
        pub fn set_filter_expression(&mut self, filter_expression: Option<FilterExpression>) {
            match filter_expression {
                Some(fe) => self._as.filter_expression = Some(fe._as),
                None => self._as.filter_expression = None,
            }
        }

        #[getter]
        pub fn get_socket_timeout(&self) -> u32 {
            self._as.socket_timeout
        }

        #[setter]
        pub fn set_socket_timeout(&mut self, socket_timeout: u32) {
            self._as.socket_timeout = socket_timeout;
        }

        #[getter]
        pub fn get_read_touch_ttl(&self) -> i32 {
            match self._as.read_touch_ttl {
                aerospike_core::ReadTouchTTL::Percent(pct) => pct as i32,
                aerospike_core::ReadTouchTTL::ServerDefault => 0,
                aerospike_core::ReadTouchTTL::DontReset => -1,
            }
        }

        #[setter]
        pub fn set_read_touch_ttl(&mut self, value: i32) -> PyResult<()> {
            self._as.read_touch_ttl = match value {
                -1 => aerospike_core::ReadTouchTTL::DontReset,
                0 => aerospike_core::ReadTouchTTL::ServerDefault,
                pct if (1..=100).contains(&pct) => aerospike_core::ReadTouchTTL::Percent(pct as u8),
                _ => return Err(pyo3::exceptions::PyValueError::new_err(
                    format!("read_touch_ttl must be -1 (don't reset), 0 (server default), or 1-100 (percentage), got {value}")
                )),
            };
            Ok(())
        }
    }

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(
        name = "AdminPolicy",
        freelist = 1000,
        module = "_aerospike_async_native",
        subclass
    )]
    #[derive(Debug, Clone)]
    pub struct AdminPolicy {
        pub(crate) _as: aerospike_core::AdminPolicy,
    }

    impl Default for AdminPolicy {
        fn default() -> Self {
            Self::new()
        }
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl AdminPolicy {
        #[new]
        pub fn new() -> Self {
            AdminPolicy {
                _as: aerospike_core::AdminPolicy::default(),
            }
        }

        #[getter]
        pub fn get_timeout(&self) -> u32 {
            self._as.timeout
        }

        #[setter]
        pub fn set_timeout(&mut self, timeout: u32) {
            self._as.timeout = timeout;
        }
    }

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(
        name = "ReadPolicy",
        freelist = 1000,
        module = "_aerospike_async_native",
        extends = BasePolicy,
        subclass
    )]
    #[derive(Debug, Clone)]
    pub struct ReadPolicy {
        pub(crate) _as: aerospike_core::ReadPolicy,
    }

    /// `ReadPolicy` encapsulates parameters for all write operations.
    #[pymethods]
    impl ReadPolicy {
        #[new]
        pub fn new() -> PyClassInitializer<Self> {
            let read_policy = ReadPolicy {
                _as: aerospike_core::ReadPolicy::default(),
            };
            let base_policy = BasePolicy::new();

            PyClassInitializer::from(base_policy).add_subclass(read_policy)
        }

        #[getter]
        pub fn get_replica(&self) -> Replica {
            (&self._as.replica).into()
        }

        #[setter]
        pub fn set_replica(&mut self, replica: Replica) {
            self._as.replica = (&replica).into();
        }

        #[getter]
        pub fn get_base_policy(&self) -> BasePolicy {
            BasePolicy {
                _as: self._as.base_policy.clone(),
            }
        }

        #[setter]
        pub fn set_base_policy(&mut self, base_policy: BasePolicy) {
            self._as.base_policy = base_policy._as;
        }

        // Override BasePolicy methods to sync with internal base_policy
        #[getter]
        pub fn get_total_timeout(&self) -> u64 {
            self._as.base_policy.total_timeout as u64
        }

        #[setter]
        pub fn set_total_timeout(&mut self, timeout_millis: u64) {
            self._as.base_policy.total_timeout = timeout_millis as u32;
        }

        #[getter]
        pub fn get_max_retries(&self) -> usize {
            self._as.base_policy.max_retries
        }

        #[setter]
        pub fn set_max_retries(&mut self, max_retries: usize) {
            self._as.base_policy.max_retries = max_retries;
        }

        #[getter]
        pub fn get_sleep_between_retries(&self) -> u64 {
            self._as.base_policy.sleep_between_retries as u64
        }

        #[setter]
        pub fn set_sleep_between_retries(&mut self, sleep_between_retries_millis: u64) {
            self._as.base_policy.sleep_between_retries =
                sleep_between_retries_millis.min(u32::MAX as u64) as u32;
        }

        #[getter]
        pub fn get_consistency_level(&self) -> ConsistencyLevel {
            (&self._as.base_policy.consistency_level).into()
        }

        #[setter]
        pub fn set_consistency_level(&mut self, consistency_level: ConsistencyLevel) {
            self._as.base_policy.consistency_level = match consistency_level {
                ConsistencyLevel::ConsistencyOne => {
                    aerospike_core::ConsistencyLevel::ConsistencyOne
                }
                ConsistencyLevel::ConsistencyAll => {
                    aerospike_core::ConsistencyLevel::ConsistencyAll
                }
            };
        }

        #[getter]
        pub fn get_socket_timeout(&self) -> u32 {
            self._as.base_policy.socket_timeout
        }

        #[setter]
        pub fn set_socket_timeout(&mut self, socket_timeout: u32) {
            self._as.base_policy.socket_timeout = socket_timeout;
        }

        // Override filter expression methods to sync with internal base_policy
        #[getter]
        pub fn get_filter_expression(&self) -> Option<FilterExpression> {
            self._as.base_policy.filter_expression.as_ref().map(|fe| FilterExpression { _as: fe.clone() })
        }

        #[setter]
        pub fn set_filter_expression(&mut self, filter_expression: Option<FilterExpression>) {
            match filter_expression {
                Some(fe) => self._as.base_policy.filter_expression = Some(fe._as),
                None => self._as.base_policy.filter_expression = None,
            }
        }

        #[getter]
        pub fn get_read_touch_ttl(&self) -> i32 {
            match self._as.base_policy.read_touch_ttl {
                aerospike_core::ReadTouchTTL::Percent(pct) => pct as i32,
                aerospike_core::ReadTouchTTL::ServerDefault => 0,
                aerospike_core::ReadTouchTTL::DontReset => -1,
            }
        }

        #[setter]
        pub fn set_read_touch_ttl(&mut self, value: i32) -> PyResult<()> {
            self._as.base_policy.read_touch_ttl = match value {
                -1 => aerospike_core::ReadTouchTTL::DontReset,
                0 => aerospike_core::ReadTouchTTL::ServerDefault,
                pct if (1..=100).contains(&pct) => aerospike_core::ReadTouchTTL::Percent(pct as u8),
                _ => return Err(pyo3::exceptions::PyValueError::new_err(
                    format!("read_touch_ttl must be -1 (don't reset), 0 (server default), or 1-100 (percentage), got {value}")
                )),
            };
            Ok(())
        }
    }

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(
        name = "WritePolicy",
        module = "_aerospike_async_native",
        extends = BasePolicy,
        subclass,
        freelist = 1000
    )]
    #[derive(Debug, Clone)]
    pub struct WritePolicy {
        pub(crate) _as: aerospike_core::WritePolicy,
    }


    /// `WritePolicy` encapsulates parameters for all write operations.

    #[pymethods]
    impl WritePolicy {
        #[new]
        pub fn new() -> PyClassInitializer<Self> {
            let write_policy = WritePolicy {
                _as: aerospike_core::WritePolicy::default(),
            };
            let base_policy = BasePolicy::new();

            PyClassInitializer::from(base_policy).add_subclass(write_policy)
        }

        #[getter(record_exists_action)]
        pub fn get_record_exists_action(&self) -> RecordExistsAction {
            (&self._as.record_exists_action).into()
        }

        #[setter(record_exists_action)]
        pub fn set_record_exists_action(&mut self, record_exists_action: RecordExistsAction) {
            self._as.record_exists_action = match record_exists_action {
                RecordExistsAction::Update => aerospike_core::RecordExistsAction::Update,
                RecordExistsAction::UpdateOnly => aerospike_core::RecordExistsAction::UpdateOnly,
                RecordExistsAction::Replace => aerospike_core::RecordExistsAction::Replace,
                RecordExistsAction::ReplaceOnly => aerospike_core::RecordExistsAction::ReplaceOnly,
                RecordExistsAction::CreateOnly => aerospike_core::RecordExistsAction::CreateOnly,
            };
        }

        #[getter]
        pub fn get_generation_policy(&self) -> GenerationPolicy {
            (&self._as.generation_policy).into()
        }

        #[setter]
        pub fn set_generation_policy(&mut self, generation_policy: GenerationPolicy) {
            self._as.generation_policy = match generation_policy {
                GenerationPolicy::None => aerospike_core::GenerationPolicy::None,
                GenerationPolicy::ExpectGenEqual => {
                    aerospike_core::GenerationPolicy::ExpectGenEqual
                }
                GenerationPolicy::ExpectGenGreater => {
                    aerospike_core::GenerationPolicy::ExpectGenGreater
                }
            };
        }

        #[getter]
        pub fn get_commit_level(&self) -> CommitLevel {
            (&self._as.commit_level).into()
        }

        #[setter]
        pub fn set_commit_level(&mut self, commit_level: CommitLevel) {
            self._as.commit_level = (&commit_level).into();
        }

        #[getter]
        pub fn get_generation(&self) -> u32 {
            self._as.generation
        }

        #[setter]
        pub fn set_generation(&mut self, generation: u32) {
            self._as.generation = generation;
        }

        #[getter]
        pub fn get_expiration(&self) -> Expiration {
            (&self._as.expiration).into()
        }

        #[setter]
        pub fn set_expiration(&mut self, expiration: Expiration) {
            self._as.expiration = (&expiration).into();
        }

        #[getter]
        pub fn get_send_key(&self) -> bool {
            self._as.send_key
        }

        #[setter]
        pub fn set_send_key(&mut self, send_key: bool) {
            self._as.send_key = send_key;
        }

        #[getter]
        pub fn get_respond_per_each_op(&self) -> bool {
            self._as.respond_per_each_op
        }

        #[setter]
        pub fn set_respond_per_each_op(&mut self, respond_per_each_op: bool) {
            self._as.respond_per_each_op = respond_per_each_op;
        }

        #[getter]
        pub fn get_durable_delete(&self) -> bool {
            self._as.durable_delete
        }

        #[setter]
        pub fn set_durable_delete(&mut self, durable_delete: bool) {
            self._as.durable_delete = durable_delete;
        }

        #[getter]
        pub fn get_base_policy(&self) -> BasePolicy {
            BasePolicy {
                _as: self._as.base_policy.clone(),
            }
        }

        #[setter]
        pub fn set_base_policy(&mut self, base_policy: BasePolicy) {
            self._as.base_policy = base_policy._as;
        }

        // Override BasePolicy methods to sync with internal base_policy
        #[getter]
        pub fn get_total_timeout(&self) -> u64 {
            self._as.base_policy.total_timeout as u64
        }

        #[setter]
        pub fn set_total_timeout(&mut self, timeout_millis: u64) {
            self._as.base_policy.total_timeout = timeout_millis as u32;
        }

        #[getter]
        pub fn get_max_retries(&self) -> usize {
            self._as.base_policy.max_retries
        }

        #[setter]
        pub fn set_max_retries(&mut self, max_retries: usize) {
            self._as.base_policy.max_retries = max_retries;
        }

        #[getter]
        pub fn get_sleep_between_retries(&self) -> u64 {
            self._as.base_policy.sleep_between_retries as u64
        }

        #[setter]
        pub fn set_sleep_between_retries(&mut self, sleep_between_retries_millis: u64) {
            self._as.base_policy.sleep_between_retries =
                sleep_between_retries_millis.min(u32::MAX as u64) as u32;
        }

        #[getter]
        pub fn get_consistency_level(&self) -> ConsistencyLevel {
            (&self._as.base_policy.consistency_level).into()
        }

        #[setter]
        pub fn set_consistency_level(&mut self, consistency_level: ConsistencyLevel) {
            self._as.base_policy.consistency_level = match consistency_level {
                ConsistencyLevel::ConsistencyOne => {
                    aerospike_core::ConsistencyLevel::ConsistencyOne
                }
                ConsistencyLevel::ConsistencyAll => {
                    aerospike_core::ConsistencyLevel::ConsistencyAll
                }
            };
        }

        #[getter]
        pub fn get_socket_timeout(&self) -> u32 {
            self._as.base_policy.socket_timeout
        }

        #[setter]
        pub fn set_socket_timeout(&mut self, socket_timeout: u32) {
            self._as.base_policy.socket_timeout = socket_timeout;
        }

        #[getter]
        pub fn get_filter_expression(&self) -> Option<FilterExpression> {
            self._as.base_policy.filter_expression.as_ref().map(|fe| FilterExpression { _as: fe.clone() })
        }

        #[setter]
        pub fn set_filter_expression(&mut self, filter_expression: Option<FilterExpression>) {
            match filter_expression {
                Some(fe) => self._as.base_policy.filter_expression = Some(fe._as),
                None => self._as.base_policy.filter_expression = None,
            }
        }

        #[getter]
        pub fn get_read_touch_ttl(&self) -> i32 {
            match self._as.base_policy.read_touch_ttl {
                aerospike_core::ReadTouchTTL::Percent(pct) => pct as i32,
                aerospike_core::ReadTouchTTL::ServerDefault => 0,
                aerospike_core::ReadTouchTTL::DontReset => -1,
            }
        }

        #[setter]
        pub fn set_read_touch_ttl(&mut self, value: i32) -> PyResult<()> {
            self._as.base_policy.read_touch_ttl = match value {
                -1 => aerospike_core::ReadTouchTTL::DontReset,
                0 => aerospike_core::ReadTouchTTL::ServerDefault,
                pct if (1..=100).contains(&pct) => aerospike_core::ReadTouchTTL::Percent(pct as u8),
                _ => return Err(pyo3::exceptions::PyValueError::new_err(
                    format!("read_touch_ttl must be -1 (don't reset), 0 (server default), or 1-100 (percentage), got {value}")
                )),
            };
            Ok(())
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  QueryPolicy
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(
        name = "QueryPolicy",
        module = "_aerospike_async_native",
        extends = BasePolicy,
        subclass,
        freelist = 1000
    )]
    #[derive(Debug, Clone)]
    pub struct QueryPolicy {
        pub(crate) _as: aerospike_core::QueryPolicy,
    }

    /// `QueryPolicy` encapsulates parameters for query operations.
    #[pymethods]
    impl QueryPolicy {
        #[new]
        pub fn new() -> PyClassInitializer<Self> {
            let query_policy = QueryPolicy {
                _as: aerospike_core::QueryPolicy::default(),
            };
            let base_policy = BasePolicy::new();

            PyClassInitializer::from(base_policy).add_subclass(query_policy)
        }

        #[getter]
        pub fn get_base_policy(&self) -> BasePolicy {
            BasePolicy {
                _as: self._as.base_policy.clone(),
            }
        }

        #[setter]
        pub fn set_base_policy(&mut self, base_policy: BasePolicy) {
            self._as.base_policy = base_policy._as;
        }

        // Override BasePolicy methods to sync with internal base_policy
        #[getter]
        pub fn get_total_timeout(&self) -> u64 {
            self._as.base_policy.total_timeout as u64
        }

        #[setter]
        pub fn set_total_timeout(&mut self, timeout_millis: u64) {
            self._as.base_policy.total_timeout = timeout_millis as u32;
        }

        #[getter]
        pub fn get_max_retries(&self) -> usize {
            self._as.base_policy.max_retries
        }

        #[setter]
        pub fn set_max_retries(&mut self, max_retries: usize) {
            self._as.base_policy.max_retries = max_retries;
        }

        #[getter]
        pub fn get_sleep_between_retries(&self) -> u64 {
            self._as.base_policy.sleep_between_retries as u64
        }

        #[setter]
        pub fn set_sleep_between_retries(&mut self, sleep_between_retries_millis: u64) {
            self._as.base_policy.sleep_between_retries =
                sleep_between_retries_millis.min(u32::MAX as u64) as u32;
        }

        #[getter]
        pub fn get_consistency_level(&self) -> ConsistencyLevel {
            (&self._as.base_policy.consistency_level).into()
        }

        #[setter]
        pub fn set_consistency_level(&mut self, consistency_level: ConsistencyLevel) {
            self._as.base_policy.consistency_level = match consistency_level {
                ConsistencyLevel::ConsistencyOne => {
                    aerospike_core::ConsistencyLevel::ConsistencyOne
                }
                ConsistencyLevel::ConsistencyAll => {
                    aerospike_core::ConsistencyLevel::ConsistencyAll
                }
            };
        }

        #[getter]
        pub fn get_socket_timeout(&self) -> u32 {
            self._as.base_policy.socket_timeout
        }

        #[setter]
        pub fn set_socket_timeout(&mut self, socket_timeout: u32) {
            self._as.base_policy.socket_timeout = socket_timeout;
        }

        #[getter]
        pub fn get_filter_expression(&self) -> Option<FilterExpression> {
            self._as.base_policy.filter_expression.as_ref().map(|fe| FilterExpression { _as: fe.clone() })
        }

        #[setter]
        pub fn set_filter_expression(&mut self, filter_expression: Option<FilterExpression>) {
            match filter_expression {
                Some(fe) => self._as.base_policy.filter_expression = Some(fe._as),
                None => self._as.base_policy.filter_expression = None,
            }
        }

        #[getter]
        pub fn get_max_concurrent_nodes(&self) -> usize {
            self._as.max_concurrent_nodes
        }

        #[setter]
        pub fn set_max_concurrent_nodes(&mut self, max_concurrent_nodes: usize) {
            self._as.max_concurrent_nodes = max_concurrent_nodes;
        }

        #[getter]
        pub fn get_record_queue_size(&self) -> usize {
            self._as.record_queue_size
        }

        #[setter]
        pub fn set_record_queue_size(&mut self, record_queue_size: usize) {
            self._as.record_queue_size = record_queue_size;
        }

        #[getter]
        pub fn get_records_per_second(&self) -> u32 {
            self._as.records_per_second
        }

        #[setter]
        pub fn set_records_per_second(&mut self, records_per_second: u32) {
            self._as.records_per_second = records_per_second;
        }

        #[getter]
        pub fn get_max_records(&self) -> u64 {
            self._as.max_records
        }

        #[setter]
        pub fn set_max_records(&mut self, max_records: u64) {
            self._as.max_records = max_records;
        }

        #[getter]
        pub fn get_expected_duration(&self) -> QueryDuration {
            QueryDuration::from(self._as.expected_duration.clone())
        }

        #[setter]
        pub fn set_expected_duration(&mut self, expected_duration: QueryDuration) {
            self._as.expected_duration = aerospike_core::policy::QueryDuration::from(&expected_duration);
        }

        #[getter]
        pub fn get_replica(&self) -> Replica {
            (&self._as.replica).into()
        }

        #[setter]
        pub fn set_replica(&mut self, replica: Replica) {
            self._as.replica = (&replica).into();
        }

        // fail_on_cluster_change field doesn't exist in TLS branch
        // #[getter]
        // pub fn get_fail_on_cluster_change(&self) -> bool {
        //     self._as.fail_on_cluster_change
        // }

        // #[setter]
        // pub fn set_fail_on_cluster_change(&mut self, fail_on_cluster_change: bool) {
        //     self._as.fail_on_cluster_change = fail_on_cluster_change;
        // }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  BatchRecord
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(
        name = "BatchRecord",
        module = "_aerospike_async_native",
        subclass,
        freelist = 1000
    )]
    #[derive(Clone, Debug)]
    pub struct BatchRecord {
        pub(crate) _as: aerospike_core::BatchRecord,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl BatchRecord {
        // Note: BatchRecord is created internally by batch operations
        // Users should not create BatchRecord instances directly

        #[getter]
        pub fn get_key(&self) -> Key {
            Key {
                _as: self._as.key.clone(),
            }
        }

        /// Get the record from this batch result.
        ///
        /// **Performance Note:** This method clones the Record data on each call.
        /// The amount of data cloned depends on the record size (bins, metadata, etc.).
        /// For optimal performance when accessing the record multiple times, cache the result in Python:
        ///
        /// results = await client.batch_read(bp, brp, keys, None)
        /// for batch_record in results:
        ///     record = batch_record.record  # Clone once
        ///     if record:
        ///         # Use record multiple times - no additional cloning
        ///         bins = record.bins
        ///         key = record.key
        ///         generation = record.generation
        ///
        /// Returns:
        ///     Optional[Record]: The record if present, None otherwise.
        #[getter]
        pub fn get_record(&self) -> Option<Record> {
            self._as.record.as_ref().map(|r| Record { _as: r.clone() })
        }

        #[getter]
        pub fn get_result_code(&self) -> Option<ResultCode> {
            self._as.result_code.map(|rc| ResultCode(rc))
        }

        #[getter]
        pub fn get_in_doubt(&self) -> bool {
            self._as.in_doubt
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  BatchPolicy
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(
        name = "BatchPolicy",
        module = "_aerospike_async_native",
        extends = BasePolicy,
        subclass,
        freelist = 1000
    )]
    #[derive(Debug, Clone)]
    pub struct BatchPolicy {
        pub(crate) _as: aerospike_core::BatchPolicy,
    }

    #[pymethods]
    impl BatchPolicy {
        #[new]
        pub fn new() -> PyClassInitializer<Self> {
            let batch_policy = BatchPolicy {
                _as: aerospike_core::BatchPolicy::default(),
            };
            let base_policy = BasePolicy::new();

            PyClassInitializer::from(base_policy).add_subclass(batch_policy)
        }

        #[getter]
        pub fn get_base_policy(&self) -> BasePolicy {
            BasePolicy {
                _as: self._as.base_policy.clone(),
            }
        }

        #[setter]
        pub fn set_base_policy(&mut self, base_policy: BasePolicy) {
            self._as.base_policy = base_policy._as;
        }

        // Override BasePolicy methods to sync with internal base_policy
        #[getter]
        pub fn get_total_timeout(&self) -> u64 {
            self._as.base_policy.total_timeout as u64
        }

        #[setter]
        pub fn set_total_timeout(&mut self, timeout_millis: u64) {
            self._as.base_policy.total_timeout = timeout_millis as u32;
        }

        #[getter]
        pub fn get_max_retries(&self) -> usize {
            self._as.base_policy.max_retries
        }

        #[setter]
        pub fn set_max_retries(&mut self, max_retries: usize) {
            self._as.base_policy.max_retries = max_retries;
        }

        #[getter]
        pub fn get_sleep_between_retries(&self) -> u64 {
            self._as.base_policy.sleep_between_retries as u64
        }

        #[setter]
        pub fn set_sleep_between_retries(&mut self, sleep_between_retries_millis: u64) {
            self._as.base_policy.sleep_between_retries =
                sleep_between_retries_millis.min(u32::MAX as u64) as u32;
        }

        #[getter]
        pub fn get_consistency_level(&self) -> ConsistencyLevel {
            (&self._as.base_policy.consistency_level).into()
        }

        #[setter]
        pub fn set_consistency_level(&mut self, consistency_level: ConsistencyLevel) {
            self._as.base_policy.consistency_level = match consistency_level {
                ConsistencyLevel::ConsistencyOne => {
                    aerospike_core::ConsistencyLevel::ConsistencyOne
                }
                ConsistencyLevel::ConsistencyAll => {
                    aerospike_core::ConsistencyLevel::ConsistencyAll
                }
            };
        }

        #[getter]
        pub fn get_socket_timeout(&self) -> u32 {
            self._as.base_policy.socket_timeout
        }

        #[setter]
        pub fn set_socket_timeout(&mut self, socket_timeout: u32) {
            self._as.base_policy.socket_timeout = socket_timeout;
        }

        #[getter]
        pub fn get_allow_inline(&self) -> bool {
            self._as.allow_inline
        }

        #[setter]
        pub fn set_allow_inline(&mut self, allow_inline: bool) {
            self._as.allow_inline = allow_inline;
        }

        #[getter]
        pub fn get_allow_inline_ssd(&self) -> bool {
            self._as.allow_inline_ssd
        }

        #[setter]
        pub fn set_allow_inline_ssd(&mut self, allow_inline_ssd: bool) {
            self._as.allow_inline_ssd = allow_inline_ssd;
        }

        #[getter]
        pub fn get_respond_all_keys(&self) -> bool {
            self._as.respond_all_keys
        }

        #[setter]
        pub fn set_respond_all_keys(&mut self, respond_all_keys: bool) {
            self._as.respond_all_keys = respond_all_keys;
        }

        // Override filter expression to sync with internal base_policy
        #[getter]
        pub fn get_filter_expression(&self) -> Option<FilterExpression> {
            self._as.base_policy.filter_expression.as_ref().map(|fe| FilterExpression { _as: fe.clone() })
        }

        #[setter]
        pub fn set_filter_expression(&mut self, filter_expression: Option<FilterExpression>) {
            match filter_expression {
                Some(fe) => self._as.base_policy.filter_expression = Some(fe._as),
                None => self._as.base_policy.filter_expression = None,
            }
        }

        #[getter]
        pub fn get_replica(&self) -> Replica {
            (&self._as.replica).into()
        }

        #[setter]
        pub fn set_replica(&mut self, replica: Replica) {
            self._as.replica = (&replica).into();
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  BatchReadPolicy
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(
        name = "BatchReadPolicy",
        module = "_aerospike_async_native",
        subclass,
        freelist = 1000
    )]
    #[derive(Debug, Clone)]
    pub struct BatchReadPolicy {
        pub(crate) _as: aerospike_core::BatchReadPolicy,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl BatchReadPolicy {
        #[new]
        pub fn new() -> Self {
            BatchReadPolicy {
                _as: aerospike_core::BatchReadPolicy::default(),
            }
        }

        #[getter]
        pub fn get_filter_expression(&self) -> Option<FilterExpression> {
            self._as.filter_expression.as_ref().map(|fe| FilterExpression { _as: fe.clone() })
        }

        #[setter]
        pub fn set_filter_expression(&mut self, filter_expression: Option<FilterExpression>) {
            match filter_expression {
                Some(fe) => self._as.filter_expression = Some(fe._as),
                None => self._as.filter_expression = None,
            }
        }

        #[getter]
        pub fn get_read_touch_ttl(&self) -> i32 {
            match self._as.read_touch_ttl {
                aerospike_core::ReadTouchTTL::Percent(pct) => pct as i32,
                aerospike_core::ReadTouchTTL::ServerDefault => 0,
                aerospike_core::ReadTouchTTL::DontReset => -1,
            }
        }

        #[setter]
        pub fn set_read_touch_ttl(&mut self, value: i32) -> PyResult<()> {
            self._as.read_touch_ttl = match value {
                -1 => aerospike_core::ReadTouchTTL::DontReset,
                0 => aerospike_core::ReadTouchTTL::ServerDefault,
                pct if (1..=100).contains(&pct) => aerospike_core::ReadTouchTTL::Percent(pct as u8),
                _ => return Err(pyo3::exceptions::PyValueError::new_err(
                    format!("read_touch_ttl must be -1 (don't reset), 0 (server default), or 1-100 (percentage), got {value}")
                )),
            };
            Ok(())
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  BatchWritePolicy
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(
        name = "BatchWritePolicy",
        module = "_aerospike_async_native",
        subclass,
        freelist = 1000
    )]
    #[derive(Debug, Clone)]
    pub struct BatchWritePolicy {
        pub(crate) _as: aerospike_core::BatchWritePolicy,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl BatchWritePolicy {
        #[new]
        pub fn new() -> Self {
            BatchWritePolicy {
                _as: aerospike_core::BatchWritePolicy::default(),
            }
        }

        #[getter]
        pub fn get_filter_expression(&self) -> Option<FilterExpression> {
            self._as.filter_expression.as_ref().map(|fe| FilterExpression { _as: fe.clone() })
        }

        #[setter]
        pub fn set_filter_expression(&mut self, filter_expression: Option<FilterExpression>) {
            match filter_expression {
                Some(fe) => self._as.filter_expression = Some(fe._as),
                None => self._as.filter_expression = None,
            }
        }

        #[getter]
        pub fn get_send_key(&self) -> bool {
            self._as.send_key
        }

        #[setter]
        pub fn set_send_key(&mut self, send_key: bool) {
            self._as.send_key = send_key;
        }

        #[getter]
        pub fn get_durable_delete(&self) -> bool {
            self._as.durable_delete
        }

        #[setter]
        pub fn set_durable_delete(&mut self, durable_delete: bool) {
            self._as.durable_delete = durable_delete;
        }

        #[getter]
        pub fn get_generation(&self) -> u32 {
            self._as.generation
        }

        #[setter]
        pub fn set_generation(&mut self, generation: u32) {
            self._as.generation = generation;
        }

        #[getter]
        pub fn get_expiration(&self) -> Expiration {
            (&self._as.expiration).into()
        }

        #[setter]
        pub fn set_expiration(&mut self, expiration: Expiration) {
            self._as.expiration = (&expiration).into();
        }

        #[getter(record_exists_action)]
        pub fn get_record_exists_action(&self) -> RecordExistsAction {
            (&self._as.record_exists_action).into()
        }

        #[setter(record_exists_action)]
        pub fn set_record_exists_action(&mut self, record_exists_action: RecordExistsAction) {
            self._as.record_exists_action = (&record_exists_action).into();
        }

        #[getter(generation_policy)]
        pub fn get_generation_policy(&self) -> GenerationPolicy {
            (&self._as.generation_policy).into()
        }

        #[setter(generation_policy)]
        pub fn set_generation_policy(&mut self, generation_policy: GenerationPolicy) {
            self._as.generation_policy = (&generation_policy).into();
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  BatchDeletePolicy
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(
        name = "BatchDeletePolicy",
        module = "_aerospike_async_native",
        subclass,
        freelist = 1000
    )]
    #[derive(Debug, Clone)]
    pub struct BatchDeletePolicy {
        pub(crate) _as: aerospike_core::BatchDeletePolicy,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl BatchDeletePolicy {
        #[new]
        pub fn new() -> Self {
            BatchDeletePolicy {
                _as: aerospike_core::BatchDeletePolicy::default(),
            }
        }

        #[getter]
        pub fn get_filter_expression(&self) -> Option<FilterExpression> {
            self._as.filter_expression.as_ref().map(|fe| FilterExpression { _as: fe.clone() })
        }

        #[setter]
        pub fn set_filter_expression(&mut self, filter_expression: Option<FilterExpression>) {
            match filter_expression {
                Some(fe) => self._as.filter_expression = Some(fe._as),
                None => self._as.filter_expression = None,
            }
        }

        #[getter]
        pub fn get_send_key(&self) -> bool {
            self._as.send_key
        }

        #[setter]
        pub fn set_send_key(&mut self, send_key: bool) {
            self._as.send_key = send_key;
        }

        #[getter]
        pub fn get_durable_delete(&self) -> bool {
            self._as.durable_delete
        }

        #[setter]
        pub fn set_durable_delete(&mut self, durable_delete: bool) {
            self._as.durable_delete = durable_delete;
        }

        #[getter]
        pub fn get_generation(&self) -> u32 {
            self._as.generation
        }

        #[setter]
        pub fn set_generation(&mut self, generation: u32) {
            self._as.generation = generation;
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  BatchUDFPolicy
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(
        name = "BatchUDFPolicy",
        module = "_aerospike_async_native",
        subclass,
        freelist = 1000
    )]
    #[derive(Debug, Clone)]
    pub struct BatchUDFPolicy {
        pub(crate) _as: aerospike_core::BatchUDFPolicy,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl BatchUDFPolicy {
        #[new]
        pub fn new() -> Self {
            BatchUDFPolicy {
                _as: aerospike_core::BatchUDFPolicy::default(),
            }
        }

        #[getter]
        pub fn get_filter_expression(&self) -> Option<FilterExpression> {
            self._as.filter_expression.as_ref().map(|fe| FilterExpression { _as: fe.clone() })
        }

        #[setter]
        pub fn set_filter_expression(&mut self, filter_expression: Option<FilterExpression>) {
            match filter_expression {
                Some(fe) => self._as.filter_expression = Some(fe._as),
                None => self._as.filter_expression = None,
            }
        }

        #[getter]
        pub fn get_send_key(&self) -> bool {
            self._as.send_key
        }

        #[setter]
        pub fn set_send_key(&mut self, send_key: bool) {
            self._as.send_key = send_key;
        }

        #[getter]
        pub fn get_durable_delete(&self) -> bool {
            self._as.durable_delete
        }

        #[setter]
        pub fn set_durable_delete(&mut self, durable_delete: bool) {
            self._as.durable_delete = durable_delete;
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  BatchReadOp / BatchWriteOp / BatchDeleteOp  (mixed-batch input types)
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// A single read operation for use with :meth:`Client.batch`.
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(
        name = "BatchReadOp",
        module = "_aerospike_async_native",
        freelist = 1000
    )]
    #[derive(Debug, Clone)]
    pub struct BatchReadOp {
        pub(crate) key: aerospike_core::Key,
        pub(crate) policy: aerospike_core::BatchReadPolicy,
        pub(crate) bins: Option<Vec<String>>,
        pub(crate) ops: Vec<OpWithCtx>,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl BatchReadOp {
        #[new]
        #[pyo3(signature = (key, bins=None, operations=None, policy=None))]
        pub fn new(
            key: &Key,
            bins: Option<Vec<String>>,
            operations: Option<Vec<Py<PyAny>>>,
            policy: Option<&BatchReadPolicy>,
        ) -> PyResult<Self> {
            let ops = match operations {
                Some(ref py_ops) => extract_py_ops_with_ctx(py_ops)?,
                None => Vec::new(),
            };
            Ok(BatchReadOp {
                key: key._as.clone(),
                policy: policy.map(|p| p._as.clone()).unwrap_or_default(),
                bins,
                ops,
            })
        }
    }

    /// A single write operation for use with :meth:`Client.batch`.
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(
        name = "BatchWriteOp",
        module = "_aerospike_async_native",
        freelist = 1000
    )]
    #[derive(Debug, Clone)]
    pub struct BatchWriteOp {
        pub(crate) key: aerospike_core::Key,
        pub(crate) policy: aerospike_core::BatchWritePolicy,
        pub(crate) ops: Vec<OpWithCtx>,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl BatchWriteOp {
        #[new]
        #[pyo3(signature = (key, operations, policy=None))]
        pub fn new(
            key: &Key,
            operations: Vec<Py<PyAny>>,
            policy: Option<&BatchWritePolicy>,
        ) -> PyResult<Self> {
            let ops = extract_py_ops_with_ctx(&operations)?;
            Ok(BatchWriteOp {
                key: key._as.clone(),
                policy: policy.map(|p| p._as.clone()).unwrap_or_default(),
                ops,
            })
        }
    }

    /// A single delete operation for use with :meth:`Client.batch`.
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(
        name = "BatchDeleteOp",
        module = "_aerospike_async_native",
        freelist = 1000
    )]
    #[derive(Debug, Clone)]
    pub struct BatchDeleteOp {
        pub(crate) key: aerospike_core::Key,
        pub(crate) policy: aerospike_core::BatchDeletePolicy,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl BatchDeleteOp {
        #[new]
        #[pyo3(signature = (key, policy=None))]
        pub fn new(
            key: &Key,
            policy: Option<&BatchDeletePolicy>,
        ) -> PyResult<Self> {
            Ok(BatchDeleteOp {
                key: key._as.clone(),
                policy: policy.map(|p| p._as.clone()).unwrap_or_default(),
            })
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  AuthMode
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(
        name = "ClientPolicy",
        module = "_aerospike_async_native",
        subclass,
        freelist = 1000
    )]
    #[derive(Clone)]
    pub struct ClientPolicy {
        pub(crate) _as: aerospike_core::ClientPolicy,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl ClientPolicy {
        #[new]
        fn new() -> PyResult<Self> {
            let res = ClientPolicy {
                _as: aerospike_core::ClientPolicy::default(),
            };

            Ok(res)
        }

        #[getter]
        fn get_user(&self) -> Option<String> {
            match &self._as.auth_mode {
                aerospike_core::AuthMode::Internal(user, _) | aerospike_core::AuthMode::External(user, _) => {
                    Some(user.clone())
                }
                _ => None,
            }
        }

        #[setter]
        pub fn set_user(&mut self, user: Option<String>) {
            match (user, &self._as.auth_mode) {
                (Some(user), aerospike_core::AuthMode::Internal(_, password)) => {
                    self._as.auth_mode = aerospike_core::AuthMode::Internal(user, password.clone());
                }
                (Some(user), aerospike_core::AuthMode::External(_, password)) => {
                    self._as.auth_mode = aerospike_core::AuthMode::External(user, password.clone());
                }
                (Some(_user), aerospike_core::AuthMode::PKI) => {
                    // PKI mode doesn't use usernames, ignore
                }
                (Some(user), _) => {
                    self._as.auth_mode = aerospike_core::AuthMode::Internal(user, "".to_string());
                }
                (None, aerospike_core::AuthMode::Internal(_, _) | aerospike_core::AuthMode::External(_, _)) => {
                    self._as.auth_mode = aerospike_core::AuthMode::None;
                }
                (None, aerospike_core::AuthMode::PKI) => {
                    // PKI mode doesn't use usernames, ignore
                }
                _ => {}
            }
        }

        #[getter]
        pub fn get_password(&self) -> Option<String> {
            match &self._as.auth_mode {
                aerospike_core::AuthMode::Internal(_, password) | aerospike_core::AuthMode::External(_, password) => {
                    Some(password.clone())
                }
                _ => None,
            }
        }

        #[setter]
        pub fn set_password(&mut self, password: Option<String>) {
            match (password, &self._as.auth_mode) {
                (Some(password), aerospike_core::AuthMode::Internal(user, _)) => {
                    self._as.auth_mode = aerospike_core::AuthMode::Internal(user.clone(), password);
                }
                (Some(password), aerospike_core::AuthMode::External(user, _)) => {
                    self._as.auth_mode = aerospike_core::AuthMode::External(user.clone(), password);
                }
                (Some(password), aerospike_core::AuthMode::None) => {
                    self._as.auth_mode = aerospike_core::AuthMode::Internal("".to_string(), password);
                }
                (Some(_), aerospike_core::AuthMode::PKI) => {
                    // PKI mode doesn't use passwords, ignore
                }
                (None, aerospike_core::AuthMode::Internal(user, _)) => {
                    self._as.auth_mode = aerospike_core::AuthMode::Internal(user.clone(), "".to_string());
                }
                (None, aerospike_core::AuthMode::External(user, _)) => {
                    self._as.auth_mode = aerospike_core::AuthMode::External(user.clone(), "".to_string());
                }
                (None, aerospike_core::AuthMode::None) => {}
                (None, aerospike_core::AuthMode::PKI) => {}
            }
        }

        /// Get the current authentication mode.
        #[getter]
        pub fn get_auth_mode(&self) -> AuthMode {
            match &self._as.auth_mode {
                aerospike_core::AuthMode::None => AuthMode::None,
                aerospike_core::AuthMode::Internal(_, _) => AuthMode::Internal,
                aerospike_core::AuthMode::External(_, _) => AuthMode::External,
                aerospike_core::AuthMode::PKI => AuthMode::PKI,
            }
        }

        /// Set the authentication mode.
        ///
        /// Args:
        ///     mode: The authentication mode (AuthMode.NONE, AuthMode.INTERNAL, AuthMode.EXTERNAL, or AuthMode.PKI)
        ///     user: Optional username (required for INTERNAL and EXTERNAL modes)
        ///     password: Optional password (required for INTERNAL and EXTERNAL modes)
        ///
        /// Note: For PKI mode, user and password are ignored. TLS with client certificate is required.
        #[pyo3(signature = (mode, user = None, password = None))]
        pub fn set_auth_mode(&mut self, mode: AuthMode, user: Option<String>, password: Option<String>) -> PyResult<()> {
            match mode {
                AuthMode::None => {
                    self._as.auth_mode = aerospike_core::AuthMode::None;
                }
                AuthMode::Internal => {
                    let user = user.unwrap_or_else(|| "".to_string());
                    let password = password.unwrap_or_else(|| "".to_string());
                    self._as.auth_mode = aerospike_core::AuthMode::Internal(user, password);
                }
                AuthMode::External => {
                    let user = user.unwrap_or_else(|| "".to_string());
                    let password = password.unwrap_or_else(|| "".to_string());
                    self._as.auth_mode = aerospike_core::AuthMode::External(user, password);
                }
                AuthMode::PKI => {
                    self._as.auth_mode = aerospike_core::AuthMode::PKI;
                }
            }
            Ok(())
        }

        /// Set authentication mode to PKI (certificate-based authentication).
        ///
        /// This requires TLS to be configured with a client certificate.
        /// Requires server version 5.7.0+.
        pub fn set_pki_auth(&mut self) {
            self._as.auth_mode = aerospike_core::AuthMode::PKI;
        }

        #[getter]
        pub fn get_timeout(&self) -> u64 {
            self._as.timeout as u64
        }

        #[setter]
        pub fn set_timeout(&mut self, timeout_millis: u64) {
            self._as.timeout = timeout_millis as u32;
        }

        /// Connection idle timeout. Every time a connection is used, its idle
        /// deadline will be extended by this duration. When this deadline is reached,
        /// the connection will be closed and discarded from the connection pool.
        #[getter]
        pub fn get_idle_timeout(&self) -> u64 {
            self._as.idle_timeout as u64
        }

        #[setter]
        pub fn set_idle_timeout(&mut self, timeout_millis: u64) {
            self._as.idle_timeout = timeout_millis as u32;
        }

        #[getter]
        pub fn get_max_conns_per_node(&self) -> usize {
            self._as.max_conns_per_node
        }

        #[setter]
        pub fn set_max_conns_per_node(&mut self, sz: usize) {
            self._as.max_conns_per_node = sz;
        }

        /// Number of connection pools used for each node. Machines with 8 CPU cores or less usually
        /// need only one connection pool per node. Machines with larger number of CPU cores may have
        /// their performance limited by contention for pooled connections. Contention for pooled
        /// connections can be reduced by creating multiple mini connection pools per node.
        #[getter]
        pub fn get_conn_pools_per_node(&self) -> usize {
            self._as.conn_pools_per_node as usize
        }

        #[setter]
        pub fn set_conn_pools_per_node(&mut self, sz: usize) {
            self._as.conn_pools_per_node = sz as u8;
        }

        /// UseServicesAlternate determines if the client should use "services-alternate"
        /// instead of "services" in info request during cluster tending.
        /// "services-alternate" returns server configured external IP addresses that client
        /// uses to talk to nodes.  "services-alternate" can be used in place of
        /// providing a client "ipMap".
        /// This feature is recommended instead of using the client-side IpMap above.
        ///
        /// "services-alternate" is available with Aerospike Server versions >= 3.7.1.
        #[getter]
        pub fn get_use_services_alternate(&self) -> bool {
            self._as.use_services_alternate
        }

        #[setter]
        pub fn set_use_services_alternate(&mut self, value: bool) {
            self._as.use_services_alternate = value;
        }

        /// Mark this client as belonging to a rack, and track server rack data.  This field is useful when directing read commands to
        /// the server node that contains the key and exists on the same rack as the client.
        /// This serves to lower cloud provider costs when nodes are distributed across different
        /// racks/data centers.
        ///
        /// Replica.PreferRack and server rack configuration must
        /// also be set to enable this functionality.
        #[getter]
        pub fn get_rack_ids(&self) -> Option<Vec<usize>> {
            self._as.rack_ids.as_ref().map(|set| set.iter().cloned().collect())
        }

        #[setter]
        pub fn set_rack_ids(&mut self, value: Option<Vec<usize>>) {
            self._as.rack_ids = value.map(|v| v.into_iter().collect());
        }

        /// Size of the thread pool used in scan and query commands. These commands are often sent to
        /// multiple server nodes in parallel threads. A thread pool improves performance because
        /// threads do not have to be created/destroyed for each command.
        // thread_pool_size field doesn't exist in TLS branch
        // #[getter]
        // pub fn get_thread_pool_size(&self) -> usize {
        //     self._as.thread_pool_size
        // }

        // #[setter]
        // pub fn set_thread_pool_size(&mut self, value: usize) {
        //     self._as.thread_pool_size = value;
        // }

        /// Throw exception if host connection fails during addHost().
        #[getter]
        pub fn get_fail_if_not_connected(&self) -> bool {
            self._as.fail_if_not_connected
        }

        #[setter]
        pub fn set_fail_if_not_connected(&mut self, value: bool) {
            self._as.fail_if_not_connected = value;
        }

        /// Threshold at which the buffer attached to the connection will be shrunk by deallocating
        /// memory instead of just resetting the size of the underlying vec.
        /// Should be set to a value that covers as large a percentile of payload sizes as possible,
        /// while also being small enough not to occupy a significant amount of memory for the life
        /// of the connection pool.
        #[getter]
        pub fn get_buffer_reclaim_threshold(&self) -> usize {
            self._as.buffer_reclaim_threshold
        }

        #[setter]
        pub fn set_buffer_reclaim_threshold(&mut self, value: usize) {
            self._as.buffer_reclaim_threshold = value;
        }

        /// TendInterval determines interval for checking for cluster state changes.
        /// Minimum possible interval is 10 Milliseconds.
        #[getter]
        pub fn get_tend_interval(&self) -> u64 {
            self._as.tend_interval as u64
        }

        #[setter]
        pub fn set_tend_interval(&mut self, interval_millis: u64) {
            self._as.tend_interval = interval_millis.min(u32::MAX as u64) as u32;
        }

        /// A IP translation table is used in cases where different clients
        /// use different server IP addresses.  This may be necessary when
        /// using clients from both inside and outside a local area
        /// network. Default is no translation.
        /// The key is the IP address returned from friend info requests to other servers.
        /// The value is the real IP address used to connect to the server.
        #[getter]
        pub fn get_ip_map(&self, py: Python) -> PyResult<Py<PyAny>> {
            match &self._as.ip_map {
                Some(map) => {
                    let py_dict = PyDict::new(py);
                    for (k, v) in map {
                        py_dict.set_item(k, v)?;
                    }
                    Ok(py_dict.into())
                }
                None => Ok(py.None()),
            }
        }

        #[setter]
        pub fn set_ip_map(&mut self, value: Option<&Bound<'_, PyDict>>) -> PyResult<()> {
            match value {
                Some(dict) => {
                    let mut map = HashMap::new();
                    for (k, v) in dict.iter() {
                        let key: String = k.extract()?;
                        let val: String = v.extract()?;
                        map.insert(key, val);
                    }
                    self._as.ip_map = Some(map);
                }
                None => {
                    self._as.ip_map = None;
                }
            }
            Ok(())
        }

        /// Expected cluster name. It not `None`, server nodes must return this cluster name in order
        /// to join the client's view of the cluster. Should only be set when connecting to servers
        /// that support the "cluster-name" info command.
        #[getter]
        pub fn get_cluster_name(&self) -> Option<String> {
            self._as.cluster_name.clone()
        }

        #[setter]
        pub fn set_cluster_name(&mut self, value: Option<String>) {
            self._as.cluster_name = value;
        }

        /// TLS configuration for secure connections.
        /// Set to None to disable TLS, or use TlsConfig to configure TLS.
        #[cfg(feature = "tls")]
        #[getter]
        pub fn get_tls_config(&self) -> Option<TlsConfig> {
            self._as.tls_config.as_ref().map(|config| TlsConfig {
                _as: config.clone(),
            })
        }

        #[cfg(feature = "tls")]
        #[setter]
        pub fn set_tls_config(&mut self, value: Option<TlsConfig>) {
            self._as.tls_config = value.map(|tls| tls._as);
        }

        fn __str__(&self) -> PyResult<String> {
            Ok("".to_string())
        }

        fn __repr__(&self) -> PyResult<String> {
            let s = self.__str__()?;
            Ok(format!("ClientPolicy('{}')", s))
        }

        // pub fn __getstate__<'py>(&self, py: Python<'py>) -> PyResult<&'py PyBytes> {
        //     Ok(PyBytes::new(py, self.bytes()))
        // }

        // pub fn __setstate__(&mut self, py: Python, state: PyObject) -> PyResult<&'a PyAny> {
        //     let bytes_state = state.extract::<&PyBytes>(py)?;
        //     let uuid_builder = Builder::from_slice(bytes_state.as_bytes());

        //     match uuid_builder {
        //         Ok(builder) => {
        //             self.handle = builder.into_uuid();
        //             Ok(())
        //         }
        //         Err(_) => Err(PyErr::new::<PyValueError, &str>(
        //             "bytes is not a 16-char string",
        //         )),
        //     }
        // }

        pub fn __copy__(&self) -> Self {
            self.clone()
        }

        pub fn __deepcopy__(&self, _memo: &Bound<PyDict>) -> Self {
            // fast bitwise copy instead of python's pickling process
            self.clone()
        }
    }
