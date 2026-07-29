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
use crate::Txn;
#[cfg(feature = "tls")]
use crate::TlsConfig;

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  BasePolicy
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(from_py_object, 
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
            // PAC opts into positional Record.results by default (rust-core
            // leaves it off so direct Rust users pay nothing).
            let bp = aerospike_core::policy::BasePolicy {
                populate_positional_results: true,
                ..aerospike_core::policy::BasePolicy::default()
            };
            BasePolicy { _as: bp }
        }

        #[getter]
        pub fn get_read_mode_ap(&self) -> ReadModeAP {
            (&self._as.read_mode_ap).into()
        }

        #[setter]
        pub fn set_read_mode_ap(&mut self, mode: ReadModeAP) {
            self._as.read_mode_ap = (&mode).into();
        }

        #[getter]
        pub fn get_read_mode_sc(&self) -> ReadModeSC {
            (&self._as.read_mode_sc).into()
        }

        #[setter]
        pub fn set_read_mode_sc(&mut self, mode: ReadModeSC) {
            self._as.read_mode_sc = (&mode).into();
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
        pub fn get_use_compression(&self) -> bool {
            self._as.use_compression
        }

        #[setter]
        pub fn set_use_compression(&mut self, use_compression: bool) {
            self._as.use_compression = use_compression;
        }

        /// Minimum command-buffer size (bytes) at which compression actually
        /// fires. Buffers `<=` this value are sent uncompressed even when
        /// ``use_compression`` is ``True``. Default: ``128``.
        #[getter]
        pub fn get_compression_threshold(&self) -> usize {
            self._as.compression_threshold
        }

        #[setter]
        pub fn set_compression_threshold(&mut self, compression_threshold: usize) {
            self._as.compression_threshold = compression_threshold;
        }

        #[getter]
        pub fn get_txn(&self) -> Option<Txn> {
            self._as.txn.as_ref().map(|arc| Txn { _as: arc.clone() })
        }

        #[setter]
        pub fn set_txn(&mut self, txn: Option<Txn>) {
            self._as.txn = txn.map(|t| t._as);
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
                _ => return Err(crate::errors::ValueError::new_err(
                    format!("read_touch_ttl must be -1 (don't reset), 0 (server default), or 1-100 (percentage), got {value}")
                )),
            };
            Ok(())
        }

        /// Extended server-error detail requested per command: 0 none,
        /// 1 subcode, 2 +message, 3 +expression trace on expression build
        /// failures. Default: 0 (disabled). Requires server 8.1.3+; older
        /// servers ignore it.
        #[getter]
        pub fn get_error_detail_verbosity(&self) -> u8 {
            self._as.error_detail_verbosity
        }

        #[setter]
        pub fn set_error_detail_verbosity(&mut self, verbosity: u8) {
            self._as.error_detail_verbosity = verbosity;
        }
    }

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(from_py_object,
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
    #[pyclass(from_py_object, 
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
            // PAC opts into positional Record.results by default.
            let mut rp = aerospike_core::ReadPolicy::default();
            rp.base_policy.populate_positional_results = true;
            let read_policy = ReadPolicy { _as: rp };
            let base_policy = BasePolicy::new();

            PyClassInitializer::from(base_policy).add_subclass(read_policy)
        }

        /// Build a ``ReadPolicy`` in a single call, setting only the provided fields.
        ///
        /// Equivalent to constructing ``ReadPolicy()`` and assigning each attribute,
        /// but crosses the Rust boundary once instead of once per attribute.  All
        /// arguments are keyword-only; any unspecified field keeps its default.
        #[staticmethod]
        #[pyo3(signature = (*, total_timeout=None, socket_timeout=None, max_retries=None, sleep_between_retries=None, replica=None, read_mode_ap=None, read_mode_sc=None, read_touch_ttl=None, use_compression=None, compression_threshold=None, error_detail_verbosity=None))]
        pub fn from_fields(
            py: Python,
            total_timeout: Option<u64>,
            socket_timeout: Option<u32>,
            max_retries: Option<usize>,
            sleep_between_retries: Option<u64>,
            replica: Option<Replica>,
            read_mode_ap: Option<ReadModeAP>,
            read_mode_sc: Option<ReadModeSC>,
            read_touch_ttl: Option<i32>,
            use_compression: Option<bool>,
            compression_threshold: Option<usize>,
            error_detail_verbosity: Option<u8>,
        ) -> PyResult<Py<ReadPolicy>> {
            let mut rp = aerospike_core::ReadPolicy::default();
            rp.base_policy.populate_positional_results = true;
            if let Some(v) = total_timeout { rp.base_policy.total_timeout = v as u32; }
            if let Some(v) = socket_timeout { rp.base_policy.socket_timeout = v; }
            if let Some(v) = max_retries { rp.base_policy.max_retries = v; }
            if let Some(v) = sleep_between_retries {
                rp.base_policy.sleep_between_retries = v.min(u32::MAX as u64) as u32;
            }
            if let Some(v) = replica { rp.replica = (&v).into(); }
            if let Some(v) = read_mode_ap { rp.base_policy.read_mode_ap = (&v).into(); }
            if let Some(v) = read_mode_sc { rp.base_policy.read_mode_sc = (&v).into(); }
            if let Some(v) = read_touch_ttl {
                rp.base_policy.read_touch_ttl = match v {
                    -1 => aerospike_core::ReadTouchTTL::DontReset,
                    0 => aerospike_core::ReadTouchTTL::ServerDefault,
                    pct if (1..=100).contains(&pct) => {
                        aerospike_core::ReadTouchTTL::Percent(pct as u8)
                    }
                    _ => return Err(crate::errors::ValueError::new_err(format!(
                        "read_touch_ttl must be -1, 0, or 1-100, got {v}"
                    ))),
                };
            }
            if let Some(v) = use_compression { rp.base_policy.use_compression = v; }
            if let Some(v) = compression_threshold { rp.base_policy.compression_threshold = v; }
            if let Some(v) = error_detail_verbosity { rp.base_policy.error_detail_verbosity = v; }
            Py::new(
                py,
                PyClassInitializer::from(BasePolicy::new())
                    .add_subclass(ReadPolicy { _as: rp }),
            )
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
        pub fn get_error_detail_verbosity(&self) -> u8 {
            self._as.base_policy.error_detail_verbosity
        }

        #[setter]
        pub fn set_error_detail_verbosity(&mut self, verbosity: u8) {
            self._as.base_policy.error_detail_verbosity = verbosity;
        }

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
        pub fn get_read_mode_ap(&self) -> ReadModeAP {
            (&self._as.base_policy.read_mode_ap).into()
        }

        #[setter]
        pub fn set_read_mode_ap(&mut self, mode: ReadModeAP) {
            self._as.base_policy.read_mode_ap = (&mode).into();
        }

        #[getter]
        pub fn get_read_mode_sc(&self) -> ReadModeSC {
            (&self._as.base_policy.read_mode_sc).into()
        }

        #[setter]
        pub fn set_read_mode_sc(&mut self, mode: ReadModeSC) {
            self._as.base_policy.read_mode_sc = (&mode).into();
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
        pub fn get_use_compression(&self) -> bool {
            self._as.base_policy.use_compression
        }

        #[setter]
        pub fn set_use_compression(&mut self, use_compression: bool) {
            self._as.base_policy.use_compression = use_compression;
        }

        /// Minimum command-buffer size (bytes) at which compression actually
        /// fires. Buffers `<=` this value are sent uncompressed even when
        /// ``use_compression`` is ``True``. Default: ``128``.
        #[getter]
        pub fn get_compression_threshold(&self) -> usize {
            self._as.base_policy.compression_threshold
        }

        #[setter]
        pub fn set_compression_threshold(&mut self, compression_threshold: usize) {
            self._as.base_policy.compression_threshold = compression_threshold;
        }

        #[getter]
        pub fn get_txn(&self) -> Option<Txn> {
            self._as.base_policy.txn.as_ref().map(|arc| Txn { _as: arc.clone() })
        }

        #[setter]
        pub fn set_txn(&mut self, txn: Option<Txn>) {
            self._as.base_policy.txn = txn.map(|t| t._as);
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
                _ => return Err(crate::errors::ValueError::new_err(
                    format!("read_touch_ttl must be -1 (don't reset), 0 (server default), or 1-100 (percentage), got {value}")
                )),
            };
            Ok(())
        }
    }

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(from_py_object, 
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
            // PAC opts into positional Record.results by default.
            let mut wp = aerospike_core::WritePolicy::default();
            wp.base_policy.populate_positional_results = true;
            let write_policy = WritePolicy { _as: wp };
            let base_policy = BasePolicy::new();

            PyClassInitializer::from(base_policy).add_subclass(write_policy)
        }

        /// Build a ``WritePolicy`` in a single call, setting only the provided fields.
        ///
        /// Equivalent to constructing ``WritePolicy()`` and assigning each attribute,
        /// but crosses the Rust boundary once instead of once per attribute.  All
        /// arguments are keyword-only; any unspecified field keeps its default.
        #[staticmethod]
        #[pyo3(signature = (*, total_timeout=None, socket_timeout=None, max_retries=None, sleep_between_retries=None, record_exists_action=None, generation_policy=None, commit_level=None, generation=None, expiration=None, send_key=None, respond_per_each_op=None, durable_delete=None, use_compression=None, compression_threshold=None, error_detail_verbosity=None))]
        pub fn from_fields(
            py: Python,
            total_timeout: Option<u64>,
            socket_timeout: Option<u32>,
            max_retries: Option<usize>,
            sleep_between_retries: Option<u64>,
            record_exists_action: Option<RecordExistsAction>,
            generation_policy: Option<GenerationPolicy>,
            commit_level: Option<CommitLevel>,
            generation: Option<u32>,
            expiration: Option<Expiration>,
            send_key: Option<bool>,
            respond_per_each_op: Option<bool>,
            durable_delete: Option<bool>,
            use_compression: Option<bool>,
            compression_threshold: Option<usize>,
            error_detail_verbosity: Option<u8>,
        ) -> PyResult<Py<WritePolicy>> {
            let mut wp = aerospike_core::WritePolicy::default();
            wp.base_policy.populate_positional_results = true;
            if let Some(v) = total_timeout { wp.base_policy.total_timeout = v as u32; }
            if let Some(v) = socket_timeout { wp.base_policy.socket_timeout = v; }
            if let Some(v) = max_retries { wp.base_policy.max_retries = v; }
            if let Some(v) = sleep_between_retries {
                wp.base_policy.sleep_between_retries = v.min(u32::MAX as u64) as u32;
            }
            if let Some(v) = record_exists_action {
                wp.record_exists_action = match v {
                    RecordExistsAction::Update => aerospike_core::RecordExistsAction::Update,
                    RecordExistsAction::UpdateOnly => aerospike_core::RecordExistsAction::UpdateOnly,
                    RecordExistsAction::Replace => aerospike_core::RecordExistsAction::Replace,
                    RecordExistsAction::ReplaceOnly => aerospike_core::RecordExistsAction::ReplaceOnly,
                    RecordExistsAction::CreateOnly => aerospike_core::RecordExistsAction::CreateOnly,
                };
            }
            if let Some(v) = generation_policy {
                wp.generation_policy = match v {
                    GenerationPolicy::None => aerospike_core::GenerationPolicy::None,
                    GenerationPolicy::ExpectGenEqual => aerospike_core::GenerationPolicy::ExpectGenEqual,
                    GenerationPolicy::ExpectGenGreater => aerospike_core::GenerationPolicy::ExpectGenGreater,
                };
            }
            if let Some(v) = commit_level { wp.commit_level = (&v).into(); }
            if let Some(v) = generation { wp.generation = v; }
            if let Some(v) = expiration { wp.expiration = (&v).into(); }
            if let Some(v) = send_key { wp.send_key = v; }
            if let Some(v) = respond_per_each_op { wp.respond_per_each_op = v; }
            if let Some(v) = durable_delete { wp.durable_delete = v; }
            if let Some(v) = use_compression { wp.base_policy.use_compression = v; }
            if let Some(v) = compression_threshold { wp.base_policy.compression_threshold = v; }
            if let Some(v) = error_detail_verbosity { wp.base_policy.error_detail_verbosity = v; }
            Py::new(
                py,
                PyClassInitializer::from(BasePolicy::new())
                    .add_subclass(WritePolicy { _as: wp }),
            )
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
        pub fn get_error_detail_verbosity(&self) -> u8 {
            self._as.base_policy.error_detail_verbosity
        }

        #[setter]
        pub fn set_error_detail_verbosity(&mut self, verbosity: u8) {
            self._as.base_policy.error_detail_verbosity = verbosity;
        }

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
        pub fn get_read_mode_ap(&self) -> ReadModeAP {
            (&self._as.base_policy.read_mode_ap).into()
        }

        #[setter]
        pub fn set_read_mode_ap(&mut self, mode: ReadModeAP) {
            self._as.base_policy.read_mode_ap = (&mode).into();
        }

        #[getter]
        pub fn get_read_mode_sc(&self) -> ReadModeSC {
            (&self._as.base_policy.read_mode_sc).into()
        }

        #[setter]
        pub fn set_read_mode_sc(&mut self, mode: ReadModeSC) {
            self._as.base_policy.read_mode_sc = (&mode).into();
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
        pub fn get_use_compression(&self) -> bool {
            self._as.base_policy.use_compression
        }

        #[setter]
        pub fn set_use_compression(&mut self, use_compression: bool) {
            self._as.base_policy.use_compression = use_compression;
        }

        /// Minimum command-buffer size (bytes) at which compression actually
        /// fires. Buffers `<=` this value are sent uncompressed even when
        /// ``use_compression`` is ``True``. Default: ``128``.
        #[getter]
        pub fn get_compression_threshold(&self) -> usize {
            self._as.base_policy.compression_threshold
        }

        #[setter]
        pub fn set_compression_threshold(&mut self, compression_threshold: usize) {
            self._as.base_policy.compression_threshold = compression_threshold;
        }

        #[getter]
        pub fn get_txn(&self) -> Option<Txn> {
            self._as.base_policy.txn.as_ref().map(|arc| Txn { _as: arc.clone() })
        }

        #[setter]
        pub fn set_txn(&mut self, txn: Option<Txn>) {
            self._as.base_policy.txn = txn.map(|t| t._as);
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
                _ => return Err(crate::errors::ValueError::new_err(
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
    #[pyclass(from_py_object, 
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
            // PAC opts into positional Record.results by default.
            let mut qp = aerospike_core::QueryPolicy::default();
            qp.base_policy.populate_positional_results = true;
            let query_policy = QueryPolicy { _as: qp };
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
        pub fn get_error_detail_verbosity(&self) -> u8 {
            self._as.base_policy.error_detail_verbosity
        }

        #[setter]
        pub fn set_error_detail_verbosity(&mut self, verbosity: u8) {
            self._as.base_policy.error_detail_verbosity = verbosity;
        }

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
        pub fn get_read_mode_ap(&self) -> ReadModeAP {
            (&self._as.base_policy.read_mode_ap).into()
        }

        #[setter]
        pub fn set_read_mode_ap(&mut self, mode: ReadModeAP) {
            self._as.base_policy.read_mode_ap = (&mode).into();
        }

        #[getter]
        pub fn get_read_mode_sc(&self) -> ReadModeSC {
            (&self._as.base_policy.read_mode_sc).into()
        }

        #[setter]
        pub fn set_read_mode_sc(&mut self, mode: ReadModeSC) {
            self._as.base_policy.read_mode_sc = (&mode).into();
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
        pub fn get_use_compression(&self) -> bool {
            self._as.base_policy.use_compression
        }

        #[setter]
        pub fn set_use_compression(&mut self, use_compression: bool) {
            self._as.base_policy.use_compression = use_compression;
        }

        /// Minimum command-buffer size (bytes) at which compression actually
        /// fires. Buffers `<=` this value are sent uncompressed even when
        /// ``use_compression`` is ``True``. Default: ``128``.
        #[getter]
        pub fn get_compression_threshold(&self) -> usize {
            self._as.base_policy.compression_threshold
        }

        #[setter]
        pub fn set_compression_threshold(&mut self, compression_threshold: usize) {
            self._as.base_policy.compression_threshold = compression_threshold;
        }

        #[getter]
        pub fn get_txn(&self) -> Option<Txn> {
            self._as.base_policy.txn.as_ref().map(|arc| Txn { _as: arc.clone() })
        }

        #[setter]
        pub fn set_txn(&mut self, txn: Option<Txn>) {
            self._as.base_policy.txn = txn.map(|t| t._as);
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
        pub fn get_include_bin_data(&self) -> bool {
            self._as.include_bin_data
        }

        #[setter]
        pub fn set_include_bin_data(&mut self, include_bin_data: bool) {
            self._as.include_bin_data = include_bin_data;
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
    #[pyclass(from_py_object, 
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
            self._as.record.as_ref().map(|r| Record { _as: r.clone(), cached_bins: None, cached_results: None })
        }

        #[getter]
        pub fn get_result_code(&self) -> Option<ResultCode> {
            self._as.result_code.map(ResultCode)
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
    #[pyclass(from_py_object, 
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
            // PAC opts into positional Record.results by default.
            let mut bp = aerospike_core::BatchPolicy::default();
            bp.base_policy.populate_positional_results = true;
            let batch_policy = BatchPolicy { _as: bp };
            let base_policy = BasePolicy::new();

            PyClassInitializer::from(base_policy).add_subclass(batch_policy)
        }

        /// Build a ``BatchPolicy`` in a single call, setting only the provided fields.
        ///
        /// Equivalent to constructing ``BatchPolicy()`` and assigning each attribute,
        /// but crosses the Rust boundary once instead of once per attribute.  All
        /// arguments are keyword-only; any unspecified field keeps its default.
        #[staticmethod]
        #[pyo3(signature = (*, total_timeout=None, socket_timeout=None, max_retries=None, sleep_between_retries=None, allow_inline=None, allow_inline_ssd=None, respond_all_keys=None, replica=None, use_compression=None, compression_threshold=None, error_detail_verbosity=None))]
        pub fn from_fields(
            py: Python,
            total_timeout: Option<u64>,
            socket_timeout: Option<u32>,
            max_retries: Option<usize>,
            sleep_between_retries: Option<u64>,
            allow_inline: Option<bool>,
            allow_inline_ssd: Option<bool>,
            respond_all_keys: Option<bool>,
            replica: Option<Replica>,
            use_compression: Option<bool>,
            compression_threshold: Option<usize>,
            error_detail_verbosity: Option<u8>,
        ) -> PyResult<Py<BatchPolicy>> {
            let mut bp = aerospike_core::BatchPolicy::default();
            bp.base_policy.populate_positional_results = true;
            if let Some(v) = total_timeout { bp.base_policy.total_timeout = v as u32; }
            if let Some(v) = socket_timeout { bp.base_policy.socket_timeout = v; }
            if let Some(v) = max_retries { bp.base_policy.max_retries = v; }
            if let Some(v) = sleep_between_retries {
                bp.base_policy.sleep_between_retries = v.min(u32::MAX as u64) as u32;
            }
            if let Some(v) = allow_inline { bp.allow_inline = v; }
            if let Some(v) = allow_inline_ssd { bp.allow_inline_ssd = v; }
            if let Some(v) = respond_all_keys { bp.respond_all_keys = v; }
            if let Some(v) = replica { bp.replica = (&v).into(); }
            if let Some(v) = use_compression { bp.base_policy.use_compression = v; }
            if let Some(v) = compression_threshold { bp.base_policy.compression_threshold = v; }
            if let Some(v) = error_detail_verbosity { bp.base_policy.error_detail_verbosity = v; }
            Py::new(
                py,
                PyClassInitializer::from(BasePolicy::new())
                    .add_subclass(BatchPolicy { _as: bp }),
            )
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
        pub fn get_error_detail_verbosity(&self) -> u8 {
            self._as.base_policy.error_detail_verbosity
        }

        #[setter]
        pub fn set_error_detail_verbosity(&mut self, verbosity: u8) {
            self._as.base_policy.error_detail_verbosity = verbosity;
        }

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
        pub fn get_read_mode_ap(&self) -> ReadModeAP {
            (&self._as.base_policy.read_mode_ap).into()
        }

        #[setter]
        pub fn set_read_mode_ap(&mut self, mode: ReadModeAP) {
            self._as.base_policy.read_mode_ap = (&mode).into();
        }

        #[getter]
        pub fn get_read_mode_sc(&self) -> ReadModeSC {
            (&self._as.base_policy.read_mode_sc).into()
        }

        #[setter]
        pub fn set_read_mode_sc(&mut self, mode: ReadModeSC) {
            self._as.base_policy.read_mode_sc = (&mode).into();
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
        pub fn get_use_compression(&self) -> bool {
            self._as.base_policy.use_compression
        }

        #[setter]
        pub fn set_use_compression(&mut self, use_compression: bool) {
            self._as.base_policy.use_compression = use_compression;
        }

        /// Minimum command-buffer size (bytes) at which compression actually
        /// fires. Buffers `<=` this value are sent uncompressed even when
        /// ``use_compression`` is ``True``. Default: ``128``.
        #[getter]
        pub fn get_compression_threshold(&self) -> usize {
            self._as.base_policy.compression_threshold
        }

        #[setter]
        pub fn set_compression_threshold(&mut self, compression_threshold: usize) {
            self._as.base_policy.compression_threshold = compression_threshold;
        }

        #[getter]
        pub fn get_txn(&self) -> Option<Txn> {
            self._as.base_policy.txn.as_ref().map(|arc| Txn { _as: arc.clone() })
        }

        #[setter]
        pub fn set_txn(&mut self, txn: Option<Txn>) {
            self._as.base_policy.txn = txn.map(|t| t._as);
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
    #[pyclass(from_py_object, 
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
                _ => return Err(crate::errors::ValueError::new_err(
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
    #[pyclass(from_py_object, 
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
    #[pyclass(from_py_object, 
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
    #[pyclass(from_py_object, 
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
    #[pyclass(from_py_object, 
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
            py: Python<'_>,
            key: &Key,
            bins: Option<Vec<String>>,
            operations: Option<Vec<Py<PyAny>>>,
            policy: Option<&BatchReadPolicy>,
        ) -> PyResult<Self> {
            let ops = match operations {
                Some(ref py_ops) => extract_py_ops_with_ctx(py, py_ops)?,
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
    #[pyclass(from_py_object, 
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
            py: Python<'_>,
            key: &Key,
            operations: Vec<Py<PyAny>>,
            policy: Option<&BatchWritePolicy>,
        ) -> PyResult<Self> {
            let ops = extract_py_ops_with_ctx(py, &operations)?;
            Ok(BatchWriteOp {
                key: key._as.clone(),
                policy: policy.map(|p| p._as.clone()).unwrap_or_default(),
                ops,
            })
        }
    }

    /// A single delete operation for use with :meth:`Client.batch`.
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(from_py_object, 
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
    #[pyclass(from_py_object, 
        name = "ClientPolicy",
        module = "_aerospike_async_native",
        subclass,
        freelist = 1000
    )]
    #[derive(Clone)]
    pub struct ClientPolicy {
        pub(crate) _as: aerospike_core::ClientPolicy,
        /// PAC-specific (not in aerospike_core): when set, every async op
        /// on this Client runs on a dedicated Tokio runtime with this many
        /// worker threads instead of the shared global runtime. Eliminates
        /// cross-loop scheduler contention under AsyncPool. `None` or
        /// `Some(0)` = use the global runtime (default).
        pub(crate) per_client_runtime_workers: Option<usize>,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl ClientPolicy {
        #[new]
        fn new() -> PyResult<Self> {
            // Tuned for the primary async use case: a single Tokio runtime
            // (or per-Client runtime in AsyncPool) serializes pool access
            // through one or two workers, so contention is naturally low
            // even at high task concurrency. Sync wrappers that drive PAC
            // from many caller threads (e.g. PSDK's SyncClient) should
            // override this on the policy before construction; 8 is a good
            // value for ~32-thread sync workloads.
            let res = ClientPolicy {
                _as: aerospike_core::ClientPolicy {
                    conn_pools_per_node: 4,
                    // Identify this client on the wire (user-agent) so bare-PAC
                    // usage is distinguishable from the bare Rust core. A higher
                    // wrapper layer overrides this with its own identifier.
                    custom_client_id: Some(format!("python-async-{}", env!("CARGO_PKG_VERSION"))),
                    ..Default::default()
                },
                per_client_runtime_workers: None,
            };

            Ok(res)
        }

        /// Get the per-Client Tokio runtime worker count, if set.
        ///
        /// See :attr:`set_per_client_runtime_workers` for semantics.
        #[getter]
        pub fn get_per_client_runtime_workers(&self) -> Option<usize> {
            self.per_client_runtime_workers
        }

        /// Per-Client Tokio runtime (opt-in). With ``Some(N)`` where
        /// ``N >= 1``, this Client gets a dedicated multi-thread runtime
        /// with ``N`` workers instead of the shared global one.
        /// ``None`` or ``Some(0)`` keeps the global runtime (default).
        ///
        /// Use this when multiple Clients on multiple async event loops
        /// coexist in one process: the global runtime's work-stealing
        /// collapses past ~4 concurrent loops on an 8-CPU host, while a
        /// dedicated per-Client runtime isolates the scheduling.
        ///
        /// Sizing: pick ``workers >= concurrency / 8`` for reasonable
        /// tail latency. Under-provisioning starves concurrent ops
        /// (32 in-flight tasks on 2 workers showed ~1000ms p99.9).
        #[setter]
        pub fn set_per_client_runtime_workers(&mut self, workers: Option<usize>) {
            self.per_client_runtime_workers = workers;
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
                    let user = user.unwrap_or_default();
                    let password = password.unwrap_or_default();
                    self._as.auth_mode = aerospike_core::AuthMode::Internal(user, password);
                }
                AuthMode::External => {
                    let user = user.unwrap_or_default();
                    let password = password.unwrap_or_default();
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

        /// Initial connection timeout in milliseconds for opening (and, when
        /// security is enabled, authenticating) a socket to a node. Applied
        /// per connection attempt during cluster tend and on-demand pool
        /// growth. ``0`` (the default) falls back to :attr:`timeout`.
        #[getter]
        pub fn get_connect_timeout(&self) -> u64 {
            self._as.connect_timeout as u64
        }

        #[setter]
        pub fn set_connect_timeout(&mut self, timeout_millis: u64) {
            self._as.connect_timeout = timeout_millis as u32;
        }

        /// Login timeout in milliseconds for the authentication handshake
        /// when security is enabled. ``0`` falls back to
        /// :attr:`connect_timeout`. Defaults to 5000 ms.
        #[getter]
        pub fn get_login_timeout(&self) -> u64 {
            self._as.login_timeout as u64
        }

        #[setter]
        pub fn set_login_timeout(&mut self, timeout_millis: u64) {
            self._as.login_timeout = timeout_millis as u32;
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

        /// Minimum number of connections allowed per server node. The client
        /// periodically allocates new connections if the total count (idle +
        /// in-flight) falls below this value.
        ///
        /// Server ``proto-fd-idle-ms`` may also need to be increased
        /// substantially if min connections are defined. The default directs
        /// the server to close connections idle for 60 seconds which can
        /// defeat the purpose of keeping connections in reserve.
        ///
        /// Default: ``0`` (disabled).
        #[getter]
        pub fn get_min_conns_per_node(&self) -> usize {
            self._as.min_conns_per_node
        }

        #[setter]
        pub fn set_min_conns_per_node(&mut self, sz: usize) {
            self._as.min_conns_per_node = sz;
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

        /// Cluster-wide cap on the number of connections that may be in the
        /// middle of being opened (TCP connect + TLS + login) at the same
        /// time. When a command finds its node's pool empty, the connection
        /// is opened by a background task while the command retries; this
        /// threshold bounds how many such opens can run concurrently across
        /// all nodes, protecting the cluster from a thundering herd after a
        /// cold start or mass disconnect. ``0`` (the default) means
        /// unlimited.
        #[getter]
        pub fn get_opening_connection_threshold(&self) -> usize {
            self._as.opening_connection_threshold
        }

        #[setter]
        pub fn set_opening_connection_threshold(&mut self, threshold: usize) {
            self._as.opening_connection_threshold = threshold;
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
            // Core now stores rack_ids as an ordered Vec (preference order),
            // matching the Python-facing list — a direct clone suffices.
            self._as.rack_ids.clone()
        }

        #[setter]
        pub fn set_rack_ids(&mut self, value: Option<Vec<usize>>) {
            self._as.rack_ids = value;
        }

        // Size of the thread pool used in scan and query commands. These commands are often sent to
        // multiple server nodes in parallel threads. A thread pool improves performance because
        // threads do not have to be created/destroyed for each command.
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

        /// **Testing-only.** When ``True``, the client never performs peer
        /// discovery or background tend: every op routes through the seed
        /// addresses given at construction. No tend task is spawned. Useful
        /// for benchmarking and unit-test setup where eliminating tend
        /// noise matters more than topology safety.
        ///
        /// **Do not enable in production.** With ``seed_only_cluster`` set,
        /// node restarts, master failover, and rebalances are invisible to
        /// the client — ops to a stale or down node will fail or hang.
        /// Default: ``False``.
        #[getter]
        pub fn get_seed_only_cluster(&self) -> bool {
            self._as.seed_only_cluster
        }

        #[setter]
        pub fn set_seed_only_cluster(&mut self, value: bool) {
            self._as.seed_only_cluster = value;
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

        /// Maximum number of errors (network errors plus server-side ``TIMEOUT``,
        /// ``DEVICE_OVERLOAD``, ``KEY_BUSY``) tolerated against a single node within
        /// one ``error_rate_window``. Once exceeded, the client trips a per-node
        /// circuit breaker and rejects further commands targeted at that node with
        /// a ``MaxErrorRate`` exception until the next window resets. Set to ``0``
        /// to disable. Default: ``100``.
        #[getter]
        pub fn get_max_error_rate(&self) -> usize {
            self._as.max_error_rate
        }

        #[setter]
        pub fn set_max_error_rate(&mut self, value: usize) {
            self._as.max_error_rate = value;
        }

        /// Number of cluster tend iterations after which each node's error counter
        /// is reset. Smaller values make the circuit breaker more aggressive,
        /// larger values more lenient. Default: ``1``.
        #[getter]
        pub fn get_error_rate_window(&self) -> usize {
            self._as.error_rate_window
        }

        #[setter]
        pub fn set_error_rate_window(&mut self, value: usize) {
            self._as.error_rate_window = value;
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

        /// Identifies the application so that client operations can be
        /// correlated with server-side metrics. Default: ``None``.
        #[getter]
        pub fn get_application_id(&self) -> Option<String> {
            self._as.application_id.clone()
        }

        #[setter]
        pub fn set_application_id(&mut self, value: Option<String>) {
            self._as.application_id = value;
        }

        /// Override the ``client_id`` portion of the ``user_agent_id``
        /// payload sent to each node on connection validation. Intended for
        /// wrapper clients that embed the Rust core; end-user code should
        /// leave this as ``None``. Default: ``None``.
        #[getter]
        pub fn get_custom_client_id(&self) -> Option<String> {
            self._as.custom_client_id.clone()
        }

        #[setter]
        pub fn set_custom_client_id(&mut self, value: Option<String>) {
            self._as.custom_client_id = value;
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
