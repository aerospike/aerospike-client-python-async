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

use pyo3::basic::CompareOp;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyclass_enum, gen_stub_pymethods};
use aerospike_core::ResultCode as CoreResultCode;

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  QueryDuration
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Expected query duration. The server treats the query in different ways depending on the expected duration.
    /// This enum is ignored for aggregation queries, background queries and server versions < 6.0.
    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum QueryDuration {
        /// Long specifies that the query is expected to return more than 100 records per node.
        #[pyo3(name = "LONG")]
        Long = 0,
        /// Short specifies that the query is expected to return less than 100 records per node.
        #[pyo3(name = "SHORT")]
        Short = 1,
        /// LongRelaxAP will treat query as a Long query, but relax read consistency for AP namespaces.
        #[pyo3(name = "LONG_RELAX_AP")]
        LongRelaxAP = 2,
    }

    impl From<&QueryDuration> for aerospike_core::policy::QueryDuration {
        fn from(input: &QueryDuration) -> Self {
            match input {
                QueryDuration::Long => aerospike_core::policy::QueryDuration::Long,
                QueryDuration::Short => aerospike_core::policy::QueryDuration::Short,
                QueryDuration::LongRelaxAP => aerospike_core::policy::QueryDuration::LongRelaxAP,
            }
        }
    }

    impl From<aerospike_core::policy::QueryDuration> for QueryDuration {
        fn from(input: aerospike_core::policy::QueryDuration) -> Self {
            match input {
                aerospike_core::policy::QueryDuration::Long => QueryDuration::Long,
                aerospike_core::policy::QueryDuration::Short => QueryDuration::Short,
                aerospike_core::policy::QueryDuration::LongRelaxAP => QueryDuration::LongRelaxAP,
            }
        }
    }

    #[pymethods]
    impl QueryDuration {
        fn __richcmp__(&self, other: &QueryDuration, op: pyo3::class::basic::CompareOp) -> pyo3::PyResult<bool> {
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

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  Replica
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

/// Priority of operations on database server.
#[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
#[pyclass(module = "_aerospike_async_native")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Replica {
    #[pyo3(name = "MASTER")]
    Master,
    #[pyo3(name = "SEQUENCE")]
    Sequence,
    #[pyo3(name = "PREFER_RACK")]
    PreferRack,
}

    impl From<&Replica> for aerospike_core::policy::Replica {
        fn from(input: &Replica) -> Self {
            match &input {
                Replica::Master => aerospike_core::policy::Replica::Master,
                Replica::Sequence => aerospike_core::policy::Replica::Sequence,
                Replica::PreferRack => aerospike_core::policy::Replica::PreferRack,
            }
        }
    }

    impl From<&aerospike_core::policy::Replica> for Replica {
        fn from(input: &aerospike_core::policy::Replica) -> Self {
            match input {
                aerospike_core::policy::Replica::Master => Replica::Master,
                aerospike_core::policy::Replica::Sequence => Replica::Sequence,
                aerospike_core::policy::Replica::PreferRack => Replica::PreferRack,
            }
        }
    }

    #[pymethods]
    impl Replica {
        fn __richcmp__(&self, other: &Replica, op: pyo3::class::basic::CompareOp) -> pyo3::PyResult<bool> {
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

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  ConsistencyLevel
    //
    ////////////////////////////////////////////////////////////////////////////////////////////
    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum ConsistencyLevel {
        #[pyo3(name = "CONSISTENCY_ONE")]
        ConsistencyOne,
        #[pyo3(name = "CONSISTENCY_ALL")]
        ConsistencyAll,
    }

    #[pymethods]
    impl ConsistencyLevel {
        fn __richcmp__(&self, other: &ConsistencyLevel, op: pyo3::class::basic::CompareOp) -> pyo3::PyResult<bool> {
            match op {
                pyo3::class::basic::CompareOp::Eq => Ok(self == other),
                pyo3::class::basic::CompareOp::Ne => Ok(self != other),
                _ => Ok(false),
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

    impl From<&ConsistencyLevel> for aerospike_core::ConsistencyLevel {
        fn from(input: &ConsistencyLevel) -> Self {
            match &input {
                ConsistencyLevel::ConsistencyOne => {
                    aerospike_core::policy::ConsistencyLevel::ConsistencyOne
                }
                ConsistencyLevel::ConsistencyAll => {
                    aerospike_core::policy::ConsistencyLevel::ConsistencyAll
                }
            }
        }
    }

    impl From<&aerospike_core::ConsistencyLevel> for ConsistencyLevel {
        fn from(input: &aerospike_core::ConsistencyLevel) -> Self {
            match input {
                aerospike_core::policy::ConsistencyLevel::ConsistencyOne => {
                    ConsistencyLevel::ConsistencyOne
                }
                aerospike_core::policy::ConsistencyLevel::ConsistencyAll => {
                    ConsistencyLevel::ConsistencyAll
                }
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  RecordExistsAction
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// `RecordExistsAction` determines how to handle record writes based on record generation.
    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(module = "_aerospike_async_native")]
    #[derive(Debug, PartialEq, Eq, Hash, Clone)]
    pub enum RecordExistsAction {
        #[pyo3(name = "UPDATE")]
        Update,
        #[pyo3(name = "UPDATE_ONLY")]
        UpdateOnly,
        #[pyo3(name = "REPLACE")]
        Replace,
        #[pyo3(name = "REPLACE_ONLY")]
        ReplaceOnly,
        #[pyo3(name = "CREATE_ONLY")]
        CreateOnly,
    }

    #[pymethods]
    impl RecordExistsAction {
        fn __richcmp__(&self, other: &RecordExistsAction, op: pyo3::class::basic::CompareOp) -> pyo3::PyResult<bool> {
            match op {
                pyo3::class::basic::CompareOp::Eq => Ok(self == other),
                pyo3::class::basic::CompareOp::Ne => Ok(self != other),
                _ => Ok(false),
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

    impl From<&RecordExistsAction> for aerospike_core::policy::RecordExistsAction {
        fn from(input: &RecordExistsAction) -> Self {
            match &input {
                RecordExistsAction::Update => aerospike_core::policy::RecordExistsAction::Update,
                RecordExistsAction::UpdateOnly => {
                    aerospike_core::policy::RecordExistsAction::UpdateOnly
                }
                RecordExistsAction::Replace => aerospike_core::policy::RecordExistsAction::Replace,
                RecordExistsAction::ReplaceOnly => {
                    aerospike_core::policy::RecordExistsAction::ReplaceOnly
                }
                RecordExistsAction::CreateOnly => {
                    aerospike_core::policy::RecordExistsAction::CreateOnly
                }
            }
        }
    }

    impl From<&aerospike_core::policy::RecordExistsAction> for RecordExistsAction {
        fn from(input: &aerospike_core::policy::RecordExistsAction) -> Self {
            match input {
                aerospike_core::policy::RecordExistsAction::Update => RecordExistsAction::Update,
                aerospike_core::policy::RecordExistsAction::UpdateOnly => {
                    RecordExistsAction::UpdateOnly
                }
                aerospike_core::policy::RecordExistsAction::Replace => RecordExistsAction::Replace,
                aerospike_core::policy::RecordExistsAction::ReplaceOnly => {
                    RecordExistsAction::ReplaceOnly
                }
                aerospike_core::policy::RecordExistsAction::CreateOnly => {
                    RecordExistsAction::CreateOnly
                }
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  GenerationPolicy
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(module = "_aerospike_async_native")]
    #[derive(Debug, PartialEq, Eq, Hash, Clone)]
    pub enum GenerationPolicy {
        #[pyo3(name = "NONE")]
        None,
        #[pyo3(name = "EXPECT_GEN_EQUAL")]
        ExpectGenEqual,
        #[pyo3(name = "EXPECT_GEN_GREATER")]
        ExpectGenGreater,
    }

    #[pymethods]
    impl GenerationPolicy {
        fn __richcmp__(&self, other: &GenerationPolicy, op: pyo3::class::basic::CompareOp) -> pyo3::PyResult<bool> {
            match op {
                pyo3::class::basic::CompareOp::Eq => Ok(self == other),
                pyo3::class::basic::CompareOp::Ne => Ok(self != other),
                _ => Ok(false),
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

    impl From<&GenerationPolicy> for aerospike_core::policy::GenerationPolicy {
        fn from(input: &GenerationPolicy) -> Self {
            match &input {
                GenerationPolicy::None => aerospike_core::policy::GenerationPolicy::None,
                GenerationPolicy::ExpectGenEqual => {
                    aerospike_core::policy::GenerationPolicy::ExpectGenEqual
                }
                GenerationPolicy::ExpectGenGreater => {
                    aerospike_core::policy::GenerationPolicy::ExpectGenGreater
                }
            }
        }
    }

    impl From<&aerospike_core::policy::GenerationPolicy> for GenerationPolicy {
        fn from(input: &aerospike_core::policy::GenerationPolicy) -> Self {
            match input {
                aerospike_core::policy::GenerationPolicy::None => GenerationPolicy::None,
                aerospike_core::policy::GenerationPolicy::ExpectGenEqual => {
                    GenerationPolicy::ExpectGenEqual
                }
                aerospike_core::policy::GenerationPolicy::ExpectGenGreater => {
                    GenerationPolicy::ExpectGenGreater
                }
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  CommitLevel
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum CommitLevel {
        #[pyo3(name = "COMMIT_ALL")]
        CommitAll,
        #[pyo3(name = "COMMIT_MASTER")]
        CommitMaster,
    }

    #[pymethods]
    impl CommitLevel {
        fn __richcmp__(&self, other: &CommitLevel, op: pyo3::class::basic::CompareOp) -> pyo3::PyResult<bool> {
            match op {
                pyo3::class::basic::CompareOp::Eq => Ok(self == other),
                pyo3::class::basic::CompareOp::Ne => Ok(self != other),
                _ => Ok(false),
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

    impl From<&CommitLevel> for aerospike_core::policy::CommitLevel {
        fn from(input: &CommitLevel) -> Self {
            match &input {
                CommitLevel::CommitAll => aerospike_core::policy::CommitLevel::CommitAll,
                CommitLevel::CommitMaster => aerospike_core::policy::CommitLevel::CommitMaster,
            }
        }
    }

    impl From<&aerospike_core::policy::CommitLevel> for CommitLevel {
        fn from(input: &aerospike_core::policy::CommitLevel) -> Self {
            match input {
                aerospike_core::policy::CommitLevel::CommitAll => CommitLevel::CommitAll,
                aerospike_core::policy::CommitLevel::CommitMaster => CommitLevel::CommitMaster,
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  Expiration
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(
        name = "Expiration",
        module = "_aerospike_async_native",
        subclass,
        freelist = 1000
    )]
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct Expiration {
        v: _Expiration,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl Expiration {
        #[classattr]
        const NAMESPACE_DEFAULT: Expiration = Expiration {
            v: _Expiration::NamespaceDefault,
        };

        #[classattr]
        const NEVER_EXPIRE: Expiration = Expiration {
            v: _Expiration::Never,
        };

        #[classattr]
        const DONT_UPDATE: Expiration = Expiration {
            v: _Expiration::DontUpdate,
        };

        #[staticmethod]
        pub fn seconds(s: u32) -> Expiration {
            Expiration {
                v: _Expiration::Seconds(s),
            }
        }

        fn __richcmp__(&self, other: &Expiration, op: pyo3::class::basic::CompareOp) -> pyo3::PyResult<bool> {
            match op {
                pyo3::class::basic::CompareOp::Eq => Ok(self == other),
                pyo3::class::basic::CompareOp::Ne => Ok(self != other),
                _ => Ok(false),
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

    impl From<&Expiration> for aerospike_core::Expiration {
        fn from(input: &Expiration) -> Self {
            match input.v {
                _Expiration::Seconds(s) => aerospike_core::Expiration::Seconds(s),
                _Expiration::NamespaceDefault => aerospike_core::Expiration::NamespaceDefault,
                _Expiration::Never => aerospike_core::Expiration::Never,
                _Expiration::DontUpdate => aerospike_core::Expiration::DontUpdate,
            }
        }
    }

    impl From<&aerospike_core::Expiration> for Expiration {
        fn from(input: &aerospike_core::Expiration) -> Self {
            match input {
                aerospike_core::Expiration::Seconds(s) => Expiration {
                    v: _Expiration::Seconds(*s),
                },
                aerospike_core::Expiration::NamespaceDefault => Expiration {
                    v: _Expiration::NamespaceDefault,
                },
                aerospike_core::Expiration::Never => Expiration {
                    v: _Expiration::Never,
                },
                aerospike_core::Expiration::DontUpdate => Expiration {
                    v: _Expiration::DontUpdate,
                },
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum _Expiration {
        Seconds(u32),
        NamespaceDefault,
        Never,
        DontUpdate,
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  IndexType
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Underlying data type of secondary index.
    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy)]
    pub enum IndexType {
        #[pyo3(name = "NUMERIC")]
        Numeric,
        #[pyo3(name = "STRING")]
        String,
        #[pyo3(name = "GEO2D_SPHERE")]
        Geo2DSphere,
    }


    impl From<&IndexType> for aerospike_core::query::IndexType {
        fn from(input: &IndexType) -> Self {
            match &input {
                IndexType::Numeric => aerospike_core::query::IndexType::Numeric,
                IndexType::String => aerospike_core::query::IndexType::String,
                IndexType::Geo2DSphere => aerospike_core::query::IndexType::Geo2DSphere,
            }
        }
    }

    #[pymethods]
    impl IndexType {
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  CollectionIndexType
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Secondary index collection type.
    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy)]
    pub enum CollectionIndexType {
        #[pyo3(name = "DEFAULT")]
        Default,
        #[pyo3(name = "LIST")]
        List,
        #[pyo3(name = "MAP_KEYS")]
        MapKeys,
        #[pyo3(name = "MAP_VALUES")]
        MapValues,
    }


    impl From<&CollectionIndexType> for aerospike_core::query::CollectionIndexType {
        fn from(input: &CollectionIndexType) -> Self {
            match &input {
                CollectionIndexType::Default => aerospike_core::query::CollectionIndexType::Default,
                CollectionIndexType::List => aerospike_core::query::CollectionIndexType::List,
                CollectionIndexType::MapKeys => aerospike_core::query::CollectionIndexType::MapKeys,
                CollectionIndexType::MapValues => {
                    aerospike_core::query::CollectionIndexType::MapValues
                }
            }
        }
    }

    #[pymethods]
    impl CollectionIndexType {
    }
    /// User-defined function (UDF) language.
    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy)]
    pub enum UDFLang {
        /// Lua embedded programming language.
        #[pyo3(name = "LUA")]
        Lua,
    }

    impl From<&UDFLang> for aerospike_core::UDFLang {
        fn from(lang: &UDFLang) -> Self {
            match lang {
                UDFLang::Lua => aerospike_core::UDFLang::Lua,
            }
        }
    }

    impl From<UDFLang> for aerospike_core::UDFLang {
        fn from(lang: UDFLang) -> Self {
            match lang {
                UDFLang::Lua => aerospike_core::UDFLang::Lua,
            }
        }
    }
    /// Authentication mode for client connections.
    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum AuthMode {
        /// No authentication will be performed.
        #[pyo3(name = "NONE")]
        None,

        /// Uses internal authentication when user/password defined. Hashed password is stored
        /// on the server. Do not send clear password. This is the default.
        #[pyo3(name = "INTERNAL")]
        Internal,

        /// Uses external authentication (like LDAP) when user/password defined. Specific external
        /// authentication is configured on server. If TLSConfig is defined, sends clear password
        /// on node login via TLS. Will return an error if TLSConfig is not defined.
        #[pyo3(name = "EXTERNAL")]
        External,

        /// Allows authentication and authorization based on a certificate. No user name or
        /// password needs to be configured. Requires TLS and a client certificate.
        /// Requires server version 5.7.0+
        #[pyo3(name = "PKI")]
        PKI,
    }

    #[pymethods]
    impl AuthMode {
        fn __richcmp__(&self, other: &AuthMode, op: pyo3::class::basic::CompareOp) -> pyo3::PyResult<bool> {
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
    // Expose ResultCode constants from Rust core to Python
    // We use the actual CoreResultCode in Rust code, and expose matching constants to Python
    // PyO3's #[pyclass] can't be used on external types, so we create a simple class with constants
    // ResultCode wrapper to expose enum values to Python
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(name = "ResultCode", module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy)]
    pub struct ResultCode(pub(crate) CoreResultCode);

    #[gen_stub_pymethods]
    #[pymethods]
    #[allow(non_snake_case)]  // Class attributes use PascalCase to match Rust enum variants
    impl ResultCode {
        fn __richcmp__(&self, other: &ResultCode, op: CompareOp) -> PyResult<bool> {
            match op {
                CompareOp::Eq => Ok(std::mem::discriminant(&self.0) == std::mem::discriminant(&other.0)),
                CompareOp::Ne => Ok(std::mem::discriminant(&self.0) != std::mem::discriminant(&other.0)),
                _ => Ok(false),
            }
        }

        fn __hash__(&self) -> u64 {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            std::mem::discriminant(&self.0).hash(&mut hasher);
            hasher.finish()
        }

        fn __repr__(&self) -> String {
            format!("ResultCode({:?})", self.0)
        }

        // Expose enum instances as class attributes (UPPER_SNAKE_CASE for Pythonic constants)
        #[classattr]
        fn OK() -> ResultCode { ResultCode(CoreResultCode::Ok) }
        #[classattr]
        fn SERVER_ERROR() -> ResultCode { ResultCode(CoreResultCode::ServerError) }
        #[classattr]
        fn KEY_NOT_FOUND_ERROR() -> ResultCode { ResultCode(CoreResultCode::KeyNotFoundError) }
        #[classattr]
        fn GENERATION_ERROR() -> ResultCode { ResultCode(CoreResultCode::GenerationError) }
        #[classattr]
        fn PARAMETER_ERROR() -> ResultCode { ResultCode(CoreResultCode::ParameterError) }
        #[classattr]
        fn KEY_EXISTS_ERROR() -> ResultCode { ResultCode(CoreResultCode::KeyExistsError) }
        #[classattr]
        fn BIN_EXISTS_ERROR() -> ResultCode { ResultCode(CoreResultCode::BinExistsError) }
        #[classattr]
        fn CLUSTER_KEY_MISMATCH() -> ResultCode { ResultCode(CoreResultCode::ClusterKeyMismatch) }
        #[classattr]
        fn SERVER_MEM_ERROR() -> ResultCode { ResultCode(CoreResultCode::ServerMemError) }
        #[classattr]
        fn TIMEOUT() -> ResultCode { ResultCode(CoreResultCode::Timeout) }
        #[classattr]
        fn ALWAYS_FORBIDDEN() -> ResultCode { ResultCode(CoreResultCode::AlwaysForbidden) }
        #[classattr]
        fn PARTITION_UNAVAILABLE() -> ResultCode { ResultCode(CoreResultCode::PartitionUnavailable) }
        #[classattr]
        fn BIN_TYPE_ERROR() -> ResultCode { ResultCode(CoreResultCode::BinTypeError) }
        #[classattr]
        fn RECORD_TOO_BIG() -> ResultCode { ResultCode(CoreResultCode::RecordTooBig) }
        #[classattr]
        fn KEY_BUSY() -> ResultCode { ResultCode(CoreResultCode::KeyBusy) }
        #[classattr]
        fn SCAN_ABORT() -> ResultCode { ResultCode(CoreResultCode::ScanAbort) }
        #[classattr]
        fn UNSUPPORTED_FEATURE() -> ResultCode { ResultCode(CoreResultCode::UnsupportedFeature) }
        #[classattr]
        fn BIN_NOT_FOUND() -> ResultCode { ResultCode(CoreResultCode::BinNotFound) }
        #[classattr]
        fn DEVICE_OVERLOAD() -> ResultCode { ResultCode(CoreResultCode::DeviceOverload) }
        #[classattr]
        fn KEY_MISMATCH() -> ResultCode { ResultCode(CoreResultCode::KeyMismatch) }
        #[classattr]
        fn INVALID_NAMESPACE() -> ResultCode { ResultCode(CoreResultCode::InvalidNamespace) }
        #[classattr]
        fn BIN_NAME_TOO_LONG() -> ResultCode { ResultCode(CoreResultCode::BinNameTooLong) }
        #[classattr]
        fn FAIL_FORBIDDEN() -> ResultCode { ResultCode(CoreResultCode::FailForbidden) }
        #[classattr]
        fn ELEMENT_NOT_FOUND() -> ResultCode { ResultCode(CoreResultCode::ElementNotFound) }
        #[classattr]
        fn ELEMENT_EXISTS() -> ResultCode { ResultCode(CoreResultCode::ElementExists) }
        #[classattr]
        fn ENTERPRISE_ONLY() -> ResultCode { ResultCode(CoreResultCode::EnterpriseOnly) }
        #[classattr]
        fn OP_NOT_APPLICABLE() -> ResultCode { ResultCode(CoreResultCode::OpNotApplicable) }
        #[classattr]
        fn FILTERED_OUT() -> ResultCode { ResultCode(CoreResultCode::FilteredOut) }
        #[classattr]
        fn LOST_CONFLICT() -> ResultCode { ResultCode(CoreResultCode::LostConflict) }
        #[classattr]
        fn XDR_KEY_BUSY() -> ResultCode { ResultCode(CoreResultCode::XDRKeyBusy) }
        #[classattr]
        fn QUERY_END() -> ResultCode { ResultCode(CoreResultCode::QueryEnd) }
        #[classattr]
        fn SECURITY_NOT_SUPPORTED() -> ResultCode { ResultCode(CoreResultCode::SecurityNotSupported) }
        #[classattr]
        fn SECURITY_NOT_ENABLED() -> ResultCode { ResultCode(CoreResultCode::SecurityNotEnabled) }
        #[classattr]
        fn NOT_AUTHENTICATED() -> ResultCode { ResultCode(CoreResultCode::NotAuthenticated) }
        #[classattr]
        fn SECURITY_SCHEME_NOT_SUPPORTED() -> ResultCode { ResultCode(CoreResultCode::SecuritySchemeNotSupported) }
        #[classattr]
        fn INVALID_COMMAND() -> ResultCode { ResultCode(CoreResultCode::InvalidCommand) }
        #[classattr]
        fn INVALID_FIELD() -> ResultCode { ResultCode(CoreResultCode::InvalidField) }
        #[classattr]
        fn ILLEGAL_STATE() -> ResultCode { ResultCode(CoreResultCode::IllegalState) }
        #[classattr]
        fn INVALID_USER() -> ResultCode { ResultCode(CoreResultCode::InvalidUser) }
        #[classattr]
        fn USER_ALREADY_EXISTS() -> ResultCode { ResultCode(CoreResultCode::UserAlreadyExists) }
        #[classattr]
        fn FORBIDDEN_PASSWORD() -> ResultCode { ResultCode(CoreResultCode::ForbiddenPassword) }
        #[classattr]
        fn UDF_BAD_RESPONSE() -> ResultCode { ResultCode(CoreResultCode::UdfBadResponse) }
        #[classattr]
        fn INDEX_FOUND() -> ResultCode { ResultCode(CoreResultCode::IndexFound) }
        #[classattr]
        fn INDEX_NOT_FOUND() -> ResultCode { ResultCode(CoreResultCode::IndexNotFound) }
        #[classattr]
        fn INDEX_OOM() -> ResultCode { ResultCode(CoreResultCode::IndexOom) }
        #[classattr]
        fn INDEX_NOT_READABLE() -> ResultCode { ResultCode(CoreResultCode::IndexNotReadable) }
        #[classattr]
        fn INDEX_GENERIC() -> ResultCode { ResultCode(CoreResultCode::IndexGeneric) }
        #[classattr]
        fn INDEX_NAME_MAX_LEN() -> ResultCode { ResultCode(CoreResultCode::IndexNameMaxLen) }
        #[classattr]
        fn INDEX_MAX_COUNT() -> ResultCode { ResultCode(CoreResultCode::IndexMaxCount) }
        #[classattr]
        fn QUERY_ABORTED() -> ResultCode { ResultCode(CoreResultCode::QueryAborted) }
        #[classattr]
        fn QUERY_QUEUE_FULL() -> ResultCode { ResultCode(CoreResultCode::QueryQueueFull) }
        #[classattr]
        fn QUERY_TIMEOUT() -> ResultCode { ResultCode(CoreResultCode::QueryTimeout) }
        #[classattr]
        fn QUERY_GENERIC() -> ResultCode { ResultCode(CoreResultCode::QueryGeneric) }
    }
