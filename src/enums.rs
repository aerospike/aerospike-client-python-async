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
    #[pyclass(from_py_object, module = "_aerospike_async_native")]
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
#[pyclass(from_py_object, module = "_aerospike_async_native")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Replica {
    #[pyo3(name = "MASTER")]
    Master,
    #[pyo3(name = "MASTER_PROLES")]
    MasterProles,
    #[pyo3(name = "RANDOM")]
    Random,
    #[pyo3(name = "SEQUENCE")]
    Sequence,
    #[pyo3(name = "PREFER_RACK")]
    PreferRack,
}

    impl From<&Replica> for aerospike_core::policy::Replica {
        fn from(input: &Replica) -> Self {
            match &input {
                Replica::Master => aerospike_core::policy::Replica::Master,
                Replica::MasterProles => aerospike_core::policy::Replica::MasterProles,
                Replica::Random => aerospike_core::policy::Replica::Random,
                Replica::Sequence => aerospike_core::policy::Replica::Sequence,
                Replica::PreferRack => aerospike_core::policy::Replica::PreferRack,
            }
        }
    }

    impl From<&aerospike_core::policy::Replica> for Replica {
        fn from(input: &aerospike_core::policy::Replica) -> Self {
            match input {
                aerospike_core::policy::Replica::Master => Replica::Master,
                aerospike_core::policy::Replica::MasterProles => Replica::MasterProles,
                aerospike_core::policy::Replica::Random => Replica::Random,
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
    //  ReadModeAP
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Read policy for AP (availability) namespaces.
    /// Indicates how duplicates should be consulted in a read operation.
    /// Only makes a difference during migrations and only applicable in AP mode.
    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(from_py_object, module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum ReadModeAP {
        /// A single node should be involved in the read operation.
        #[pyo3(name = "ONE")]
        One,
        /// All duplicates should be consulted in the read operation.
        #[pyo3(name = "ALL")]
        All,
    }

    #[pymethods]
    impl ReadModeAP {
        fn __richcmp__(&self, other: &ReadModeAP, op: pyo3::class::basic::CompareOp) -> pyo3::PyResult<bool> {
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

    impl From<&ReadModeAP> for aerospike_core::policy::ReadModeAP {
        fn from(input: &ReadModeAP) -> Self {
            match input {
                ReadModeAP::One => aerospike_core::policy::ReadModeAP::One,
                ReadModeAP::All => aerospike_core::policy::ReadModeAP::All,
            }
        }
    }

    impl From<&aerospike_core::policy::ReadModeAP> for ReadModeAP {
        fn from(input: &aerospike_core::policy::ReadModeAP) -> Self {
            match input {
                aerospike_core::policy::ReadModeAP::One => ReadModeAP::One,
                aerospike_core::policy::ReadModeAP::All => ReadModeAP::All,
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  ReadModeSC
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Read policy for SC (strong consistency) namespaces.
    /// Determines SC read consistency options.
    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(from_py_object, module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum ReadModeSC {
        /// Ensures this client will only see an increasing sequence of record versions.
        /// Client only reads from master. This is the default.
        #[pyo3(name = "SESSION")]
        Session,
        /// Ensures all clients will only see an increasing sequence of record versions.
        /// Client only reads from master.
        #[pyo3(name = "LINEARIZE")]
        Linearize,
        /// The client may read from master or any full (non-migrating) replica.
        /// Increasing sequence of record versions is not guaranteed.
        #[pyo3(name = "ALLOW_REPLICA")]
        AllowReplica,
        /// The client may read from master or any full (non-migrating) replica or from
        /// unavailable partitions. Increasing sequence of record versions is not guaranteed.
        #[pyo3(name = "ALLOW_UNAVAILABLE")]
        AllowUnavailable,
    }

    #[pymethods]
    impl ReadModeSC {
        fn __richcmp__(&self, other: &ReadModeSC, op: pyo3::class::basic::CompareOp) -> pyo3::PyResult<bool> {
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

    impl From<&ReadModeSC> for aerospike_core::policy::ReadModeSC {
        fn from(input: &ReadModeSC) -> Self {
            match input {
                ReadModeSC::Session => aerospike_core::policy::ReadModeSC::Session,
                ReadModeSC::Linearize => aerospike_core::policy::ReadModeSC::Linearize,
                ReadModeSC::AllowReplica => aerospike_core::policy::ReadModeSC::AllowReplica,
                ReadModeSC::AllowUnavailable => aerospike_core::policy::ReadModeSC::AllowUnavailable,
            }
        }
    }

    impl From<&aerospike_core::policy::ReadModeSC> for ReadModeSC {
        fn from(input: &aerospike_core::policy::ReadModeSC) -> Self {
            match input {
                aerospike_core::policy::ReadModeSC::Session => ReadModeSC::Session,
                aerospike_core::policy::ReadModeSC::Linearize => ReadModeSC::Linearize,
                aerospike_core::policy::ReadModeSC::AllowReplica => ReadModeSC::AllowReplica,
                aerospike_core::policy::ReadModeSC::AllowUnavailable => ReadModeSC::AllowUnavailable,
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
    #[pyclass(from_py_object, module = "_aerospike_async_native")]
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
    #[pyclass(from_py_object, module = "_aerospike_async_native")]
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
    #[pyclass(from_py_object, module = "_aerospike_async_native")]
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
    #[pyclass(from_py_object, 
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
    #[pyclass(from_py_object, module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy)]
    pub enum IndexType {
        #[pyo3(name = "NUMERIC")]
        Numeric,
        #[pyo3(name = "STRING")]
        String,
        #[pyo3(name = "GEO2D_SPHERE")]
        Geo2DSphere,
        #[pyo3(name = "BLOB")]
        Blob,
    }


    impl From<&IndexType> for aerospike_core::query::IndexType {
        fn from(input: &IndexType) -> Self {
            match &input {
                IndexType::Numeric => aerospike_core::query::IndexType::Numeric,
                IndexType::String => aerospike_core::query::IndexType::String,
                IndexType::Geo2DSphere => aerospike_core::query::IndexType::Geo2DSphere,
                IndexType::Blob => aerospike_core::query::IndexType::Blob,
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
    #[pyclass(from_py_object, module = "_aerospike_async_native")]
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
    #[pyclass(from_py_object, module = "_aerospike_async_native")]
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
    #[pyclass(from_py_object, module = "_aerospike_async_native")]
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
    #[pyclass(from_py_object, name = "ResultCode", module = "_aerospike_async_native")]
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
        #[classattr]
        fn MRT_BLOCKED() -> ResultCode { ResultCode(CoreResultCode::MrtBlocked) }
        #[classattr]
        fn MRT_VERSION_MISMATCH() -> ResultCode { ResultCode(CoreResultCode::MrtVersionMismatch) }
        #[classattr]
        fn MRT_EXPIRED() -> ResultCode { ResultCode(CoreResultCode::MrtExpired) }
        #[classattr]
        fn MRT_TOO_MANY_WRITES() -> ResultCode { ResultCode(CoreResultCode::MrtTooManyWrites) }
        #[classattr]
        fn MRT_COMMITTED() -> ResultCode { ResultCode(CoreResultCode::MrtCommitted) }
        #[classattr]
        fn MRT_ABORTED() -> ResultCode { ResultCode(CoreResultCode::MrtAborted) }
        #[classattr]
        fn MRT_ALREADY_LOCKED() -> ResultCode { ResultCode(CoreResultCode::MrtAlreadyLocked) }
        #[classattr]
        fn MRT_MONITOR_EXISTS() -> ResultCode { ResultCode(CoreResultCode::MrtMonitorExists) }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  TxnState
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(from_py_object, module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum TxnState {
        #[pyo3(name = "OPEN")]
        Open,
        #[pyo3(name = "VERIFIED")]
        Verified,
        #[pyo3(name = "COMMITTED")]
        Committed,
        #[pyo3(name = "ABORTED")]
        Aborted,
    }

    #[pymethods]
    impl TxnState {
        fn __richcmp__(&self, other: &TxnState, op: pyo3::class::basic::CompareOp) -> pyo3::PyResult<bool> {
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

    impl From<aerospike_core::TxnState> for TxnState {
        fn from(s: aerospike_core::TxnState) -> Self {
            match s {
                aerospike_core::TxnState::Open => TxnState::Open,
                aerospike_core::TxnState::Verified => TxnState::Verified,
                aerospike_core::TxnState::Committed => TxnState::Committed,
                aerospike_core::TxnState::Aborted => TxnState::Aborted,
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  CommitStatus
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(from_py_object, module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum CommitStatus {
        #[pyo3(name = "OK")]
        Ok,
        #[pyo3(name = "ALREADY_COMMITTED")]
        AlreadyCommitted,
        #[pyo3(name = "ROLL_FORWARD_ABANDONED")]
        RollForwardAbandoned,
        #[pyo3(name = "CLOSE_ABANDONED")]
        CloseAbandoned,
    }

    #[pymethods]
    impl CommitStatus {
        fn __richcmp__(&self, other: &CommitStatus, op: pyo3::class::basic::CompareOp) -> pyo3::PyResult<bool> {
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

    impl From<aerospike_core::CommitStatus> for CommitStatus {
        fn from(s: aerospike_core::CommitStatus) -> Self {
            match s {
                aerospike_core::CommitStatus::Ok => CommitStatus::Ok,
                aerospike_core::CommitStatus::AlreadyCommitted => CommitStatus::AlreadyCommitted,
                aerospike_core::CommitStatus::RollForwardAbandoned => CommitStatus::RollForwardAbandoned,
                aerospike_core::CommitStatus::CloseAbandoned => CommitStatus::CloseAbandoned,
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  AbortStatus
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(from_py_object, module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum AbortStatus {
        #[pyo3(name = "OK")]
        Ok,
        #[pyo3(name = "ALREADY_ABORTED")]
        AlreadyAborted,
        #[pyo3(name = "ROLL_BACK_ABANDONED")]
        RollBackAbandoned,
        #[pyo3(name = "CLOSE_ABANDONED")]
        CloseAbandoned,
    }

    #[pymethods]
    impl AbortStatus {
        fn __richcmp__(&self, other: &AbortStatus, op: pyo3::class::basic::CompareOp) -> pyo3::PyResult<bool> {
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

    impl From<aerospike_core::AbortStatus> for AbortStatus {
        fn from(s: aerospike_core::AbortStatus) -> Self {
            match s {
                aerospike_core::AbortStatus::Ok => AbortStatus::Ok,
                aerospike_core::AbortStatus::AlreadyAborted => AbortStatus::AlreadyAborted,
                aerospike_core::AbortStatus::RollBackAbandoned => AbortStatus::RollBackAbandoned,
                aerospike_core::AbortStatus::CloseAbandoned => AbortStatus::CloseAbandoned,
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  LoopVarPart
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Identifies which element of a loop variable to access in path expressions.
    ///
    /// Used with loop-variable expression constructors such as
    /// ``FilterExpression.int_loop_var``, ``FilterExpression.map_loop_var``, etc.
    ///
    /// Requires Aerospike Server version >= 8.1.1.
    // Note: pyo3_stub_gen generates minimal stubs; full stubs are added in postprocess_stubs.py.
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(from_py_object, name = "LoopVarPart", module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct LoopVarPart(pub i64);

    #[gen_stub_pymethods]
    #[pymethods]
    impl LoopVarPart {
        /// Map key part of the loop variable.
        #[classattr]
        const MAP_KEY: LoopVarPart = LoopVarPart(0);
        /// Value part of the loop variable (list element or map value).
        #[classattr]
        const VALUE: LoopVarPart = LoopVarPart(1);
        /// Index part of the loop variable (parent list index).
        #[classattr]
        const INDEX: LoopVarPart = LoopVarPart(2);

        fn __richcmp__(&self, other: &LoopVarPart, op: pyo3::class::basic::CompareOp) -> pyo3::PyResult<bool> {
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

        fn __repr__(&self) -> String {
            let name = match self.0 {
                0 => "MAP_KEY",
                1 => "VALUE",
                2 => "INDEX",
                _ => "UNKNOWN",
            };
            format!("LoopVarPart.{}", name)
        }
    }

    impl From<&LoopVarPart> for aerospike_core::expressions::LoopVarPart {
        fn from(p: &LoopVarPart) -> Self {
            aerospike_core::expressions::LoopVarPart(p.0)
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  SelectFlags
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Flags controlling the return value of a ``CdtOperation.select_by_path`` operation.
    ///
    /// JSDK-shape namespace of plain ``int`` constants. Combine with bitwise OR
    /// (``SelectFlags.VALUE | SelectFlags.NO_FAIL``) — the result is a regular
    /// ``int`` and can be passed directly to ``CdtOperation.select_by_path(..., flag=...)``.
    ///
    /// Requires Aerospike Server version >= 8.1.1.
    // Note: pyo3_stub_gen generates minimal stubs for structs with #[classattr] constants.
    // Full stubs are added in postprocess_stubs.py.
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(name = "SelectFlags", module = "_aerospike_async_native")]
    pub struct SelectFlags;

    #[pymethods]
    impl SelectFlags {
        /// Return the full matching subtree (root to leaf), keeping only matched nodes.
        #[classattr]
        const MATCHING_TREE: i64 = 0;
        /// Return the values of the finally-selected nodes.
        #[classattr]
        const VALUE: i64 = 1;
        /// Synonym for ``VALUE`` — clarifies list element expectations.
        #[classattr]
        const LIST_VALUE: i64 = 1;
        /// Synonym for ``VALUE`` — clarifies map value expectations.
        #[classattr]
        const MAP_VALUE: i64 = 1;
        /// Return only the map keys of the finally-selected nodes.
        #[classattr]
        const MAP_KEY: i64 = 2;
        /// Return map key-value pairs of the finally-selected nodes.
        #[classattr]
        const MAP_KEY_VALUE: i64 = 3;
        /// Ignore type mismatches instead of failing.
        #[classattr]
        const NO_FAIL: i64 = 0x10;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  ModifyFlags
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Flags controlling the behavior of a ``CdtOperation.modify_by_path`` operation.
    ///
    /// JSDK-shape namespace of plain ``int`` constants. Combine with bitwise OR
    /// (``ModifyFlags.DEFAULT | ModifyFlags.NO_FAIL``) — the result is a regular
    /// ``int`` and can be passed directly to ``CdtOperation.modify_by_path(..., flag=...)``.
    ///
    /// Requires Aerospike Server version >= 8.1.1.
    // Note: pyo3_stub_gen generates minimal stubs for structs with #[classattr] constants.
    // Full stubs are added in postprocess_stubs.py.
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(name = "ModifyFlags", module = "_aerospike_async_native")]
    pub struct ModifyFlags;

    #[pymethods]
    impl ModifyFlags {
        /// Default behavior — fails on type mismatches.
        #[classattr]
        const DEFAULT: i64 = 0;
        /// Ignore type errors instead of failing.
        #[classattr]
        const NO_FAIL: i64 = 0x10;
    }
