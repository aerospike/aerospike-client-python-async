// #![deny(warnings)]
extern crate pyo3;

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use pyo3::basic::CompareOp;
use pyo3::exceptions::{PyException, PyIndexError, PyKeyError, PyValueError};
use pyo3::exceptions::{PyStopAsyncIteration, PyTypeError};
use pyo3::types::{PyBool, PyByteArray, PyBytes, PyDict, PyList};
use pyo3::{prelude::*, IntoPyObjectExt};

use pyo3_async_runtimes::tokio as pyo3_asyncio;
use pyo3_stub_gen::{
    define_stub_info_gatherer, derive::gen_stub_pyclass, derive::gen_stub_pyclass_enum, derive::gen_stub_pyfunction,
    derive::gen_stub_pymethods, PyStubType, TypeInfo,
};

use tokio::sync::{Mutex, RwLock};

#[allow(unused_imports)]
use aerospike_core::as_geo;
use aerospike_core::as_val;
use aerospike_core::errors::Error;
use aerospike_core::query::RecordStream;
use aerospike_core::ResultCode as CoreResultCode;
use aerospike_core::Value;
use aerospike_core::ParticleType;


fn bins_flag(bins: Option<Vec<String>>) -> aerospike_core::Bins {
    match bins {
        None => aerospike_core::Bins::All,
        Some(bins) => {
            if !bins.is_empty() {
                aerospike_core::Bins::Some(bins)
            } else {
                aerospike_core::Bins::None
            }
        }
    }
}

// Define a function to gather stub information.
define_stub_info_gatherer!(stub_info);

use pyo3::create_exception;

// Create all exceptions using create_exception! macro
// Base exception class
create_exception!(aerospike_async.exceptions, AerospikeError, pyo3::exceptions::PyException);

// Server-related exceptions
// ServerError is a custom exception with a result_code property
// Note: It extends PyException directly, but Python-side it should be treated as an AerospikeError subclass
#[gen_stub_pyclass(module = "_aerospike_async_native")]
#[pyclass(extends = PyException)]
pub struct ServerError {
    result_code: CoreResultCode,
}

#[gen_stub_pymethods]
#[pymethods]
impl ServerError {
    #[new]
    fn new(_message: String, result_code: ResultCode) -> PyResult<Self> {
        // Note: message is handled by the base PyException, we only store result_code
        Ok(ServerError { result_code: result_code.0 })
    }

    #[getter]
    fn result_code(&self) -> ResultCode {
        ResultCode(self.result_code)
    }
}

// Helper function to create ServerError as a PyErr
fn create_server_error(message: String, result_code: CoreResultCode) -> PyErr {
    Python::attach(|py| -> PyErr {
        let server_error_type = py.get_type::<ServerError>();
        let result_code_wrapper = ResultCode(result_code);
        match server_error_type.call1((message.clone(), result_code_wrapper)) {
            Ok(server_error_obj) => PyErr::from_value(server_error_obj),
            Err(e) => e,
        }
    })
}
create_exception!(aerospike_async.exceptions, UDFBadResponse, AerospikeError);
create_exception!(aerospike_async.exceptions, TimeoutError, AerospikeError);
create_exception!(aerospike_async.exceptions, BadResponse, AerospikeError);

// Connection-related exceptions
create_exception!(aerospike_async.exceptions, ConnectionError, AerospikeError);
create_exception!(aerospike_async.exceptions, InvalidNodeError, AerospikeError);
create_exception!(aerospike_async.exceptions, NoMoreConnections, AerospikeError);
create_exception!(aerospike_async.exceptions, RecvError, AerospikeError);

// Data parsing/validation exceptions
create_exception!(aerospike_async.exceptions, Base64DecodeError, AerospikeError);
create_exception!(aerospike_async.exceptions, InvalidUTF8, AerospikeError);
create_exception!(aerospike_async.exceptions, ParseAddressError, AerospikeError);
create_exception!(aerospike_async.exceptions, ParseIntError, AerospikeError);
create_exception!(aerospike_async.exceptions, ValueError, AerospikeError);

// System/IO exceptions
create_exception!(aerospike_async.exceptions, IoError, AerospikeError);
create_exception!(aerospike_async.exceptions, PasswordHashError, AerospikeError);

// Client configuration exceptions
create_exception!(aerospike_async.exceptions, InvalidRustClientArgs, AerospikeError);

// Client-side errors
create_exception!(aerospike_async.exceptions, ClientError, AerospikeError);


// Must define a wrapper type because of the orphan rule
struct RustClientError(Error);

impl From<RustClientError> for PyErr {
    fn from(value: RustClientError) -> Self {
        // RustClientError -> Error -> Custom Exception Classes
        match value.0 {
            Error::Base64(e) => Base64DecodeError::new_err(e.to_string()),
            Error::InvalidUtf8(e) => InvalidUTF8::new_err(e.to_string()),
            Error::Io(e) => IoError::new_err(e.to_string()),
            // MpscRecv error variant doesn't exist in TLS branch
            // Error::MpscRecv(_) => RecvError::new_err("The sending half of a channel has been closed, so no messages can be received"),
            Error::ParseAddr(e) => ParseAddressError::new_err(e.to_string()),
            Error::ParseInt(e) => ParseIntError::new_err(e.to_string()),
            Error::PwHash(e) => PasswordHashError::new_err(e.to_string()),
            Error::BadResponse(string) => BadResponse::new_err(string),
            Error::Connection(string) => ConnectionError::new_err(string),
            Error::InvalidArgument(string) => ValueError::new_err(string),
            Error::InvalidNode(string) => InvalidNodeError::new_err(string),
            Error::NoMoreConnections => NoMoreConnections::new_err("Exceeded max. number of connections per node."),
            Error::ServerError(result_code, in_doubt, node) => {
                let message = format!("Code: {:?}, In Doubt: {}, Node: {}", result_code, in_doubt, node);
                create_server_error(message, result_code)
            },
            Error::UdfBadResponse(string) => UDFBadResponse::new_err(string),
            Error::Timeout(string) => TimeoutError::new_err(string),
            Error::Chain(first, second) => {
                // For Chain errors, look for the most specific error type
                // Check first error
                match first.as_ref() {
                    Error::ServerError(result_code, in_doubt, node) => {
                        let message = format!("Code: {:?}, In Doubt: {}, Node: {}", result_code, in_doubt, node);
                        create_server_error(message, *result_code)
                    },
                    Error::BadResponse(msg) => {
                        BadResponse::new_err(msg.clone())
                    },
                    Error::ClientError(msg) => {
                        // Check second error for more specific type
                        match second.as_ref() {
                            Error::ServerError(result_code, in_doubt, node) => {
                                let message = format!("Code: {:?}, In Doubt: {}, Node: {}", result_code, in_doubt, node);
                                create_server_error(message, *result_code)
                            },
                            Error::BadResponse(msg) => {
                                BadResponse::new_err(msg.clone())
                            },
                            _ => AerospikeError::new_err(format!("Client error: {}", msg))
                        }
                    },
                    _ => {
                        // Check second error
                        match second.as_ref() {
                            Error::ServerError(result_code, in_doubt, node) => {
                                let message = format!("Code: {:?}, In Doubt: {}, Node: {}", result_code, in_doubt, node);
                                create_server_error(message, *result_code)
                            },
                            Error::BadResponse(msg) => {
                                BadResponse::new_err(msg.clone())
                            },
                            Error::ClientError(msg) => {
                                AerospikeError::new_err(format!("Client error: {}", msg))
                            },
                            _ => AerospikeError::new_err("Chain error with no recognized sub-errors")
                        }
                    }
                }
            },
            Error::ClientError(msg) => ClientError::new_err(msg),
            other => AerospikeError::new_err(format!("Unknown error: {:?}", other)),
        }
    }
}


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

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  PrivilegeCode
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Secondary index collection type.
    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy)]
    pub enum PrivilegeCode {
        /// User can edit/remove other users.  Global scope only.
        UserAdmin,

        /// User can perform systems administration functions on a database that do not involve user
        /// administration.  Examples include server configuration.
        /// Global scope only.
        SysAdmin,

        /// User can perform UDF and SINDEX administration actions. Global scope only.
        DataAdmin,

        /// User can perform user defined function(UDF) administration actions.
        /// Examples include create/drop UDF. Global scope only.
        /// Requires server version 6+
        UDFAdmin,

        /// User can perform secondary index administration actions.
        /// Examples include create/drop index. Global scope only.
        /// Requires server version 6+
        SIndexAdmin,

        /// User can read data only.
        Read,

        /// User can read and write data.
        ReadWrite,

        /// User can read and write data through user defined functions.
        ReadWriteUDF,

        /// User can read and write data through user defined functions.
        Write,

        /// User can truncate data only.
        /// Requires server version 6+
        Truncate,

        /// User can perform data masking administration actions.
        /// Global scope only.
        MaskingAdmin,

        /// User can read masked data only.
        ReadMasked,

        /// User can write masked data only.
        WriteMasked,
    }


    impl From<&PrivilegeCode> for aerospike_core::PrivilegeCode {
        fn from(input: &PrivilegeCode) -> Self {
            match &input {
                PrivilegeCode::UserAdmin => aerospike_core::PrivilegeCode::UserAdmin,
                PrivilegeCode::SysAdmin => aerospike_core::PrivilegeCode::SysAdmin,
                PrivilegeCode::DataAdmin => aerospike_core::PrivilegeCode::DataAdmin,
                PrivilegeCode::UDFAdmin => aerospike_core::PrivilegeCode::UDFAdmin,
                PrivilegeCode::SIndexAdmin => aerospike_core::PrivilegeCode::SIndexAdmin,
                PrivilegeCode::Read => aerospike_core::PrivilegeCode::Read,
                PrivilegeCode::ReadWrite => aerospike_core::PrivilegeCode::ReadWrite,
                PrivilegeCode::ReadWriteUDF => aerospike_core::PrivilegeCode::ReadWriteUDF,
                PrivilegeCode::Write => aerospike_core::PrivilegeCode::Write,
                PrivilegeCode::Truncate => aerospike_core::PrivilegeCode::Truncate,
                PrivilegeCode::MaskingAdmin => aerospike_core::PrivilegeCode::MaskingAdmin,
                PrivilegeCode::ReadMasked => aerospike_core::PrivilegeCode::ReadMasked,
                PrivilegeCode::WriteMasked => aerospike_core::PrivilegeCode::WriteMasked,
            }
        }
    }

    impl From<&aerospike_core::PrivilegeCode> for PrivilegeCode {
        fn from(input: &aerospike_core::PrivilegeCode) -> Self {
            match &input {
                aerospike_core::PrivilegeCode::UserAdmin => PrivilegeCode::UserAdmin,
                aerospike_core::PrivilegeCode::SysAdmin => PrivilegeCode::SysAdmin,
                aerospike_core::PrivilegeCode::DataAdmin => PrivilegeCode::DataAdmin,
                aerospike_core::PrivilegeCode::UDFAdmin => PrivilegeCode::UDFAdmin,
                aerospike_core::PrivilegeCode::SIndexAdmin => PrivilegeCode::SIndexAdmin,
                aerospike_core::PrivilegeCode::Read => PrivilegeCode::Read,
                aerospike_core::PrivilegeCode::ReadWrite => PrivilegeCode::ReadWrite,
                aerospike_core::PrivilegeCode::ReadWriteUDF => PrivilegeCode::ReadWriteUDF,
                aerospike_core::PrivilegeCode::Write => PrivilegeCode::Write,
                aerospike_core::PrivilegeCode::Truncate => PrivilegeCode::Truncate,
                aerospike_core::PrivilegeCode::MaskingAdmin => PrivilegeCode::MaskingAdmin,
                aerospike_core::PrivilegeCode::ReadMasked => PrivilegeCode::ReadMasked,
                aerospike_core::PrivilegeCode::WriteMasked => PrivilegeCode::WriteMasked,
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  ExpressionType (ExpType)
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Expression Data Types for usage in some `FilterExpressions`
    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy)]
    pub enum ExpType {
        #[pyo3(name = "NIL")]
        Nil,
        #[pyo3(name = "BOOL")]
        Bool,
        #[pyo3(name = "INT")]
        Int,
        #[pyo3(name = "STRING")]
        String,
        #[pyo3(name = "LIST")]
        List,
        #[pyo3(name = "MAP")]
        Map,
        #[pyo3(name = "BLOB")]
        Blob,
        #[pyo3(name = "FLOAT")]
        Float,
        #[pyo3(name = "GEO")]
        Geo,
        #[pyo3(name = "HLL")]
        HLL,
    }

    impl From<&ExpType> for aerospike_core::expressions::ExpType {
        fn from(input: &ExpType) -> Self {
            match &input {
                ExpType::Nil => aerospike_core::expressions::ExpType::NIL,
                ExpType::Bool => aerospike_core::expressions::ExpType::BOOL,
                ExpType::Int => aerospike_core::expressions::ExpType::INT,
                ExpType::String => aerospike_core::expressions::ExpType::STRING,
                ExpType::List => aerospike_core::expressions::ExpType::LIST,
                ExpType::Map => aerospike_core::expressions::ExpType::MAP,
                ExpType::Blob => aerospike_core::expressions::ExpType::BLOB,
                ExpType::Float => aerospike_core::expressions::ExpType::FLOAT,
                ExpType::Geo => aerospike_core::expressions::ExpType::GEO,
                ExpType::HLL => aerospike_core::expressions::ExpType::HLL,
            }
        }
    }

    #[pymethods]
    impl ExpType {
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  ListOrderType
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(name = "ListOrderType", module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum ListOrderType {
        /// List is not ordered. This is the default.
        #[pyo3(name = "UNORDERED")]
        Unordered,
        /// List is ordered.
        #[pyo3(name = "ORDERED")]
        Ordered,
    }

    #[pymethods]
    impl ListOrderType {
        fn __richcmp__(&self, other: &ListOrderType, op: pyo3::class::basic::CompareOp) -> pyo3::PyResult<bool> {
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

    impl From<&ListOrderType> for aerospike_core::operations::lists::ListOrderType {
        fn from(input: &ListOrderType) -> Self {
            match input {
                ListOrderType::Unordered => aerospike_core::operations::lists::ListOrderType::Unordered,
                ListOrderType::Ordered => aerospike_core::operations::lists::ListOrderType::Ordered,
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  ListWriteFlags
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(name = "ListWriteFlags", module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum ListWriteFlags {
        /// Default is the default behavior. It means: Allow duplicate values and insertions at any index.
        #[pyo3(name = "DEFAULT")]
        Default,
        /// AddUnique means: Only add unique values.
        #[pyo3(name = "ADD_UNIQUE")]
        AddUnique,
        /// InsertBounded means: Enforce list boundaries when inserting. Do not allow values to be inserted at index outside current list boundaries.
        #[pyo3(name = "INSERT_BOUNDED")]
        InsertBounded,
        /// NoFail means: do not raise error if a list item fails due to write flag constraints.
        #[pyo3(name = "NO_FAIL")]
        NoFail,
        /// Partial means: allow other valid list items to be committed if a list item fails due to write flag constraints.
        #[pyo3(name = "PARTIAL")]
        Partial,
    }

    #[pymethods]
    impl ListWriteFlags {
        fn __richcmp__(&self, other: &ListWriteFlags, op: pyo3::class::basic::CompareOp) -> pyo3::PyResult<bool> {
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

    impl From<&ListWriteFlags> for aerospike_core::operations::lists::ListWriteFlags {
        fn from(input: &ListWriteFlags) -> Self {
            match input {
                ListWriteFlags::Default => aerospike_core::operations::lists::ListWriteFlags::Default,
                ListWriteFlags::AddUnique => aerospike_core::operations::lists::ListWriteFlags::AddUnique,
                ListWriteFlags::InsertBounded => aerospike_core::operations::lists::ListWriteFlags::InsertBounded,
                ListWriteFlags::NoFail => aerospike_core::operations::lists::ListWriteFlags::NoFail,
                ListWriteFlags::Partial => aerospike_core::operations::lists::ListWriteFlags::Partial,
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  ListReturnType
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(name = "ListReturnType", module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum ListReturnType {
        /// Do not return a result.
        #[pyo3(name = "NONE")]
        None,
        /// Return index offset order.
        #[pyo3(name = "INDEX")]
        Index,
        /// Return reverse index offset order.
        #[pyo3(name = "REVERSE_INDEX")]
        ReverseIndex,
        /// Return value order.
        #[pyo3(name = "RANK")]
        Rank,
        /// Return reverse value order.
        #[pyo3(name = "REVERSE_RANK")]
        ReverseRank,
        /// Return count of items selected.
        #[pyo3(name = "COUNT")]
        Count,
        /// Return value for single key read and value list for range read.
        #[pyo3(name = "VALUE")]
        Value,
        /// Return true if count > 0.
        #[pyo3(name = "EXISTS")]
        Exists,
        /// Invert meaning of list command and return values.
        #[pyo3(name = "INVERTED")]
        Inverted,
    }

    #[pymethods]
    impl ListReturnType {
        fn __richcmp__(&self, other: &ListReturnType, op: pyo3::class::basic::CompareOp) -> pyo3::PyResult<bool> {
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

    impl From<&ListReturnType> for aerospike_core::operations::lists::ListReturnType {
        fn from(input: &ListReturnType) -> Self {
            match input {
                ListReturnType::None => aerospike_core::operations::lists::ListReturnType::None,
                ListReturnType::Index => aerospike_core::operations::lists::ListReturnType::Index,
                ListReturnType::ReverseIndex => aerospike_core::operations::lists::ListReturnType::ReverseIndex,
                ListReturnType::Rank => aerospike_core::operations::lists::ListReturnType::Rank,
                ListReturnType::ReverseRank => aerospike_core::operations::lists::ListReturnType::ReverseRank,
                ListReturnType::Count => aerospike_core::operations::lists::ListReturnType::Count,
                ListReturnType::Value => aerospike_core::operations::lists::ListReturnType::Values,
                ListReturnType::Exists => aerospike_core::operations::lists::ListReturnType::Exists,
                ListReturnType::Inverted => aerospike_core::operations::lists::ListReturnType::Inverted,
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  ListSortFlags
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(name = "ListSortFlags", module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum ListSortFlags {
        /// Default. Preserve duplicate values when sorting list.
        #[pyo3(name = "DEFAULT")]
        Default,
        /// Drop duplicate values when sorting list.
        #[pyo3(name = "DROP_DUPLICATES")]
        DropDuplicates,
    }

    #[pymethods]
    impl ListSortFlags {
        fn __richcmp__(&self, other: &ListSortFlags, op: pyo3::class::basic::CompareOp) -> pyo3::PyResult<bool> {
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

    impl From<&ListSortFlags> for aerospike_core::operations::lists::ListSortFlags {
        fn from(input: &ListSortFlags) -> Self {
            match input {
                ListSortFlags::Default => aerospike_core::operations::lists::ListSortFlags::Default,
                ListSortFlags::DropDuplicates => aerospike_core::operations::lists::ListSortFlags::DropDuplicates,
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  MapOrder
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(name = "MapOrder", module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum MapOrder {
        /// Map is not ordered. This is the default.
        #[pyo3(name = "UNORDERED")]
        Unordered,
        /// Order map by key.
        #[pyo3(name = "KEY_ORDERED")]
        KeyOrdered,
        /// Order map by key, then value.
        #[pyo3(name = "KEY_VALUE_ORDERED")]
        KeyValueOrdered,
    }

    #[pymethods]
    impl MapOrder {
        fn __richcmp__(&self, other: &MapOrder, op: pyo3::class::basic::CompareOp) -> pyo3::PyResult<bool> {
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

    impl From<&MapOrder> for aerospike_core::operations::maps::MapOrder {
        fn from(input: &MapOrder) -> Self {
            match input {
                MapOrder::Unordered => aerospike_core::operations::maps::MapOrder::Unordered,
                MapOrder::KeyOrdered => aerospike_core::operations::maps::MapOrder::KeyOrdered,
                MapOrder::KeyValueOrdered => aerospike_core::operations::maps::MapOrder::KeyValueOrdered,
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  MapWriteMode
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(name = "MapWriteMode", module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum MapWriteMode {
        /// If the key already exists, the item will be overwritten.
        /// If the key does not exist, a new item will be created.
        #[pyo3(name = "UPDATE")]
        Update,
        /// If the key already exists, the item will be overwritten.
        /// If the key does not exist, the write will fail.
        #[pyo3(name = "UPDATE_ONLY")]
        UpdateOnly,
        /// If the key already exists, the write will fail.
        /// If the key does not exist, a new item will be created.
        #[pyo3(name = "CREATE_ONLY")]
        CreateOnly,
    }

    #[pymethods]
    impl MapWriteMode {
        fn __richcmp__(&self, other: &MapWriteMode, op: pyo3::class::basic::CompareOp) -> pyo3::PyResult<bool> {
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

    impl From<&MapWriteMode> for aerospike_core::operations::maps::MapWriteMode {
        fn from(input: &MapWriteMode) -> Self {
            match input {
                MapWriteMode::Update => aerospike_core::operations::maps::MapWriteMode::Update,
                MapWriteMode::UpdateOnly => aerospike_core::operations::maps::MapWriteMode::UpdateOnly,
                MapWriteMode::CreateOnly => aerospike_core::operations::maps::MapWriteMode::CreateOnly,
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  MapReturnType
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(name = "MapReturnType", module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum MapReturnType {
        /// Do not return a result.
        #[pyo3(name = "NONE")]
        None,
        /// Return key index order.
        #[pyo3(name = "INDEX")]
        Index,
        /// Return reverse key order.
        #[pyo3(name = "REVERSE_INDEX")]
        ReverseIndex,
        /// Return value order.
        #[pyo3(name = "RANK")]
        Rank,
        /// Return reverse value order.
        #[pyo3(name = "REVERSE_RANK")]
        ReverseRank,
        /// Return count of items selected.
        #[pyo3(name = "COUNT")]
        Count,
        /// Return key for single key read and key list for range read.
        #[pyo3(name = "KEY")]
        Key,
        /// Return value for single key read and value list for range read.
        #[pyo3(name = "VALUE")]
        Value,
        /// Return key/value items.
        #[pyo3(name = "KEY_VALUE")]
        KeyValue,
        /// Returns true if count > 0.
        #[pyo3(name = "EXISTS")]
        Exists,
        /// Returns an unordered map.
        #[pyo3(name = "UNORDERED_MAP")]
        UnorderedMap,
        /// Returns an ordered map.
        #[pyo3(name = "ORDERED_MAP")]
        OrderedMap,
    }

    #[pymethods]
    impl MapReturnType {
        fn __richcmp__(&self, other: &MapReturnType, op: pyo3::class::basic::CompareOp) -> pyo3::PyResult<bool> {
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

    impl From<&MapReturnType> for aerospike_core::operations::maps::MapReturnType {
        fn from(input: &MapReturnType) -> Self {
            match input {
                MapReturnType::None => aerospike_core::operations::maps::MapReturnType::None,
                MapReturnType::Index => aerospike_core::operations::maps::MapReturnType::Index,
                MapReturnType::ReverseIndex => aerospike_core::operations::maps::MapReturnType::ReverseIndex,
                MapReturnType::Rank => aerospike_core::operations::maps::MapReturnType::Rank,
                MapReturnType::ReverseRank => aerospike_core::operations::maps::MapReturnType::ReverseRank,
                MapReturnType::Count => aerospike_core::operations::maps::MapReturnType::Count,
                MapReturnType::Key => aerospike_core::operations::maps::MapReturnType::Key,
                MapReturnType::Value => aerospike_core::operations::maps::MapReturnType::Value,
                MapReturnType::KeyValue => aerospike_core::operations::maps::MapReturnType::KeyValue,
                MapReturnType::Exists => aerospike_core::operations::maps::MapReturnType::Exists,
                MapReturnType::UnorderedMap => aerospike_core::operations::maps::MapReturnType::UnorderedMap,
                MapReturnType::OrderedMap => aerospike_core::operations::maps::MapReturnType::OrderedMap,
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  CTX (Context) for nested CDT operations
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Context for nested CDT (Complex Data Type) operations.
    /// Used to specify the location of nested lists/maps within a record.
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(name = "CTX", module = "_aerospike_async_native")]
    #[derive(Clone, Debug)]
    pub struct CTX {
        ctx: aerospike_core::operations::cdt_context::CdtContext,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl CTX {
        /// Lookup list by index offset.
        /// If the index is negative, the resolved index starts backwards from end of list.
        /// Examples: 0 = first item, 4 = fifth item, -1 = last item, -3 = third to last item.
        #[staticmethod]
        pub fn list_index(index: i64) -> Self {
            CTX {
                ctx: aerospike_core::operations::cdt_context::ctx_list_index(index),
            }
        }

        /// Create list with given type at index offset, given an order and pad.
        #[staticmethod]
        pub fn list_index_create(index: i64, order: ListOrderType, pad: bool) -> Self {
            CTX {
                ctx: aerospike_core::operations::cdt_context::ctx_list_index_create(
                    index,
                    (&order).into(),
                    pad,
                ),
            }
        }

        /// Lookup list by rank.
        /// 0 = smallest value, N = Nth smallest value, -1 = largest value.
        #[staticmethod]
        pub fn list_rank(rank: i64) -> Self {
            CTX {
                ctx: aerospike_core::operations::cdt_context::ctx_list_rank(rank),
            }
        }

        /// Lookup list by value.
        #[staticmethod]
        pub fn list_value(value: PythonValue) -> Self {
            CTX {
                ctx: aerospike_core::operations::cdt_context::ctx_list_value(
                    aerospike_core::Value::from(value),
                ),
            }
        }

        /// Lookup map by index offset.
        /// If the index is negative, the resolved index starts backwards from end of list.
        #[staticmethod]
        pub fn map_index(key: PythonValue) -> Self {
            CTX {
                ctx: aerospike_core::operations::cdt_context::ctx_map_index(
                    aerospike_core::Value::from(key),
                ),
            }
        }

        /// Lookup map by rank.
        /// 0 = smallest value, N = Nth smallest value, -1 = largest value.
        #[staticmethod]
        pub fn map_rank(rank: i64) -> Self {
            CTX {
                ctx: aerospike_core::operations::cdt_context::ctx_map_rank(rank),
            }
        }

        /// Lookup map by key.
        #[staticmethod]
        pub fn map_key(key: PythonValue) -> Self {
            CTX {
                ctx: aerospike_core::operations::cdt_context::ctx_map_key(
                    aerospike_core::Value::from(key),
                ),
            }
        }

        /// Create map with given type at map key.
        #[staticmethod]
        pub fn map_key_create(key: PythonValue, order: MapOrder) -> Self {
            CTX {
                ctx: aerospike_core::operations::cdt_context::ctx_map_key_create(
                    aerospike_core::Value::from(key),
                    (&order).into(),
                ),
            }
        }

        /// Lookup map by value.
        #[staticmethod]
        pub fn map_value(value: PythonValue) -> Self {
            CTX {
                ctx: aerospike_core::operations::cdt_context::ctx_map_value(
                    aerospike_core::Value::from(value),
                ),
            }
        }
    }

    impl From<&CTX> for aerospike_core::operations::cdt_context::CdtContext {
        fn from(ctx: &CTX) -> Self {
            ctx.ctx.clone()
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  Filter Expression
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Filter expression, which can be applied to most commands, to control which records are
    /// affected by the command.
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(
        name = "FilterExpression",
        module = "_aerospike_async_native",
        subclass,
        freelist = 1000
    )]
    #[derive(Clone)]
    pub struct FilterExpression {
        _as: aerospike_core::expressions::Expression,
    }

    impl PartialEq for FilterExpression {
        fn eq(&self, other: &Self) -> bool {
            // For now, we'll use a simple approach - compare the debug representation
            // This is not perfect but will work for testing purposes
            format!("{:?}", self._as) == format!("{:?}", other._as)
        }
    }

    impl Eq for FilterExpression {}

    impl std::hash::Hash for FilterExpression {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            // Use the debug representation for hashing
            format!("{:?}", self._as).hash(state);
        }
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl FilterExpression {
        #[staticmethod]
        /// Create a record key expression of specified type.
        pub fn key(exp_type: ExpType) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::key((&exp_type).into()),
            }
        }

        #[staticmethod]
        /// Create function that returns if the primary key is stored in the record meta data
        /// as a boolean expression. This would occur when `send_key` is true on record write.
        pub fn key_exists() -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::key_exists(),
            }
        }

        #[staticmethod]
        /// Create 64 bit int bin expression.
        pub fn int_bin(name: String) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::int_bin(name),
            }
        }

        #[staticmethod]
        /// Create string bin expression.
        pub fn string_bin(name: String) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::string_bin(name),
            }
        }

        #[staticmethod]
        /// Create blob bin expression.
        pub fn blob_bin(name: String) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::blob_bin(name),
            }
        }

        #[staticmethod]
        /// Create 64 bit float bin expression.
        pub fn float_bin(name: String) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::float_bin(name),
            }
        }

        #[staticmethod]
        /// Create geo bin expression.
        pub fn geo_bin(name: String) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::geo_bin(name),
            }
        }

        #[staticmethod]
        /// Create list bin expression.
        pub fn list_bin(name: String) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::list_bin(name),
            }
        }

        #[staticmethod]
        /// Create map bin expression.
        pub fn map_bin(name: String) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::map_bin(name),
            }
        }

        #[staticmethod]
        /// Create a HLL bin expression
        pub fn hll_bin(name: String) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::hll_bin(name),
            }
        }

        #[staticmethod]
        /// Create function that returns if bin of specified name exists.
        pub fn bin_exists(name: String) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::ne(
                    aerospike_core::expressions::bin_type(name),
                    aerospike_core::expressions::int_val(0_i64),
                ),
            }
        }

        #[staticmethod]
        /// Create function that returns bin's integer particle type.
        pub fn bin_type(name: String) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::bin_type(name),
            }
        }

        #[staticmethod]
        /// Create function that returns record set name string.
        pub fn set_name() -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::set_name(),
            }
        }

        #[staticmethod]
        /// Create function that returns record size on disk.
        /// If server storage-engine is memory, then zero is returned.
        /// Note: device_size() is deprecated, use record_size() instead for server version 7.0+.
        pub fn device_size() -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::record_size(),
            }
        }

        #[staticmethod]
        /// Create function that returns record last update time expressed as 64 bit integer
        /// nanoseconds since 1970-01-01 epoch.
        pub fn last_update() -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::last_update(),
            }
        }

        #[staticmethod]
        /// Create expression that returns milliseconds since the record was last updated.
        /// This expression usually evaluates quickly because record meta data is cached in memory.
        pub fn since_update() -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::since_update(),
            }
        }

        #[staticmethod]
        /// Create function that returns record expiration time expressed as 64 bit integer
        /// nanoseconds since 1970-01-01 epoch.
        pub fn void_time() -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::void_time(),
            }
        }

        #[staticmethod]
        /// Create function that returns record expiration time (time to live) in integer seconds.
        pub fn ttl() -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::ttl(),
            }
        }

        #[staticmethod]
        /// Create expression that returns if record has been deleted and is still in tombstone state.
        /// This expression usually evaluates quickly because record meta data is cached in memory.
        pub fn is_tombstone() -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::is_tombstone(),
            }
        }

        #[staticmethod]
        /// Create function that returns record digest modulo as integer.
        pub fn digest_modulo(modulo: i64) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::digest_modulo(modulo),
            }
        }

        #[staticmethod]
        /// Create function like regular expression string operation.
        pub fn regex_compare(regex: String, flags: i64, bin: FilterExpression) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::regex_compare(regex, flags, bin._as),
            }
        }

        #[staticmethod]
        /// Create compare geospatial operation.
        pub fn geo_compare(left: FilterExpression, right: FilterExpression) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::geo_compare(left._as, right._as),
            }
        }

        #[staticmethod]
        /// Creates 64 bit integer value
        pub fn int_val(val: i64) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::int_val(val),
            }
        }

        #[staticmethod]
        /// Creates a Boolean value
        pub fn bool_val(val: bool) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::bool_val(val),
            }
        }

        #[staticmethod]
        /// Creates String bin value
        pub fn string_val(val: String) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::string_val(val),
            }
        }

        #[staticmethod]
        /// Creates 64 bit float bin value
        pub fn float_val(val: f64) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::float_val(val),
            }
        }

        #[staticmethod]
        /// Creates Blob bin value
        pub fn blob_val(val: Vec<u8>) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::blob_val(val),
            }
        }

        #[staticmethod]
        /// Create List bin value.
        pub fn list_val(val: Vec<PythonValue>) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::list_val(
                    val.into_iter()
                        .map(|v| aerospike_core::Value::from(v))
                        .collect(),
                ),
            }
        }

        #[staticmethod]
        /// Create Map bin value.
        pub fn map_val(val: HashMap<PythonValue, PythonValue>) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::map_val(
                    val.into_iter()
                        .map(|(k, v)| {
                            (
                                aerospike_core::Value::from(k),
                                aerospike_core::Value::from(v),
                            )
                        })
                        .collect(),
                ),
            }
        }

        #[staticmethod]
        /// Create geospatial json string value.
        pub fn geo_val(val: String) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::geo_val(val),
            }
        }

        #[staticmethod]
        /// Create a Nil PHPValue
        pub fn nil() -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::nil(),
            }
        }

        #[staticmethod]
        #[pyo3(name = "not_")]
        /// Create "not" operator expression.
        pub fn not(exp: FilterExpression) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::not(exp._as),
            }
        }

        #[staticmethod]
        #[pyo3(name = "and_")]
        /// Create "and" (&&) operator that applies to a variable number of expressions.
        /// // (a > 5 || a == 0) && b < 3
        pub fn and(exps: Vec<FilterExpression>) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::and(
                    exps.into_iter().map(|exp| exp._as).collect(),
                ),
            }
        }

        #[staticmethod]
        #[pyo3(name = "or_")]
        /// Create "or" (||) operator that applies to a variable number of expressions.
        pub fn or(exps: Vec<FilterExpression>) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::or(exps.into_iter().map(|exp| exp._as).collect()),
            }
        }

        #[staticmethod]
        /// Create "xor" (^) operator that applies to a variable number of expressions.
        pub fn xor(exps: Vec<FilterExpression>) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::xor(
                    exps.into_iter().map(|exp| exp._as).collect(),
                ),
            }
        }

        #[staticmethod]
        /// Create equal (==) expression.
        pub fn eq(left: FilterExpression, right: FilterExpression) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::eq(left._as, right._as),
            }
        }

        #[staticmethod]
        /// Create not equal (!=) expression
        pub fn ne(left: FilterExpression, right: FilterExpression) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::ne(left._as, right._as),
            }
        }

        #[staticmethod]
        /// Create greater than (>) operation.
        pub fn gt(left: FilterExpression, right: FilterExpression) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::gt(left._as, right._as),
            }
        }

        #[staticmethod]
        /// Create greater than or equal (>=) operation.
        pub fn ge(left: FilterExpression, right: FilterExpression) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::ge(left._as, right._as),
            }
        }

        #[staticmethod]
        /// Create less than (<) operation.
        pub fn lt(left: FilterExpression, right: FilterExpression) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::lt(left._as, right._as),
            }
        }

        #[staticmethod]
        /// Create less than or equals (<=) operation.
        pub fn le(left: FilterExpression, right: FilterExpression) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::le(left._as, right._as),
            }
        }

        #[staticmethod]
        /// Create "add" (+) operator that applies to a variable number of expressions.
        /// Return sum of all `FilterExpressions` given. All arguments must resolve to the same type (integer or float).
        /// Requires server version 5.6.0+.
        pub fn num_add(exps: Vec<FilterExpression>) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::num_add(
                    exps.into_iter().map(|exp| exp._as).collect(),
                ),
            }
        }

        #[staticmethod]
        /// Create "subtract" (-) operator that applies to a variable number of expressions.
        /// If only one `FilterExpressions` is provided, return the negation of that argument.
        /// Otherwise, return the sum of the 2nd to Nth `FilterExpressions` subtracted from the 1st
        /// `FilterExpressions`. All `FilterExpressions` must resolve to the same type (integer or float).
        /// Requires server version 5.6.0+.
        pub fn num_sub(exps: Vec<FilterExpression>) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::num_sub(
                    exps.into_iter().map(|exp| exp._as).collect(),
                ),
            }
        }

        #[staticmethod]
        /// Create "multiply" (*) operator that applies to a variable number of expressions.
        /// Return the product of all `FilterExpressions`. If only one `FilterExpressions` is supplied, return
        /// that `FilterExpressions`. All `FilterExpressions` must resolve to the same type (integer or float).
        /// Requires server version 5.6.0+.
        pub fn num_mul(exps: Vec<FilterExpression>) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::num_mul(
                    exps.into_iter().map(|exp| exp._as).collect(),
                ),
            }
        }

        #[staticmethod]
        /// Create "divide" (/) operator that applies to a variable number of expressions.
        /// If there is only one `FilterExpressions`, returns the reciprocal for that `FilterExpressions`.
        /// Otherwise, return the first `FilterExpressions` divided by the product of the rest.
        /// All `FilterExpressions` must resolve to the same type (integer or float).
        /// Requires server version 5.6.0+.
        pub fn num_div(exps: Vec<FilterExpression>) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::num_div(
                    exps.into_iter().map(|exp| exp._as).collect(),
                ),
            }
        }

        #[staticmethod]
        /// Create "power" operator that raises a "base" to the "exponent" power.
        /// All arguments must resolve to floats.
        /// Requires server version 5.6.0+.
        pub fn num_pow(base: FilterExpression, exponent: FilterExpression) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::num_pow(base._as, exponent._as),
            }
        }

        #[staticmethod]
        /// Create "log" operator for logarithm of "num" with base "base".
        /// All arguments must resolve to floats.
        /// Requires server version 5.6.0+.
        pub fn num_log(num: FilterExpression, base: FilterExpression) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::num_log(num._as, base._as),
            }
        }

        #[staticmethod]
        /// Create "modulo" (%) operator that determines the remainder of "numerator"
        /// divided by "denominator". All arguments must resolve to integers.
        /// Requires server version 5.6.0+.
        pub fn num_mod(numerator: FilterExpression, denominator: FilterExpression) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::num_mod(numerator._as, denominator._as),
            }
        }

        #[staticmethod]
        /// Create operator that returns absolute value of a number.
        /// All arguments must resolve to integer or float.
        /// Requires server version 5.6.0+.
        pub fn num_abs(value: FilterExpression) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::num_abs(value._as),
            }
        }

        #[staticmethod]
        /// Create expression that rounds a floating point number down to the closest integer value.
        /// The return type is float.
        // Requires server version 5.6.0+.
        pub fn num_floor(num: FilterExpression) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::num_floor(num._as),
            }
        }

        #[staticmethod]
        /// Create expression that rounds a floating point number up to the closest integer value.
        /// The return type is float.
        /// Requires server version 5.6.0+.
        pub fn num_ceil(num: FilterExpression) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::num_ceil(num._as),
            }
        }

        #[staticmethod]
        /// Create expression that converts an integer to a float.
        /// Requires server version 5.6.0+.
        pub fn to_int(num: FilterExpression) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::to_int(num._as),
            }
        }

        #[staticmethod]
        /// Create expression that converts a float to an integer.
        /// Requires server version 5.6.0+.
        pub fn to_float(num: FilterExpression) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::to_float(num._as),
            }
        }

        #[staticmethod]
        /// Create integer "and" (&) operator that is applied to two or more integers.
        /// All arguments must resolve to integers.
        /// Requires server version 5.6.0+.
        pub fn int_and(exps: Vec<FilterExpression>) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::int_and(
                    exps.into_iter().map(|exp| exp._as).collect(),
                ),
            }
        }

        #[staticmethod]
        /// Create integer "or" (|) operator that is applied to two or more integers.
        /// All arguments must resolve to integers.
        /// Requires server version 5.6.0+.
        pub fn int_or(exps: Vec<FilterExpression>) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::int_or(
                    exps.into_iter().map(|exp| exp._as).collect(),
                ),
            }
        }

        #[staticmethod]
        /// Create integer "xor" (^) operator that is applied to two or more integers.
        /// All arguments must resolve to integers.
        /// Requires server version 5.6.0+.
        pub fn int_xor(exps: Vec<FilterExpression>) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::int_xor(
                    exps.into_iter().map(|exp| exp._as).collect(),
                ),
            }
        }

        #[staticmethod]
        /// Create integer "not" (~) operator.
        /// Requires server version 5.6.0+.
        pub fn int_not(exp: FilterExpression) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::int_not(exp._as),
            }
        }

        #[staticmethod]
        /// Create integer "left shift" (<<) operator.
        /// Requires server version 5.6.0+.
        pub fn int_lshift(value: FilterExpression, shift: FilterExpression) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::int_lshift(value._as, shift._as),
            }
        }

        #[staticmethod]
        /// Create integer "logical right shift" (>>>) operator.
        /// Requires server version 5.6.0+.
        pub fn int_rshift(value: FilterExpression, shift: FilterExpression) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::int_rshift(value._as, shift._as),
            }
        }

        #[staticmethod]
        /// Create integer "arithmetic right shift" (>>) operator.
        /// The sign bit is preserved and not shifted.
        /// Requires server version 5.6.0+.
        pub fn int_arshift(value: FilterExpression, shift: FilterExpression) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::int_arshift(value._as, shift._as),
            }
        }

        #[staticmethod]
        /// Create expression that returns count of integer bits that are set to 1.
        /// Requires server version 5.6.0+
        pub fn int_count(exp: FilterExpression) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::int_count(exp._as),
            }
        }

        #[staticmethod]
        /// Create expression that scans integer bits from left (most significant bit) to
        /// right (least significant bit), looking for a search bit value. When the
        /// search value is found, the index of that bit (where the most significant bit is
        /// index 0) is returned. If "search" is true, the scan will search for the bit
        /// value 1. If "search" is false it will search for bit value 0.
        /// Requires server version 5.6.0+.
        pub fn int_lscan(value: FilterExpression, search: FilterExpression) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::int_lscan(value._as, search._as),
            }
        }

        #[staticmethod]
        /// Create expression that scans integer bits from right (least significant bit) to
        /// left (most significant bit), looking for a search bit value. When the
        /// search value is found, the index of that bit (where the most significant bit is
        /// index 0) is returned. If "search" is true, the scan will search for the bit
        /// value 1. If "search" is false it will search for bit value 0.
        /// Requires server version 5.6.0+.
        pub fn int_rscan(value: FilterExpression, search: FilterExpression) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::int_rscan(value._as, search._as),
            }
        }

        #[staticmethod]
        /// Create expression that returns the minimum value in a variable number of expressions.
        /// All arguments must be the same type (integer or float).
        /// Requires server version 5.6.0+.
        pub fn min(exps: Vec<FilterExpression>) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::min(
                    exps.into_iter().map(|exp| exp._as).collect(),
                ),
            }
        }

        #[staticmethod]
        /// Create expression that returns the maximum value in a variable number of expressions.
        /// All arguments must be the same type (integer or float).
        /// Requires server version 5.6.0+.
        pub fn max(exps: Vec<FilterExpression>) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::max(
                    exps.into_iter().map(|exp| exp._as).collect(),
                ),
            }
        }

        //--------------------------------------------------
        // Variables
        //--------------------------------------------------

        #[staticmethod]
        /// Conditionally select an expression from a variable number of expression pairs
        /// followed by default expression action.
        /// Requires server version 5.6.0+.
        /// ```
        /// // Args Format: bool exp1, action exp1, bool exp2, action exp2, ..., action-default
        /// // Apply operator based on type.
        pub fn cond(exps: Vec<FilterExpression>) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::cond(
                    exps.into_iter().map(|exp| exp._as).collect(),
                ),
            }
        }

        #[staticmethod]
        /// Define variables and expressions in scope.
        /// Requires server version 5.6.0+.
        /// ```
        /// // 5 < a < 10
        pub fn exp_let(exps: Vec<FilterExpression>) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::exp_let(
                    exps.into_iter().map(|exp| exp._as).collect(),
                ),
            }
        }

        #[staticmethod]
        #[pyo3(name = "def_")]
        /// Assign variable to an expression that can be accessed later.
        /// Requires server version 5.6.0+.
        /// ```
        /// // 5 < a < 10
        pub fn def(name: String, value: FilterExpression) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::def(name, value._as),
            }
        }

        #[staticmethod]
        /// Retrieve expression value from a variable.
        /// Requires server version 5.6.0+.
        pub fn var(name: String) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::var(name),
            }
        }

        fn __richcmp__(&self, other: &FilterExpression, op: pyo3::class::basic::CompareOp) -> pyo3::PyResult<bool> {
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

        #[staticmethod]
        /// Create unknown value. Used to intentionally fail an expression.
        /// The failure can be ignored with `ExpWriteFlags` `EVAL_NO_FAIL`
        /// or `ExpReadFlags` `EVAL_NO_FAIL`.
        /// Requires server version 5.6.0+.
        pub fn unknown() -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::unknown(),
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  PartitionStatus
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(name = "PartitionStatus", module = "_aerospike_async_native")]
    #[derive(Debug)]
    pub struct PartitionStatus {
        _as: aerospike_core::query::PartitionStatus,
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

        // Note: id is read-only (matches Java's final int id)

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
            self._as.digest.map(|d| hex::encode(d))
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
                    None => Ok(py.None().into()),
                },
                "retry" => Ok(PyBool::new(py, self.get_retry()).into_bound_py_any(py).unwrap().into()),
                "digest" => match self.get_digest() {
                    Some(v) => Ok(v.into_pyobject(py).unwrap().into_any().into()),
                    None => Ok(py.None().into()),
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
    #[pyclass(
        name = "PartitionFilter",
        module = "_aerospike_async_native",
        freelist = 1000
    )]
    #[derive(Debug, Clone)]
    pub struct PartitionFilter {
        _as: aerospike_core::query::PartitionFilter,
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
            self._as.digest.map(|d| hex::encode(d))
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
                    let rt = tokio::runtime::Handle::try_current()
                        .map_err(|_| PyValueError::new_err("No Tokio runtime available. This method must be called from within an async context."))?;
                    
                    let mut py_partitions = Vec::new();
                    for arc_mutex_status in partitions.iter() {
                        let status_guard = rt.block_on(arc_mutex_status.lock());
                        let status = &*status_guard;
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
                        let bval = status._as.bval;
                        let id = status._as.id;
                        let retry = status._as.retry;
                        let digest_bytes = status._as.digest;
                        let core_status = aerospike_core::query::PartitionStatus {
                            id,
                            retry,
                            bval,
                            digest: digest_bytes,
                            node: None,
                            sequence: None,
                        };
                        rust_partitions.push(Arc::new(tokio::sync::Mutex::new(core_status)));
                    }
                    self._as.partitions = Some(rust_partitions);
                }
            }
            Ok(())
        }
    }

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
        _as: aerospike_core::policy::BasePolicy,
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
            match &self._as.consistency_level {
                aerospike_core::ConsistencyLevel::ConsistencyOne => {
                    ConsistencyLevel::ConsistencyOne
                }
                aerospike_core::ConsistencyLevel::ConsistencyAll => {
                    ConsistencyLevel::ConsistencyAll
                }
            }
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
        pub fn get_timeout(&self) -> u64 {
            self._as.total_timeout as u64
        }

        #[setter]
        pub fn set_timeout(&mut self, timeout_millis: u64) {
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
            self._as
                .sleep_between_retries
                .map(|duration| duration.as_millis() as u64)
                .unwrap_or_default()
        }

        #[setter]
        pub fn set_sleep_between_retries(&mut self, sleep_between_retries_millis: u64) {
            let sleep_between_retries = Duration::from_millis(sleep_between_retries_millis);
            self._as.sleep_between_retries = Some(sleep_between_retries);
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
        _as: aerospike_core::AdminPolicy,
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
        _as: aerospike_core::ReadPolicy,
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
            match &self._as.replica {
                aerospike_core::policy::Replica::Master => Replica::Master,
                aerospike_core::policy::Replica::Sequence => Replica::Sequence,
                aerospike_core::policy::Replica::PreferRack => Replica::PreferRack,
            }
        }

        #[setter]
        pub fn set_replica(&mut self, replica: Replica) {
            self._as.replica = match replica {
                Replica::Master => aerospike_core::policy::Replica::Master,
                Replica::Sequence => aerospike_core::policy::Replica::Sequence,
                Replica::PreferRack => aerospike_core::policy::Replica::PreferRack,
            }
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
        _as: aerospike_core::WritePolicy,
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
            match &self._as.record_exists_action {
                aerospike_core::RecordExistsAction::Update => RecordExistsAction::Update,
                aerospike_core::RecordExistsAction::UpdateOnly => RecordExistsAction::UpdateOnly,
                aerospike_core::RecordExistsAction::Replace => RecordExistsAction::Replace,
                aerospike_core::RecordExistsAction::ReplaceOnly => RecordExistsAction::ReplaceOnly,
                aerospike_core::RecordExistsAction::CreateOnly => RecordExistsAction::CreateOnly,
            }
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
            match &self._as.generation_policy {
                aerospike_core::GenerationPolicy::None => GenerationPolicy::None,
                aerospike_core::GenerationPolicy::ExpectGenEqual => {
                    GenerationPolicy::ExpectGenEqual
                }
                aerospike_core::GenerationPolicy::ExpectGenGreater => {
                    GenerationPolicy::ExpectGenGreater
                }
            }
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
            match &self._as.commit_level {
                aerospike_core::CommitLevel::CommitAll => CommitLevel::CommitAll,
                aerospike_core::CommitLevel::CommitMaster => CommitLevel::CommitMaster,
            }
        }

        #[setter]
        pub fn set_commit_level(&mut self, commit_level: CommitLevel) {
            self._as.commit_level = match commit_level {
                CommitLevel::CommitAll => aerospike_core::CommitLevel::CommitAll,
                CommitLevel::CommitMaster => aerospike_core::CommitLevel::CommitMaster,
            };
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
            match &self._as.expiration {
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
        _as: aerospike_core::QueryPolicy,
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
            match self._as.replica {
                aerospike_core::policy::Replica::Master => Replica::Master,
                aerospike_core::policy::Replica::Sequence => Replica::Sequence,
                aerospike_core::policy::Replica::PreferRack => Replica::PreferRack,
            }
        }

        #[setter]
        pub fn set_replica(&mut self, replica: Replica) {
            self._as.replica = aerospike_core::policy::Replica::from(&replica);
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

        #[getter]
        pub fn get_filter_expression(&self) -> Option<FilterExpression> {
            self._as.filter_expression().as_ref().map(|fe| FilterExpression { _as: fe.clone() })
        }

        #[setter]
        pub fn set_filter_expression(&mut self, filter_expression: Option<FilterExpression>) {
            match filter_expression {
                Some(fe) => self._as.base_policy.filter_expression = Some(fe._as),
                None => self._as.base_policy.filter_expression = None,
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  ScanPolicy
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(
        name = "ScanPolicy",
        module = "_aerospike_async_native",
        extends = BasePolicy,
        subclass,
        freelist = 1000
    )]
    #[derive(Debug, Clone)]
    pub struct ScanPolicy {
        _as: aerospike_core::ScanPolicy,
    }

    /// `ScanPolicy` encapsulates optional parameters used in scan operations.
    #[pymethods]
    impl ScanPolicy {
        #[new]
        pub fn new() -> PyClassInitializer<Self> {
            let scan_policy = ScanPolicy {
                _as: aerospike_core::ScanPolicy::default(),
            };
            let base_policy = BasePolicy::new();

            PyClassInitializer::from(base_policy).add_subclass(scan_policy)
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
        pub fn get_max_records(&self) -> u64 {
            self._as.max_records
        }

        #[setter]
        pub fn set_max_records(&mut self, max_records: u64) {
            self._as.max_records = max_records;
        }

        // #[getter]
        // pub fn get_filter_expression(&self) -> Option<FilterExpression> {
        //     match &self._as.filter_expression {
        //         Some(fe) => Some(FilterExpression { _as: fe.clone() }),
        //         None => None,
        //     }
        // }

        // #[setter]
        // pub fn set_filter_expression(&mut self, filter_expression: Option<FilterExpression>) {
        //     match filter_expression {
        //         Some(fe) => self._as.filter_expression = Some(fe._as),
        //         None => self._as.filter_expression = None,
        //     }
        // }
    }

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(
        name = "ClientPolicy",
        module = "_aerospike_async_native",
        subclass,
        freelist = 1000
    )]
    #[derive(Clone)]
    pub struct ClientPolicy {
        _as: aerospike_core::ClientPolicy,
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
                (Some(user), _) => {
                    self._as.auth_mode = aerospike_core::AuthMode::Internal(user, "".to_string());
                }
                (None, aerospike_core::AuthMode::Internal(_, _) | aerospike_core::AuthMode::External(_, _)) => {
                    self._as.auth_mode = aerospike_core::AuthMode::None;
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
            self._as.conn_pools_per_node
        }

        #[setter]
        pub fn set_conn_pools_per_node(&mut self, sz: usize) {
            self._as.conn_pools_per_node = sz;
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
            self._as.tend_interval.as_millis() as u64
        }

        #[setter]
        pub fn set_tend_interval(&mut self, interval_millis: u64) {
            self._as.tend_interval = Duration::from_millis(interval_millis);
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

    /**********************************************************************************
     *
     * Record
     *
     **********************************************************************************/

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(subclass, freelist = 1)]
    #[derive(Clone)]
    struct Record {
        _as: aerospike_core::Record,
    }

    #[pymethods]
    impl Record {
        pub fn bin(&self, name: &str) -> Option<Py<PyAny>> {
            let b = self._as.bins.get(name);
            b.map(|v| {
                let v: PythonValue = v.to_owned().into();
                Python::attach(|py| v.into_pyobject(py).unwrap().unbind())
            })
        }

        #[getter]
        pub fn get_bins(&self) -> Py<PyAny> {
            let b = self._as.bins.clone();
            let v: PythonValue = b.into();
            Python::attach(|py| v.into_pyobject(py).unwrap().unbind())
        }

        #[getter]
        pub fn get_generation(&self) -> Option<u32> {
            Some(self._as.generation)
        }

        #[getter]
        pub fn get_ttl(&self) -> Option<u32> {
            self._as.time_to_live().map(|v| v.as_secs() as u32)
        }

        #[getter]
        pub fn get_key(&self) -> Option<Key> {
            self._as.key.as_ref().map(|k| Key { _as: k.clone() })
        }

        fn __str__(&self) -> PyResult<String> {
            Ok(format!("{}", self))
        }

        fn __repr__(&self) -> PyResult<String> {
            let s = self.__str__()?;
            Ok(format!("Record({})", s))
        }
    }

    impl fmt::Display for Record {
        fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
            write!(f, "generation: {}", self._as.generation)?;
            write!(f, ", ttl: ")?;
            let _ = match self._as.time_to_live() {
                None => "None".fmt(f),
                Some(duration) => duration.as_secs().fmt(f),
            };
            write!(f, ", key: {:?}", self._as.key)?;
            write!(f, ", bins: {{")?;
            for (i, (k, v)) in self._as.bins.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "'{}': {}", k, v)?;
            }
            write!(f, "}}")?;
            Ok(())
        }
    }

    /**********************************************************************************
     *
     * Key
     *
     **********************************************************************************/

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(subclass, freelist = 1)]
    #[derive(Clone)]
    pub struct Key {
        _as: aerospike_core::Key,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl Key {
        #[new]
        fn new(namespace: &str, set: &str, key: PythonValue) -> Self {
            // Pass key value directly to core client - supports strings, bytes, integers, and None
            let _as = aerospike_core::Key::new(namespace, set, key.into()).unwrap();
            Key { _as }
        }

        #[staticmethod]
        /// Create a Key from a namespace, set, and digest (20-byte hash).
        /// The digest can be provided as bytes or a hex-encoded string.
        pub fn key_with_digest(namespace: &str, set: &str, digest: &Bound<'_, PyAny>) -> PyResult<Self> {
            let digest_bytes: Vec<u8> = if let Ok(bytes) = digest.extract::<Vec<u8>>() {
                bytes
            } else if let Ok(hex_str) = digest.extract::<String>() {
                hex::decode(&hex_str).map_err(|e| PyValueError::new_err(format!("Invalid hex digest: {}", e)))?
            } else if let Ok(byte_array) = digest.extract::<&[u8]>() {
                byte_array.to_vec()
            } else {
                return Err(PyTypeError::new_err("Digest must be bytes, bytearray, or hex string"));
            };

            if digest_bytes.len() != 20 {
                return Err(PyValueError::new_err(format!(
                    "Digest must be exactly 20 bytes, got {} bytes",
                    digest_bytes.len()
                )));
            }

            let mut digest_array = [0u8; 20];
            digest_array.copy_from_slice(&digest_bytes);

            let _as = aerospike_core::Key {
                namespace: namespace.to_string(),
                set_name: set.to_string(),
                user_key: None,
                digest: digest_array,
            };

            Ok(Key { _as })
        }

        #[getter]
        pub fn get_namespace(&self) -> String {
            self._as.namespace.clone()
        }

        #[getter]
        pub fn get_set_name(&self) -> String {
            self._as.set_name.clone()
        }

        #[getter(value)]
        pub fn get_value(&self) -> Option<PythonValue> {
            // Return key value as-is (preserves integer, string, bytes, etc.)
            match &self._as.user_key {
                Some(v) => {
                    let pv: PythonValue = v.clone().into();
                    Some(pv)
                }
                None => None,
            }
        }

        #[getter]
        pub fn get_digest(&self) -> Option<String> {
            Some(hex::encode(self._as.digest))
        }

        fn __richcmp__(&self, other: Key, op: CompareOp) -> bool {
            match op {
                CompareOp::Eq => self._as.digest == other._as.digest,
                CompareOp::Ne => self._as.digest != other._as.digest,
                _ => false,
            }
        }

        fn __str__(&self) -> PyResult<String> {
            Ok(format!("{}", self._as))
        }

        fn __repr__(&self) -> PyResult<String> {
            let s = self.__str__()?;
            Ok(format!("Key({})", s))
        }

        pub fn __copy__(&self) -> Self {
            self.clone()
        }

        pub fn __deepcopy__(&self, _memo: &Bound<PyDict>) -> Self {
            // fast bitwise copy instead of python's pickling process
            self.clone()
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  OperationType (internal enum)
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Internal enum that owns all operation data, avoiding lifetime issues when converting to
    /// the core client's operation types.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub enum OperationType {
        /// Get operation - reads all bins from the record.
        Get(),
        /// Get operation - reads a specific bin from the record.
        GetBin(String),
        /// GetHeader operation - reads only record metadata (generation, TTL), no bin data.
        GetHeader(),
        /// Put operation - writes a bin to the record.
        Put(String, PythonValue),
        /// Add operation - increments/decrements an integer bin value.
        Add(String, PythonValue),
        /// Append operation - appends to a string bin value.
        Append(String, PythonValue),
        /// Prepend operation - prepends to a string bin value.
        Prepend(String, PythonValue),
        /// Delete operation - deletes the record.
        Delete(),
        /// Touch operation - updates the record's TTL without modifying bin data.
        Touch(),
        /// List get operation - gets element at index.
        ListGet(String, i64),
        /// List size operation - gets list size.
        ListSize(String),
        /// List pop operation - pops and returns element at index.
        ListPop(String, i64),
        /// List clear operation - clears the list.
        ListClear(String),
        /// List get_range operation - gets range of elements.
        ListGetRange(String, i64, i64),
        /// List set operation - sets element at index.
        ListSet(String, i64, PythonValue),
        /// List remove operation - removes element at index.
        ListRemove(String, i64),
        /// List remove_range operation - removes range of elements.
        ListRemoveRange(String, i64, i64),
        /// List get_range_from operation - gets range from index to end.
        ListGetRangeFrom(String, i64),
        /// List pop_range operation - pops range of elements.
        ListPopRange(String, i64, i64),
        /// List pop_range_from operation - pops range from index to end.
        ListPopRangeFrom(String, i64),
        /// List remove_range_from operation - removes range from index to end.
        ListRemoveRangeFrom(String, i64),
        /// List trim operation - trims list to range.
        ListTrim(String, i64, i64),
        /// List append operation - appends a value to the list (requires ListPolicy).
        ListAppend(String, PythonValue, ListPolicy),
        /// List append_items operation - appends multiple values to the list (requires ListPolicy).
        ListAppendItems(String, Vec<PythonValue>, ListPolicy),
        /// List insert operation - inserts a value at index (requires ListPolicy).
        ListInsert(String, i64, PythonValue, ListPolicy),
        /// List insert_items operation - inserts multiple values at index (requires ListPolicy).
        ListInsertItems(String, i64, Vec<PythonValue>, ListPolicy),
        /// List increment operation - increments element at index by value (requires ListPolicy).
        ListIncrement(String, i64, i64, ListPolicy),
        /// List sort operation - sorts the list (requires ListSortFlags).
        ListSort(String, ListSortFlags),
        /// List set_order operation - sets list order (ORDERED/UNORDERED).
        ListSetOrder(String, ListOrderType),
        /// List get_by_index operation - gets element by index with return type (requires ListReturnType).
        ListGetByIndex(String, i64, ListReturnType),
        /// List get_by_index_range operation - gets elements by index range with return type (requires ListReturnType).
        ListGetByIndexRange(String, i64, Option<i64>, ListReturnType),
        /// List get_by_rank operation - gets element by rank with return type (requires ListReturnType).
        ListGetByRank(String, i64, ListReturnType),
        /// List get_by_rank_range operation - gets elements by rank range with return type (requires ListReturnType).
        ListGetByRankRange(String, i64, Option<i64>, ListReturnType),
        /// List get_by_value operation - gets elements by value with return type (requires ListReturnType).
        ListGetByValue(String, PythonValue, ListReturnType),
        /// List get_by_value_range operation - gets elements by value range with return type (requires ListReturnType).
        ListGetByValueRange(String, PythonValue, PythonValue, ListReturnType),
        /// List get_by_value_list operation - gets elements by value list with return type (requires ListReturnType).
        ListGetByValueList(String, Vec<PythonValue>, ListReturnType),
        /// List get_by_value_relative_rank_range operation - gets elements by value relative rank range (requires ListReturnType).
        ListGetByValueRelativeRankRange(String, PythonValue, i64, Option<i64>, ListReturnType),
        /// List remove_by_index operation - removes element by index with return type (requires ListReturnType).
        ListRemoveByIndex(String, i64, ListReturnType),
        /// List remove_by_index_range operation - removes elements by index range with return type (requires ListReturnType).
        ListRemoveByIndexRange(String, i64, Option<i64>, ListReturnType),
        /// List remove_by_rank operation - removes element by rank with return type (requires ListReturnType).
        ListRemoveByRank(String, i64, ListReturnType),
        /// List remove_by_rank_range operation - removes elements by rank range with return type (requires ListReturnType).
        ListRemoveByRankRange(String, i64, Option<i64>, ListReturnType),
        /// List remove_by_value operation - removes elements by value with return type (requires ListReturnType).
        ListRemoveByValue(String, PythonValue, ListReturnType),
        /// List remove_by_value_list operation - removes elements by value list with return type (requires ListReturnType).
        ListRemoveByValueList(String, Vec<PythonValue>, ListReturnType),
        /// List remove_by_value_range operation - removes elements by value range with return type (requires ListReturnType).
        ListRemoveByValueRange(String, PythonValue, PythonValue, ListReturnType),
        /// List remove_by_value_relative_rank_range operation - removes elements by value relative rank range (requires ListReturnType).
        ListRemoveByValueRelativeRankRange(String, PythonValue, i64, Option<i64>, ListReturnType),
        /// List create operation - creates a list with order and persisted index.
        ListCreate(String, ListOrderType, bool, bool),
        /// Map size operation - gets map size.
        MapSize(String),
        /// Map clear operation - clears the map.
        MapClear(String),
        /// Map put operation - puts a key-value pair (requires MapPolicy).
        MapPut(String, PythonValue, PythonValue, MapPolicy),
        /// Map put_items operation - puts multiple key-value pairs (requires MapPolicy).
        MapPutItems(String, Vec<(PythonValue, PythonValue)>, MapPolicy),
        /// Map increment_value operation - increments value by key (requires MapPolicy).
        MapIncrementValue(String, PythonValue, i64, MapPolicy),
        /// Map decrement_value operation - decrements value by key (requires MapPolicy).
        MapDecrementValue(String, PythonValue, i64, MapPolicy),
        /// Map get_by_key operation - gets value by key (requires MapReturnType).
        MapGetByKey(String, PythonValue, MapReturnType),
        /// Map remove_by_key operation - removes item by key (requires MapReturnType).
        MapRemoveByKey(String, PythonValue, MapReturnType),
        /// Map get_by_key_range operation - gets items by key range (requires MapReturnType).
        MapGetByKeyRange(String, PythonValue, PythonValue, MapReturnType),
        /// Map remove_by_key_range operation - removes items by key range (requires MapReturnType).
        MapRemoveByKeyRange(String, PythonValue, PythonValue, MapReturnType),
        /// Map get_by_index operation - gets item by index (requires MapReturnType).
        MapGetByIndex(String, i64, MapReturnType),
        /// Map remove_by_index operation - removes item by index (requires MapReturnType).
        MapRemoveByIndex(String, i64, MapReturnType),
        /// Map get_by_index_range operation - gets items by index range (requires MapReturnType).
        MapGetByIndexRange(String, i64, i64, MapReturnType),
        /// Map remove_by_index_range operation - removes items by index range (requires MapReturnType).
        MapRemoveByIndexRange(String, i64, i64, MapReturnType),
        /// Map get_by_index_range_from operation - gets items from index to end (requires MapReturnType).
        MapGetByIndexRangeFrom(String, i64, MapReturnType),
        /// Map remove_by_index_range_from operation - removes items from index to end (requires MapReturnType).
        MapRemoveByIndexRangeFrom(String, i64, MapReturnType),
        /// Map get_by_rank operation - gets item by rank (requires MapReturnType).
        MapGetByRank(String, i64, MapReturnType),
        /// Map remove_by_rank operation - removes item by rank (requires MapReturnType).
        MapRemoveByRank(String, i64, MapReturnType),
        /// Map get_by_rank_range operation - gets items by rank range (requires MapReturnType).
        MapGetByRankRange(String, i64, i64, MapReturnType),
        /// Map remove_by_rank_range operation - removes items by rank range (requires MapReturnType).
        MapRemoveByRankRange(String, i64, i64, MapReturnType),
        /// Map get_by_rank_range_from operation - gets items from rank to end (requires MapReturnType).
        MapGetByRankRangeFrom(String, i64, MapReturnType),
        /// Map remove_by_rank_range_from operation - removes items from rank to end (requires MapReturnType).
        MapRemoveByRankRangeFrom(String, i64, MapReturnType),
        /// Map get_by_value operation - gets items by value (requires MapReturnType).
        MapGetByValue(String, PythonValue, MapReturnType),
        /// Map remove_by_value operation - removes items by value (requires MapReturnType).
        MapRemoveByValue(String, PythonValue, MapReturnType),
        /// Map get_by_value_range operation - gets items by value range (requires MapReturnType).
        MapGetByValueRange(String, PythonValue, PythonValue, MapReturnType),
        /// Map remove_by_value_range operation - removes items by value range (requires MapReturnType).
        MapRemoveByValueRange(String, PythonValue, PythonValue, MapReturnType),
        /// Map get_by_key_list operation - gets items by a list of keys (requires MapReturnType).
        MapGetByKeyList(String, Vec<PythonValue>, MapReturnType),
        /// Map remove_by_key_list operation - removes items by a list of keys (requires MapReturnType).
        MapRemoveByKeyList(String, Vec<PythonValue>, MapReturnType),
        /// Map get_by_value_list operation - gets items by a list of values (requires MapReturnType).
        MapGetByValueList(String, Vec<PythonValue>, MapReturnType),
        /// Map remove_by_value_list operation - removes items by a list of values (requires MapReturnType).
        MapRemoveByValueList(String, Vec<PythonValue>, MapReturnType),
        /// Map set_map_policy operation - sets map policy.
        MapSetMapPolicy(String, MapPolicy),
        /// Map get_by_key_relative_index_range operation - gets items by key relative index range (requires MapReturnType).
        MapGetByKeyRelativeIndexRange(String, PythonValue, i64, Option<i64>, MapReturnType),
        /// Map get_by_value_relative_rank_range operation - gets items by value relative rank range (requires MapReturnType).
        MapGetByValueRelativeRankRange(String, PythonValue, i64, Option<i64>, MapReturnType),
        /// Map remove_by_key_relative_index_range operation - removes items by key relative index range (requires MapReturnType).
        MapRemoveByKeyRelativeIndexRange(String, PythonValue, i64, Option<i64>, MapReturnType),
        /// Map remove_by_value_relative_rank_range operation - removes items by value relative rank range (requires MapReturnType).
        MapRemoveByValueRelativeRankRange(String, PythonValue, i64, Option<i64>, MapReturnType),
        /// Map create operation - creates a map with order.
        MapCreate(String, MapOrder),
        /// Bit resize operation - resizes byte array (requires BitPolicy).
        BitResize(String, i64, Option<BitwiseResizeFlags>, BitPolicy),
        /// Bit insert operation - inserts bytes (requires BitPolicy).
        BitInsert(String, i64, PythonValue, BitPolicy),
        /// Bit remove operation - removes bytes (requires BitPolicy).
        BitRemove(String, i64, i64, BitPolicy),
        /// Bit set operation - sets bits (requires BitPolicy).
        BitSet(String, i64, i64, PythonValue, BitPolicy),
        /// Bit or operation - performs bitwise OR (requires BitPolicy).
        BitOr(String, i64, i64, PythonValue, BitPolicy),
        /// Bit xor operation - performs bitwise XOR (requires BitPolicy).
        BitXor(String, i64, i64, PythonValue, BitPolicy),
        /// Bit and operation - performs bitwise AND (requires BitPolicy).
        BitAnd(String, i64, i64, PythonValue, BitPolicy),
        /// Bit not operation - performs bitwise NOT (requires BitPolicy).
        BitNot(String, i64, i64, BitPolicy),
        /// Bit lshift operation - performs left shift (requires BitPolicy).
        BitLShift(String, i64, i64, i64, BitPolicy),
        /// Bit rshift operation - performs right shift (requires BitPolicy).
        BitRShift(String, i64, i64, i64, BitPolicy),
        /// Bit add operation - adds to integer value (requires BitPolicy).
        BitAdd(String, i64, i64, i64, bool, BitwiseOverflowActions, BitPolicy),
        /// Bit subtract operation - subtracts from integer value (requires BitPolicy).
        BitSubtract(String, i64, i64, i64, bool, BitwiseOverflowActions, BitPolicy),
        /// Bit set_int operation - sets integer value (requires BitPolicy).
        BitSetInt(String, i64, i64, i64, BitPolicy),
        /// Bit get operation - gets bits (read-only).
        BitGet(String, i64, i64),
        /// Bit count operation - counts set bits (read-only).
        BitCount(String, i64, i64),
        /// Bit lscan operation - scans left for value (read-only).
        BitLScan(String, i64, i64, bool),
        /// Bit rscan operation - scans right for value (read-only).
        BitRScan(String, i64, i64, bool),
        /// Bit get_int operation - gets integer value (read-only).
        BitGetInt(String, i64, i64, bool),
    }

    /// Python wrapper for Operation enum.
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(subclass, freelist = 1000)]
    #[derive(Clone, Debug)]
    pub struct Operation {
        op: OperationType,
    }

    impl Default for Operation {
        fn default() -> Self {
            Operation {
                op: OperationType::Get(),
            }
        }
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl Operation {
        #[new]
        pub fn new() -> Self {
            Self::default()
        }

        /// Create a Get operation (reads all bins).
        #[staticmethod]
        pub fn get() -> Self {
            Operation {
                op: OperationType::Get(),
            }
        }

        /// Create a Get operation for a specific bin.
        #[staticmethod]
        pub fn get_bin(bin_name: String) -> Self {
            Operation {
                op: OperationType::GetBin(bin_name),
            }
        }

        /// Create a Put operation.
        #[staticmethod]
        pub fn put(bin_name: String, value: PythonValue) -> Self {
            Operation {
                op: OperationType::Put(bin_name, value),
            }
        }

        /// Create a GetHeader operation (metadata only, no bin data).
        #[staticmethod]
        pub fn get_header() -> Self {
            Operation {
                op: OperationType::GetHeader(),
            }
        }

        /// Create a Delete operation.
        #[staticmethod]
        pub fn delete() -> Self {
            Operation {
                op: OperationType::Delete(),
            }
        }

        /// Create a Touch operation (updates TTL).
        #[staticmethod]
        pub fn touch() -> Self {
            Operation {
                op: OperationType::Touch(),
            }
        }

        /// Create an Add operation (increments/decrements integer bin value).
        #[staticmethod]
        pub fn add(bin_name: String, value: PythonValue) -> Self {
            Operation {
                op: OperationType::Add(bin_name, value),
            }
        }

        /// Create an Append operation (appends to string bin value).
        #[staticmethod]
        pub fn append(bin_name: String, value: PythonValue) -> Self {
            Operation {
                op: OperationType::Append(bin_name, value),
            }
        }

        /// Create a Prepend operation (prepends to string bin value).
        #[staticmethod]
        pub fn prepend(bin_name: String, value: PythonValue) -> Self {
            Operation {
                op: OperationType::Prepend(bin_name, value),
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  ListOperation
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// List bin operations. Create list operations used by the client's `operate()` method.
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(subclass, freelist = 1000)]
    #[derive(Clone, Debug)]
    pub struct ListOperation {
        op: OperationType,
        ctx: Option<Vec<CTX>>,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl ListOperation {
        /// Create a List get operation (gets element at index).
        #[staticmethod]
        pub fn get(bin_name: String, index: i64) -> Self {
            ListOperation {
                op: OperationType::ListGet(bin_name, index),
                ctx: None,
            }
        }

        /// Create a List size operation (gets list size).
        #[staticmethod]
        pub fn size(bin_name: String) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListSize(bin_name),
            }
        }

        /// Create a List pop operation (pops and returns element at index).
        #[staticmethod]
        pub fn pop(bin_name: String, index: i64) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListPop(bin_name, index),
            }
        }

        /// Create a List clear operation (clears the list).
        #[staticmethod]
        pub fn clear(bin_name: String) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListClear(bin_name),
            }
        }

        /// Create a List get_range operation (gets range of elements).
        #[staticmethod]
        pub fn get_range(bin_name: String, index: i64, count: i64) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListGetRange(bin_name, index, count),
            }
        }

        /// Create a List set operation (sets element at index).
        #[staticmethod]
        pub fn set(bin_name: String, index: i64, value: PythonValue) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListSet(bin_name, index, value),
            }
        }

        /// Create a List remove operation (removes element at index).
        #[staticmethod]
        pub fn remove(bin_name: String, index: i64) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListRemove(bin_name, index),
            }
        }

        /// Create a List remove_range operation (removes range of elements).
        #[staticmethod]
        pub fn remove_range(bin_name: String, index: i64, count: i64) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListRemoveRange(bin_name, index, count),
            }
        }

        /// Create a List get_range_from operation (gets range from index to end).
        #[staticmethod]
        pub fn get_range_from(bin_name: String, index: i64) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListGetRangeFrom(bin_name, index),
            }
        }

        /// Create a List pop_range operation (pops range of elements).
        #[staticmethod]
        pub fn pop_range(bin_name: String, index: i64, count: i64) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListPopRange(bin_name, index, count),
            }
        }

        /// Create a List pop_range_from operation (pops range from index to end).
        #[staticmethod]
        pub fn pop_range_from(bin_name: String, index: i64) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListPopRangeFrom(bin_name, index),
            }
        }

        /// Create a List remove_range_from operation (removes range from index to end).
        #[staticmethod]
        pub fn remove_range_from(bin_name: String, index: i64) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListRemoveRangeFrom(bin_name, index),
            }
        }

        /// Create a List trim operation (trims list to range).
        #[staticmethod]
        pub fn trim(bin_name: String, index: i64, count: i64) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListTrim(bin_name, index, count),
            }
        }

        /// Create a List append operation (appends a value to the list, requires ListPolicy).
        #[staticmethod]
        pub fn append(bin_name: String, value: PythonValue, policy: ListPolicy) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListAppend(bin_name, value, policy),
            }
        }

        /// Create a List append_items operation (appends multiple values to the list, requires ListPolicy).
        #[staticmethod]
        pub fn append_items(bin_name: String, values: Vec<PythonValue>, policy: ListPolicy) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListAppendItems(bin_name, values, policy),
            }
        }

        /// Create a List insert operation (inserts a value at index, requires ListPolicy).
        #[staticmethod]
        pub fn insert(bin_name: String, index: i64, value: PythonValue, policy: ListPolicy) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListInsert(bin_name, index, value, policy),
            }
        }

        /// Create a List insert_items operation (inserts multiple values at index, requires ListPolicy).
        #[staticmethod]
        pub fn insert_items(bin_name: String, index: i64, values: Vec<PythonValue>, policy: ListPolicy) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListInsertItems(bin_name, index, values, policy),
            }
        }

        /// Create a List increment operation (increments element at index by value, requires ListPolicy).
        #[staticmethod]
        pub fn increment(bin_name: String, index: i64, value: i64, policy: ListPolicy) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListIncrement(bin_name, index, value, policy),
            }
        }

        /// Create a List sort operation (sorts the list, requires ListSortFlags).
        #[staticmethod]
        pub fn sort(bin_name: String, flags: ListSortFlags) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListSort(bin_name, flags),
            }
        }

        /// Create a List set_order operation (sets list order ORDERED/UNORDERED).
        #[staticmethod]
        pub fn set_order(bin_name: String, order: ListOrderType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListSetOrder(bin_name, order),
            }
        }

        /// Create a List get_by_index operation (gets element by index with return type, requires ListReturnType).
        #[staticmethod]
        pub fn get_by_index(bin_name: String, index: i64, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListGetByIndex(bin_name, index, return_type),
            }
        }

        /// Create a List get_by_index_range operation (gets elements by index range with return type, requires ListReturnType).
        /// If count is None, gets from index to end of list.
        #[staticmethod]
        pub fn get_by_index_range(bin_name: String, index: i64, count: Option<i64>, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListGetByIndexRange(bin_name, index, count, return_type),
            }
        }

        /// Create a List get_by_rank operation (gets element by rank with return type, requires ListReturnType).
        #[staticmethod]
        pub fn get_by_rank(bin_name: String, rank: i64, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListGetByRank(bin_name, rank, return_type),
            }
        }

        /// Create a List get_by_rank_range operation (gets elements by rank range with return type, requires ListReturnType).
        /// If count is None, gets from rank to end of list.
        #[staticmethod]
        pub fn get_by_rank_range(bin_name: String, rank: i64, count: Option<i64>, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListGetByRankRange(bin_name, rank, count, return_type),
            }
        }

        /// Create a List get_by_value operation (gets elements by value with return type, requires ListReturnType).
        #[staticmethod]
        pub fn get_by_value(bin_name: String, value: PythonValue, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListGetByValue(bin_name, value, return_type),
            }
        }

        /// Create a List get_by_value_range operation (gets elements by value range with return type, requires ListReturnType).
        #[staticmethod]
        pub fn get_by_value_range(bin_name: String, begin: PythonValue, end: PythonValue, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListGetByValueRange(bin_name, begin, end, return_type),
            }
        }

        /// Create a List get_by_value_list operation (gets elements by value list with return type, requires ListReturnType).
        #[staticmethod]
        pub fn get_by_value_list(bin_name: String, values: Vec<PythonValue>, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListGetByValueList(bin_name, values, return_type),
            }
        }

        /// Create a List get_by_value_relative_rank_range operation (gets elements by value relative rank range, requires ListReturnType).
        #[staticmethod]
        pub fn get_by_value_relative_rank_range(bin_name: String, value: PythonValue, rank: i64, count: Option<i64>, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListGetByValueRelativeRankRange(bin_name, value, rank, count, return_type),
            }
        }

        /// Create a List remove_by_index operation (removes element by index with return type, requires ListReturnType).
        #[staticmethod]
        pub fn remove_by_index(bin_name: String, index: i64, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListRemoveByIndex(bin_name, index, return_type),
            }
        }

        /// Create a List remove_by_index_range operation (removes elements by index range with return type, requires ListReturnType).
        /// If count is None, removes from index to end of list.
        #[staticmethod]
        pub fn remove_by_index_range(bin_name: String, index: i64, count: Option<i64>, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListRemoveByIndexRange(bin_name, index, count, return_type),
            }
        }

        /// Create a List remove_by_rank operation (removes element by rank with return type, requires ListReturnType).
        #[staticmethod]
        pub fn remove_by_rank(bin_name: String, rank: i64, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListRemoveByRank(bin_name, rank, return_type),
            }
        }

        /// Create a List remove_by_rank_range operation (removes elements by rank range with return type, requires ListReturnType).
        /// If count is None, removes from rank to end of list.
        #[staticmethod]
        pub fn remove_by_rank_range(bin_name: String, rank: i64, count: Option<i64>, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListRemoveByRankRange(bin_name, rank, count, return_type),
            }
        }

        /// Create a List remove_by_value operation (removes elements by value with return type, requires ListReturnType).
        #[staticmethod]
        pub fn remove_by_value(bin_name: String, value: PythonValue, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListRemoveByValue(bin_name, value, return_type),
            }
        }

        /// Create a List remove_by_value_list operation (removes elements by value list with return type, requires ListReturnType).
        #[staticmethod]
        pub fn remove_by_value_list(bin_name: String, values: Vec<PythonValue>, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListRemoveByValueList(bin_name, values, return_type),
            }
        }

        /// Create a List remove_by_value_range operation (removes elements by value range with return type, requires ListReturnType).
        #[staticmethod]
        pub fn remove_by_value_range(bin_name: String, begin: PythonValue, end: PythonValue, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListRemoveByValueRange(bin_name, begin, end, return_type),
            }
        }

        /// Create a List remove_by_value_relative_rank_range operation (removes elements by value relative rank range, requires ListReturnType).
        #[staticmethod]
        pub fn remove_by_value_relative_rank_range(bin_name: String, value: PythonValue, rank: i64, count: Option<i64>, return_type: ListReturnType) -> Self {
            ListOperation {
                ctx: None,
                op: OperationType::ListRemoveByValueRelativeRankRange(bin_name, value, rank, count, return_type),
            }
        }

        /// Create a List create operation (creates a list with order and persisted index).
        #[staticmethod]
        pub fn create(bin_name: String, order: ListOrderType, pad: bool, persist_index: bool) -> Self {
            ListOperation {
                op: OperationType::ListCreate(bin_name, order, pad, persist_index),
                ctx: None,
            }
        }

        /// Set the context for this operation. Used for nested CDT operations.
        pub fn set_context(&self, ctx: Vec<CTX>) -> Self {
            ListOperation {
                op: self.op.clone(),
                ctx: Some(ctx),
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  MapOperation
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Map bin operations. Create map operations used by the client's `operate()` method.
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(subclass, freelist = 1000)]
    #[derive(Clone, Debug)]
    pub struct MapOperation {
        op: OperationType,
        ctx: Option<Vec<CTX>>,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl MapOperation {
        /// Create a Map size operation (gets map size).
        #[staticmethod]
        pub fn size(bin_name: String) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapSize(bin_name),
            }
        }

        #[staticmethod]
        pub fn clear(bin_name: String) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapClear(bin_name),
            }
        }

        #[staticmethod]
        pub fn put(bin_name: String, key: PythonValue, value: PythonValue, policy: MapPolicy) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapPut(bin_name, key, value, policy),
            }
        }

        #[staticmethod]
        pub fn put_items(bin_name: String, items: Vec<(PythonValue, PythonValue)>, policy: MapPolicy) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapPutItems(bin_name, items, policy),
            }
        }

        #[staticmethod]
        pub fn increment_value(bin_name: String, key: PythonValue, value: i64, policy: MapPolicy) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapIncrementValue(bin_name, key, value, policy),
            }
        }

        #[staticmethod]
        pub fn decrement_value(bin_name: String, key: PythonValue, value: i64, policy: MapPolicy) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapDecrementValue(bin_name, key, value, policy),
            }
        }

        #[staticmethod]
        pub fn get_by_key(bin_name: String, key: PythonValue, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapGetByKey(bin_name, key, return_type),
            }
        }

        #[staticmethod]
        pub fn remove_by_key(bin_name: String, key: PythonValue, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapRemoveByKey(bin_name, key, return_type),
            }
        }

        #[staticmethod]
        pub fn get_by_key_range(bin_name: String, begin: PythonValue, end: PythonValue, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapGetByKeyRange(bin_name, begin, end, return_type),
            }
        }

        #[staticmethod]
        pub fn remove_by_key_range(bin_name: String, begin: PythonValue, end: PythonValue, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapRemoveByKeyRange(bin_name, begin, end, return_type),
            }
        }

        #[staticmethod]
        pub fn get_by_index(bin_name: String, index: i64, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapGetByIndex(bin_name, index, return_type),
            }
        }

        #[staticmethod]
        pub fn remove_by_index(bin_name: String, index: i64, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapRemoveByIndex(bin_name, index, return_type),
            }
        }

        #[staticmethod]
        pub fn get_by_index_range(bin_name: String, index: i64, count: i64, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapGetByIndexRange(bin_name, index, count, return_type),
            }
        }

        #[staticmethod]
        pub fn remove_by_index_range(bin_name: String, index: i64, count: i64, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapRemoveByIndexRange(bin_name, index, count, return_type),
            }
        }

        #[staticmethod]
        pub fn get_by_index_range_from(bin_name: String, index: i64, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapGetByIndexRangeFrom(bin_name, index, return_type),
            }
        }

        #[staticmethod]
        pub fn remove_by_index_range_from(bin_name: String, index: i64, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapRemoveByIndexRangeFrom(bin_name, index, return_type),
            }
        }

        #[staticmethod]
        pub fn get_by_rank(bin_name: String, rank: i64, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapGetByRank(bin_name, rank, return_type),
            }
        }

        #[staticmethod]
        pub fn remove_by_rank(bin_name: String, rank: i64, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapRemoveByRank(bin_name, rank, return_type),
            }
        }

        #[staticmethod]
        pub fn get_by_rank_range(bin_name: String, rank: i64, count: i64, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapGetByRankRange(bin_name, rank, count, return_type),
            }
        }

        #[staticmethod]
        pub fn remove_by_rank_range(bin_name: String, rank: i64, count: i64, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapRemoveByRankRange(bin_name, rank, count, return_type),
            }
        }

        #[staticmethod]
        pub fn get_by_rank_range_from(bin_name: String, rank: i64, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapGetByRankRangeFrom(bin_name, rank, return_type),
            }
        }

        #[staticmethod]
        pub fn remove_by_rank_range_from(bin_name: String, rank: i64, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapRemoveByRankRangeFrom(bin_name, rank, return_type),
            }
        }

        #[staticmethod]
        pub fn get_by_value(bin_name: String, value: PythonValue, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapGetByValue(bin_name, value, return_type),
            }
        }

        #[staticmethod]
        pub fn remove_by_value(bin_name: String, value: PythonValue, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapRemoveByValue(bin_name, value, return_type),
            }
        }

        #[staticmethod]
        pub fn get_by_value_range(bin_name: String, begin: PythonValue, end: PythonValue, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapGetByValueRange(bin_name, begin, end, return_type),
            }
        }

        #[staticmethod]
        pub fn remove_by_value_range(bin_name: String, begin: PythonValue, end: PythonValue, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapRemoveByValueRange(bin_name, begin, end, return_type),
            }
        }

        #[staticmethod]
        pub fn get_by_key_list(bin_name: String, keys: Vec<PythonValue>, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapGetByKeyList(bin_name, keys, return_type),
            }
        }

        #[staticmethod]
        pub fn remove_by_key_list(bin_name: String, keys: Vec<PythonValue>, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapRemoveByKeyList(bin_name, keys, return_type),
            }
        }

        #[staticmethod]
        pub fn get_by_value_list(bin_name: String, values: Vec<PythonValue>, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapGetByValueList(bin_name, values, return_type),
            }
        }

        #[staticmethod]
        pub fn remove_by_value_list(bin_name: String, values: Vec<PythonValue>, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapRemoveByValueList(bin_name, values, return_type),
            }
        }

        /// Create a Map set_map_policy operation (sets map policy).
        #[staticmethod]
        pub fn set_map_policy(bin_name: String, policy: MapPolicy) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapSetMapPolicy(bin_name, policy),
            }
        }

        /// Create a Map get_by_key_relative_index_range operation (gets items by key relative index range, requires MapReturnType).
        #[staticmethod]
        pub fn get_by_key_relative_index_range(bin_name: String, key: PythonValue, index: i64, count: Option<i64>, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapGetByKeyRelativeIndexRange(bin_name, key, index, count, return_type),
            }
        }

        /// Create a Map get_by_value_relative_rank_range operation (gets items by value relative rank range, requires MapReturnType).
        #[staticmethod]
        pub fn get_by_value_relative_rank_range(bin_name: String, value: PythonValue, rank: i64, count: Option<i64>, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapGetByValueRelativeRankRange(bin_name, value, rank, count, return_type),
            }
        }

        /// Create a Map remove_by_key_relative_index_range operation (removes items by key relative index range, requires MapReturnType).
        #[staticmethod]
        pub fn remove_by_key_relative_index_range(bin_name: String, key: PythonValue, index: i64, count: Option<i64>, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapRemoveByKeyRelativeIndexRange(bin_name, key, index, count, return_type),
            }
        }

        /// Create a Map remove_by_value_relative_rank_range operation (removes items by value relative rank range, requires MapReturnType).
        #[staticmethod]
        pub fn remove_by_value_relative_rank_range(bin_name: String, value: PythonValue, rank: i64, count: Option<i64>, return_type: MapReturnType) -> Self {
            MapOperation {
                ctx: None,
                op: OperationType::MapRemoveByValueRelativeRankRange(bin_name, value, rank, count, return_type),
            }
        }

        /// Create a Map create operation (creates a map with order).
        #[staticmethod]
        pub fn create(bin_name: String, order: MapOrder) -> Self {
            MapOperation {
                op: OperationType::MapCreate(bin_name, order),
                ctx: None,
            }
        }

        /// Set the context for this operation. Used for nested CDT operations.
        pub fn set_context(&self, ctx: Vec<CTX>) -> Self {
            MapOperation {
                op: self.op.clone(),
                ctx: Some(ctx),
            }
        }
    }


    // Expose ResultCode constants from Rust core to Python
    // We use the actual CoreResultCode in Rust code, and expose matching constants to Python
    // PyO3's #[pyclass] can't be used on external types, so we create a simple class with constants
    // ResultCode wrapper to expose enum values to Python
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(name = "ResultCode", module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy)]
    pub struct ResultCode(CoreResultCode);

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
    }

    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(name = "BitwiseResizeFlags", module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum BitwiseResizeFlags {
        #[pyo3(name = "DEFAULT")]
        Default = 0,
        #[pyo3(name = "FROM_FRONT")]
        FromFront = 1,
        #[pyo3(name = "GROW_ONLY")]
        GrowOnly = 2,
        #[pyo3(name = "SHRINK_ONLY")]
        ShrinkOnly = 4,
    }

    impl From<BitwiseResizeFlags> for aerospike_core::operations::bitwise::BitwiseResizeFlags {
        fn from(flags: BitwiseResizeFlags) -> Self {
            match flags {
                BitwiseResizeFlags::Default => aerospike_core::operations::bitwise::BitwiseResizeFlags::Default,
                BitwiseResizeFlags::FromFront => aerospike_core::operations::bitwise::BitwiseResizeFlags::FromFront,
                BitwiseResizeFlags::GrowOnly => aerospike_core::operations::bitwise::BitwiseResizeFlags::GrowOnly,
                BitwiseResizeFlags::ShrinkOnly => aerospike_core::operations::bitwise::BitwiseResizeFlags::ShrinkOnly,
            }
        }
    }

    #[pymethods]
    impl BitwiseResizeFlags {
    }

    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(name = "BitwiseWriteFlags", module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum BitwiseWriteFlags {
        #[pyo3(name = "DEFAULT")]
        Default = 0,
        #[pyo3(name = "CREATE_ONLY")]
        CreateOnly = 1,
        #[pyo3(name = "UPDATE_ONLY")]
        UpdateOnly = 2,
        #[pyo3(name = "NO_FAIL")]
        NoFail = 4,
        #[pyo3(name = "PARTIAL")]
        Partial = 8,
    }

    impl From<BitwiseWriteFlags> for u8 {
        fn from(flags: BitwiseWriteFlags) -> Self {
            flags as u8
        }
    }

    #[pymethods]
    impl BitwiseWriteFlags {
    }

    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(name = "BitwiseOverflowActions", module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum BitwiseOverflowActions {
        #[pyo3(name = "FAIL")]
        Fail = 0,
        #[pyo3(name = "SATURATE")]
        Saturate = 2,
        #[pyo3(name = "WRAP")]
        Wrap = 4,
    }

    impl From<BitwiseOverflowActions> for aerospike_core::operations::bitwise::BitwiseOverflowActions {
        fn from(action: BitwiseOverflowActions) -> Self {
            match action {
                BitwiseOverflowActions::Fail => aerospike_core::operations::bitwise::BitwiseOverflowActions::Fail,
                BitwiseOverflowActions::Saturate => aerospike_core::operations::bitwise::BitwiseOverflowActions::Saturate,
                BitwiseOverflowActions::Wrap => aerospike_core::operations::bitwise::BitwiseOverflowActions::Wrap,
            }
        }
    }

    #[pymethods]
    impl BitwiseOverflowActions {
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  BitPolicy
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(
        name = "BitPolicy",
        module = "_aerospike_async_native",
        subclass,
        freelist = 1000
    )]
    #[derive(Debug, Clone, Copy)]
    pub struct BitPolicy {
        _as: aerospike_core::operations::bitwise::BitPolicy,
    }

    impl PartialEq for BitPolicy {
        fn eq(&self, other: &Self) -> bool {
            self._as.flags == other._as.flags
        }
    }

    impl Eq for BitPolicy {}

    impl Default for BitPolicy {
        fn default() -> Self {
            Self::new(None)
        }
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl BitPolicy {
        #[new]
        /// Create a new BitPolicy with the specified write flags.
        /// Default is default write flags.
        pub fn new(write_flags: Option<BitwiseWriteFlags>) -> Self {
            let write_flags = write_flags.unwrap_or(BitwiseWriteFlags::Default);
            BitPolicy {
                _as: aerospike_core::operations::bitwise::BitPolicy::new(write_flags.into()),
            }
        }

        /// Get the write flags.
        pub fn get_write_flags(&self) -> u8 {
            self._as.flags
        }

        /// Set the write flags.
        pub fn set_write_flags(&mut self, flags: BitwiseWriteFlags) {
            self._as.flags = flags.into();
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  ListPolicy
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(
        name = "ListPolicy",
        module = "_aerospike_async_native",
        subclass,
        freelist = 1000
    )]
    #[derive(Debug, Clone, Copy)]
    pub struct ListPolicy {
        _as: aerospike_core::operations::lists::ListPolicy,
    }

    impl PartialEq for ListPolicy {
        fn eq(&self, other: &Self) -> bool {
            // Compare the underlying policy fields manually since core client doesn't implement PartialEq
            self._as.attributes as u8 == other._as.attributes as u8 && self._as.flags == other._as.flags
        }
    }

    impl Eq for ListPolicy {}

    impl Default for ListPolicy {
        fn default() -> Self {
            Self::new(None, None)
        }
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl ListPolicy {
        #[new]
        /// Create a new ListPolicy with the specified order and write flags.
        /// Default is unordered list with default write flags.
        pub fn new(order: Option<ListOrderType>, write_flags: Option<ListWriteFlags>) -> Self {
            let order = order.unwrap_or(ListOrderType::Unordered);
            let write_flags = write_flags.unwrap_or(ListWriteFlags::Default);
            ListPolicy {
                _as: aerospike_core::operations::lists::ListPolicy::new(
                    (&order).into(),
                    (&write_flags).into(),
                ),
            }
        }

        #[getter]
        pub fn get_order(&self) -> ListOrderType {
            match self._as.attributes {
                aerospike_core::operations::lists::ListOrderType::Unordered => ListOrderType::Unordered,
                aerospike_core::operations::lists::ListOrderType::Ordered => ListOrderType::Ordered,
            }
        }

        #[setter]
        pub fn set_order(&mut self, order: ListOrderType) {
            self._as.attributes = (&order).into();
        }

        #[getter]
        pub fn get_write_flags(&self) -> ListWriteFlags {
            match self._as.flags {
                0 => ListWriteFlags::Default,
                1 => ListWriteFlags::AddUnique,
                2 => ListWriteFlags::InsertBounded,
                4 => ListWriteFlags::NoFail,
                8 => ListWriteFlags::Partial,
                _ => ListWriteFlags::Default,
            }
        }

        #[setter]
        pub fn set_write_flags(&mut self, write_flags: ListWriteFlags) {
            self._as.flags = match write_flags {
                ListWriteFlags::Default => 0,
                ListWriteFlags::AddUnique => 1,
                ListWriteFlags::InsertBounded => 2,
                ListWriteFlags::NoFail => 4,
                ListWriteFlags::Partial => 8,
            };
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  BitOperation
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Bit operations. Create bit operations used by the client's `operate()` method.
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(subclass, freelist = 1000)]
    #[derive(Clone, Debug)]
    pub struct BitOperation {
        op: OperationType,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl BitOperation {
        /// Create a Bit resize operation (resizes byte array, requires BitPolicy).
        #[staticmethod]
        pub fn resize(bin_name: String, byte_size: i64, resize_flags: Option<BitwiseResizeFlags>, policy: BitPolicy) -> Self {
            BitOperation {
                op: OperationType::BitResize(bin_name, byte_size, resize_flags, policy),
            }
        }
        /// Create a Bit insert operation (inserts bytes, requires BitPolicy).
        #[staticmethod]
        pub fn insert(bin_name: String, byte_offset: i64, value: PythonValue, policy: BitPolicy) -> Self {
            BitOperation {
                op: OperationType::BitInsert(bin_name, byte_offset, value, policy),
            }
        }

        /// Create a Bit remove operation (removes bytes, requires BitPolicy).
        #[staticmethod]
        pub fn remove(bin_name: String, byte_offset: i64, byte_size: i64, policy: BitPolicy) -> Self {
            BitOperation {
                op: OperationType::BitRemove(bin_name, byte_offset, byte_size, policy),
            }
        }

        /// Create a Bit set operation (sets bits, requires BitPolicy).
        #[staticmethod]
        pub fn set(bin_name: String, bit_offset: i64, bit_size: i64, value: PythonValue, policy: BitPolicy) -> Self {
            BitOperation {
                op: OperationType::BitSet(bin_name, bit_offset, bit_size, value, policy),
            }
        }

        /// Create a Bit or operation (performs bitwise OR, requires BitPolicy).
        #[staticmethod]
        pub fn or(bin_name: String, bit_offset: i64, bit_size: i64, value: PythonValue, policy: BitPolicy) -> Self {
            BitOperation {
                op: OperationType::BitOr(bin_name, bit_offset, bit_size, value, policy),
            }
        }

        /// Create a Bit xor operation (performs bitwise XOR, requires BitPolicy).
        #[staticmethod]
        pub fn xor(bin_name: String, bit_offset: i64, bit_size: i64, value: PythonValue, policy: BitPolicy) -> Self {
            BitOperation {
                op: OperationType::BitXor(bin_name, bit_offset, bit_size, value, policy),
            }
        }

        /// Create a Bit and operation (performs bitwise AND, requires BitPolicy).
        #[staticmethod]
        pub fn and(bin_name: String, bit_offset: i64, bit_size: i64, value: PythonValue, policy: BitPolicy) -> Self {
            BitOperation {
                op: OperationType::BitAnd(bin_name, bit_offset, bit_size, value, policy),
            }
        }

        /// Create a Bit not operation (performs bitwise NOT, requires BitPolicy).
        #[staticmethod]
        pub fn not(bin_name: String, bit_offset: i64, bit_size: i64, policy: BitPolicy) -> Self {
            BitOperation {
                op: OperationType::BitNot(bin_name, bit_offset, bit_size, policy),
            }
        }

        /// Create a Bit lshift operation (performs left shift, requires BitPolicy).
        #[staticmethod]
        pub fn lshift(bin_name: String, bit_offset: i64, bit_size: i64, shift: i64, policy: BitPolicy) -> Self {
            BitOperation {
                op: OperationType::BitLShift(bin_name, bit_offset, bit_size, shift, policy),
            }
        }

        /// Create a Bit rshift operation (performs right shift, requires BitPolicy).
        #[staticmethod]
        pub fn rshift(bin_name: String, bit_offset: i64, bit_size: i64, shift: i64, policy: BitPolicy) -> Self {
            BitOperation {
                op: OperationType::BitRShift(bin_name, bit_offset, bit_size, shift, policy),
            }
        }

        /// Create a Bit add operation (adds to integer value, requires BitPolicy).
        #[staticmethod]
        pub fn add(bin_name: String, bit_offset: i64, bit_size: i64, value: i64, signed: bool, action: BitwiseOverflowActions, policy: BitPolicy) -> Self {
            BitOperation {
                op: OperationType::BitAdd(bin_name, bit_offset, bit_size, value, signed, action, policy),
            }
        }

        /// Create a Bit subtract operation (subtracts from integer value, requires BitPolicy).
        #[staticmethod]
        pub fn subtract(bin_name: String, bit_offset: i64, bit_size: i64, value: i64, signed: bool, action: BitwiseOverflowActions, policy: BitPolicy) -> Self {
            BitOperation {
                op: OperationType::BitSubtract(bin_name, bit_offset, bit_size, value, signed, action, policy),
            }
        }

        /// Create a Bit set_int operation (sets integer value, requires BitPolicy).
        #[staticmethod]
        pub fn set_int(bin_name: String, bit_offset: i64, bit_size: i64, value: i64, policy: BitPolicy) -> Self {
            BitOperation {
                op: OperationType::BitSetInt(bin_name, bit_offset, bit_size, value, policy),
            }
        }

        /// Create a Bit get operation (gets bits, read-only).
        #[staticmethod]
        pub fn get(bin_name: String, bit_offset: i64, bit_size: i64) -> Self {
            BitOperation {
                op: OperationType::BitGet(bin_name, bit_offset, bit_size),
            }
        }

        /// Create a Bit count operation (counts set bits, read-only).
        #[staticmethod]
        pub fn count(bin_name: String, bit_offset: i64, bit_size: i64) -> Self {
            BitOperation {
                op: OperationType::BitCount(bin_name, bit_offset, bit_size),
            }
        }

        /// Create a Bit lscan operation (scans left for value, read-only).
        #[staticmethod]
        pub fn lscan(bin_name: String, bit_offset: i64, bit_size: i64, value: bool) -> Self {
            BitOperation {
                op: OperationType::BitLScan(bin_name, bit_offset, bit_size, value),
            }
        }

        /// Create a Bit rscan operation (scans right for value, read-only).
        #[staticmethod]
        pub fn rscan(bin_name: String, bit_offset: i64, bit_size: i64, value: bool) -> Self {
            BitOperation {
                op: OperationType::BitRScan(bin_name, bit_offset, bit_size, value),
            }
        }

        /// Create a Bit get_int operation (gets integer value, read-only).
        #[staticmethod]
        pub fn get_int(bin_name: String, bit_offset: i64, bit_size: i64, signed: bool) -> Self {
            BitOperation {
                op: OperationType::BitGetInt(bin_name, bit_offset, bit_size, signed),
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  MapPolicy
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(
        name = "MapPolicy",
        module = "_aerospike_async_native",
        subclass,
        freelist = 1000
    )]
    #[derive(Debug, Clone, Copy)]
    pub struct MapPolicy {
        _as: aerospike_core::operations::maps::MapPolicy,
    }

    impl PartialEq for MapPolicy {
        fn eq(&self, other: &Self) -> bool {
            // Compare the underlying policy fields manually since core client doesn't implement PartialEq
            self._as.order as u8 == other._as.order as u8 && self._as.write_mode as u8 == other._as.write_mode as u8
        }
    }

    impl Eq for MapPolicy {}

    impl Default for MapPolicy {
        fn default() -> Self {
            Self::new(None, None)
        }
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl MapPolicy {
        #[new]
        /// Create a new MapPolicy with the specified order and write mode.
        /// Default is unordered map with update write mode.
        pub fn new(order: Option<MapOrder>, write_mode: Option<MapWriteMode>) -> Self {
            let order = order.unwrap_or(MapOrder::Unordered);
            let write_mode = write_mode.unwrap_or(MapWriteMode::Update);
            MapPolicy {
                _as: aerospike_core::operations::maps::MapPolicy::new(
                    (&order).into(),
                    (&write_mode).into(),
                ),
            }
        }

        #[getter]
        pub fn get_order(&self) -> MapOrder {
            match self._as.order {
                aerospike_core::operations::maps::MapOrder::Unordered => MapOrder::Unordered,
                aerospike_core::operations::maps::MapOrder::KeyOrdered => MapOrder::KeyOrdered,
                aerospike_core::operations::maps::MapOrder::KeyValueOrdered => MapOrder::KeyValueOrdered,
            }
        }

        #[setter]
        pub fn set_order(&mut self, order: MapOrder) {
            self._as.order = (&order).into();
        }

        #[getter]
        pub fn get_write_mode(&self) -> MapWriteMode {
            match self._as.write_mode {
                aerospike_core::operations::maps::MapWriteMode::Update => MapWriteMode::Update,
                aerospike_core::operations::maps::MapWriteMode::UpdateOnly => MapWriteMode::UpdateOnly,
                aerospike_core::operations::maps::MapWriteMode::CreateOnly => MapWriteMode::CreateOnly,
            }
        }

        #[setter]
        pub fn set_write_mode(&mut self, write_mode: MapWriteMode) {
            self._as.write_mode = (&write_mode).into();
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  Statement
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Query statement parameters.
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(
        name = "Statement",
        module = "_aerospike_async_native",
        subclass,
        freelist = 1000
    )]
    #[derive(Clone)]
    pub struct Statement {
        _as: aerospike_core::Statement,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl Statement {
        #[new]
        #[pyo3(signature = (namespace, set_name = None, bins = None, index_name = None))]
        pub fn __construct(
            namespace: &str,
            set_name: Option<&str>,
            bins: Option<Vec<String>>,
            index_name: Option<String>,
        ) -> Self {
            let set_name_str = set_name.unwrap_or("");
            let mut stmt = Statement {
                _as: aerospike_core::Statement::new(namespace, set_name_str, bins_flag(bins)),
            };
            stmt._as.index_name = index_name;
            stmt
        }

        #[getter]
        pub fn get_index_name(&self) -> Option<String> {
            self._as.index_name.clone()
        }

        #[setter]
        pub fn set_index_name(&mut self, index_name: Option<String>) {
            self._as.index_name = index_name;
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
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  Filter
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Query filter definition. Currently, only one filter is allowed in a Statement, and must be on a
    /// bin which has a secondary index defined.
    ///
    /// Filter instances should be instantiated using one of the provided macros:
    ///
    /// - `as_eq`
    /// - `as_range`
    /// - `as_contains`
    /// - `as_contains_range`
    /// - `as_within_region`
    /// - `as_within_radius`
    /// - `as_regions_containing_point`
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(
        name = "Filter",
        module = "_aerospike_async_native",
        subclass,
        freelist = 1000
    )]
    #[derive(Clone, Debug)]
    pub struct Filter {
        _as: aerospike_core::query::Filter,
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

        #[staticmethod]
        pub fn equal(bin_name: &str, value: PythonValue) -> Self {
            Filter {
                _as: aerospike_core::as_eq!(
                    bin_name,
                    aerospike_core::Value::from(value)
                ),
            }
        }

        #[staticmethod]
        pub fn range(bin_name: &str, begin: PythonValue, end: PythonValue) -> Self {
            Filter {
                _as: aerospike_core::as_range!(
                    bin_name,
                    aerospike_core::Value::from(begin),
                    aerospike_core::Value::from(end)
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
                _as: aerospike_core::as_contains!(
                    bin_name,
                    aerospike_core::Value::from(value),
                    aerospike_core::query::CollectionIndexType::from(cit)
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
                _as: aerospike_core::as_contains_range!(
                    bin_name,
                    aerospike_core::Value::from(begin),
                    aerospike_core::Value::from(end),
                    aerospike_core::query::CollectionIndexType::from(cit)
                ),
            }
        }

        #[staticmethod]
        // Example code :
        // $pointString = '{"type":"AeroCircle","coordinates":[[-89.0000,23.0000], 1000]}'
        // Filter::regionsContainingPoint("binName", $pointString)
        pub fn within_region(
            bin_name: &str,
            region: &str,
            cit: Option<&CollectionIndexType>,
        ) -> Self {
            let default = CollectionIndexType::Default;
            let cit = cit.unwrap_or(&default);
            Filter {
                _as: aerospike_core::as_within_region!(
                    bin_name,
                    region,
                    aerospike_core::query::CollectionIndexType::from(cit)
                ),
            }
        }

        #[staticmethod]
        // Example code :
        // $lng = -89.0005;
        // $lat = 43.0004;
        // $radius = 1000;
        // $filter = Filter::withinRadius("binName", $lng, $lat, $radius);
        // Note: Public API uses (lng, lat) to match GeoJSON standard [longitude, latitude]
        //
        // WORKAROUND: The as_within_radius! macro has bugs:
        // 1. It expects parameters in (lat, lng) order, not (lng, lat)
        // 2. It has a typo: generates "Aeroircle" instead of "AeroCircle"
        // Since we can't fix the macro (it's in aerospike-core), we manually construct
        // the AeroCircle GeoJSON string with correct type name and use within_region
        pub fn within_radius(
            bin_name: &str,
            lng: f64,
            lat: f64,
            radius: f64,
            cit: Option<&CollectionIndexType>,
        ) -> Self {
            let default = CollectionIndexType::Default;
            let cit = cit.unwrap_or(&default);

            // Manually construct AeroCircle GeoJSON string
            // Format: { "type": "AeroCircle", "coordinates": [[lng, lat], radius] }
            // Note: Must use "AeroCircle" (correct) not "Aeroircle" (macro typo)
            let aero_circle = format!(
                "{{ \"type\": \"AeroCircle\", \"coordinates\": [[{:.8}, {:.8}], {}] }}",
                lng, lat, radius
            );

            // Use within_region with correctly formatted AeroCircle string
            Filter {
                _as: aerospike_core::as_within_region!(
                    bin_name,
                    &aero_circle,
                    aerospike_core::query::CollectionIndexType::from(cit)
                ),
            }
        }

        #[staticmethod]
        // Example code :
        // $pointString = '{"type":"Point","coordinates":[-89.0000,23.0000]}'
        // Filter::regionsContainingPoint("binName", $pointString)
        pub fn regions_containing_point(
            bin_name: &str,
            point: &str,
            cit: Option<&CollectionIndexType>,
        ) -> Self {
            let default = CollectionIndexType::Default;
            let cit = cit.unwrap_or(&default);
            Filter {
                _as: aerospike_core::as_regions_containing_point!(
                    bin_name,
                    point,
                    aerospike_core::query::CollectionIndexType::from(cit)
                ),
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
    #[pyclass(
        name = "Recordset",
        module = "_aerospike_async_native",
        subclass,
        freelist = 1000
    )]
    pub struct Recordset {
        _as: Arc<aerospike_core::Recordset>,
        _stream: Arc<Mutex<Option<Pin<Box<RecordStream>>>>>,
    }

    impl Clone for Recordset {
        fn clone(&self) -> Self {
            Recordset {
                _as: self._as.clone(),
                _stream: Arc::new(Mutex::new(None)),
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

        fn __aiter__(&self) -> Self {
            self.clone()
        }

        fn __anext__<'a>(&'a mut self, py: Python<'a>) -> PyResult<Py<PyAny>> {
            let recordset = self._as.clone();
            let stream_mutex = self._stream.clone();

            pyo3_asyncio::future_into_py(py, async move {
                // Initialize stream if needed, then poll
                let mut stream_opt = stream_mutex.lock().await;
                if stream_opt.is_none() {
                    *stream_opt = Some(Box::pin(recordset.clone().into_stream()));
                }

                if let Some(ref mut stream) = *stream_opt {
                    use futures::StreamExt;
                    match stream.as_mut().next().await {
                        Some(Ok(rec)) => {
                            Python::attach(|py| {
                                let res = Record { _as: rec };
                                let py_obj: Py<PyAny> = res.into_pyobject(py).unwrap().unbind().into();
                                Ok(Some(py_obj))
                            })
                        }
                        Some(Err(e)) => {
                            Err(PyErr::from(RustClientError(e)))
                        }
                        None => {
                            Err(PyStopAsyncIteration::new_err("Recordset iteration complete"))
                        }
                    }
                } else {
                    Err(PyStopAsyncIteration::new_err("Recordset iteration complete"))
                }
            })
            .map(|bound| bound.unbind())
        }
    }

    /**********************************************************************************
     *
     * User
     *
     **********************************************************************************/

    #[pyclass(subclass, freelist = 1, module = "_aerospike_async_native")]
    #[derive(Clone)]
    struct User {
        _as: aerospike_core::User,
    }

    #[pymethods]
    impl User {
        #[getter]
        /// User name.
        pub fn get_user(&self) -> String {
            self._as.user.clone()
        }

        #[getter]
        /// List of assigned roles.
        pub fn get_roles(&self) -> Vec<String> {
            self._as.roles.clone()
        }

        #[getter]
        /// List of read statistics. List may be nil.
        /// Current statistics by offset are:
        ///
        /// 0: read quota in records per second
        /// 1: single record read command rate (TPS)
        /// 2: read scan/query record per second rate (RPS)
        /// 3: number of limitless read scans/queries
        ///
        /// Future server releases may add additional statistics.
        pub fn get_read_info(&self) -> Vec<u32> {
            self._as.read_info.clone()
        }

        #[getter]
        /// List of write statistics. List may be nil.
        /// Current statistics by offset are:
        ///
        /// 0: write quota in records per second
        /// 1: single record write command rate (TPS)
        /// 2: write scan/query record per second rate (RPS)
        /// 3: number of limitless write scans/queries
        ///
        /// Future server releases may add additional statistics.
        pub fn get_write_info(&self) -> Vec<u32> {
            self._as.write_info.clone()
        }

        #[getter]
        /// Number of currently open connections for the user
        pub fn get_conns_in_user(&self) -> u32 {
            self._as.conns_in_use
        }
    }

    /**********************************************************************************
     *
     * Role
     *
     **********************************************************************************/

    #[pyclass(subclass, freelist = 1, module = "_aerospike_async_native")]
    #[derive(Clone)]
    struct Role {
        _as: aerospike_core::Role,
    }

    #[pymethods]
    impl Role {
        #[getter]
        /// Role name.
        pub fn get_name(&self) -> String {
            self._as.name.clone()
        }

        #[getter]
        /// List of assigned privileges.
        pub fn get_privileges(&self) -> Vec<Privilege> {
            self._as
                .privileges
                .iter()
                .map(|p| Privilege { _as: p.clone() })
                .collect()
        }

        #[getter]
        /// The list of allowable IP addresses.
        pub fn get_allowlist(&self) -> Vec<String> {
            self._as.allowlist.clone()
        }

        #[getter]
        /// Maximum reads per second limit for the role.
        pub fn get_read_quota(&self) -> u32 {
            self._as.read_quota
        }

        #[getter]
        /// Maximum writes per second limit for the role.
        pub fn get_write_quota(&self) -> u32 {
            self._as.write_quota
        }
    }

    /**********************************************************************************
     *
     * Privilege
     *
     **********************************************************************************/

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(
        name = "Privilege",
        module = "_aerospike_async_native",
        subclass,
        freelist = 1
    )]
    #[derive(Clone)]
    pub struct Privilege {
        _as: aerospike_core::Privilege,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl Privilege {
        #[new]
        pub fn __construct(
            code: &PrivilegeCode,
            namespace: Option<String>,
            set_name: Option<String>,
        ) -> Self {
            Privilege {
                _as: aerospike_core::Privilege::new(code.into(), namespace, set_name),
            }
        }

        #[getter]
        pub fn get_code(&self) -> PrivilegeCode {
            (&self._as.code).into()
        }

        #[getter]
        pub fn get_namespace(&self) -> Option<String> {
            self._as.namespace.clone()
        }

        #[getter]
        pub fn get_set_name(&self) -> Option<String> {
            self._as.set_name.clone()
        }

        fn as_string(&self) -> String {
            match (&self._as.namespace, &self._as.set_name) {
                (Some(ns), Some(set)) => format!("{}:{}.{}", self._as.code, ns, set),
                (Some(ns), None) => format!("{}:{}", self._as.code, ns),
                (None, _) => format!("{}", self._as.code),
            }
        }

        fn __str__(&self) -> PyResult<String> {
            Ok(self.as_string())
        }

        fn __repr__(&self) -> PyResult<String> {
            let s = self.__str__()?;
            Ok(format!("Privilege({})", s))
        }
    }


    impl fmt::Display for Privilege {
        fn fmt(&self, f: &mut fmt::Formatter) -> std::result::Result<(), fmt::Error> {
            write!(f, "{}", self.as_string())
        }
    }

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
            let c = aerospike_core::Client::new(&as_policy, &as_seeds)
                .await
                .map_err(|e| PyErr::from(RustClientError(e)))?;

            let res = Client {
                _as: Arc::new(RwLock::new(c)),
                seeds: seeds.clone(),
            };

            // Python::with_gil(|_py| Ok(res))
            Ok(res)
        })?
        .into())
    }

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(subclass, freelist = 1)]
    #[derive(Clone)]
    pub struct Client {
        _as: Arc<RwLock<aerospike_core::Client>>,
        seeds: String,
    }

    // Helper function to check if a key exists (internal use, shared by exists() and exists_legacy())
    impl Client {
        async fn exists_internal(
            client: std::sync::Arc<RwLock<aerospike_core::Client>>,
            policy: aerospike_core::ReadPolicy,
            key: aerospike_core::Key,
        ) -> Result<bool, Error> {
            client.read().await.exists(&policy, &key).await
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
                    .read()
                    .await
                    .close()
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;
                Ok(())
            })
        }

        /// Returns true if the client is connected to any cluster nodes.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[bool]", imports=("typing")))]
        pub fn is_connected<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();

            pyo3_asyncio::future_into_py(py, async move {
                Ok(client
                    .read()
                    .await
                    .is_connected()
                    .await)
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

            pyo3_asyncio::future_into_py(py, async move {
                client
                    .read()
                    .await
                    .put(&policy, &key, &bin_vec)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(())
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
            // Get the filter expression from the ReadPolicy
            let has_filter_expression = policy.get_filter_expression().is_some();

            // The filter expression should already be properly set in the base_policy
            let policy = policy._as.clone();
            let key = key._as.clone();
            let client = self._as.clone();

            pyo3_asyncio::future_into_py(py, async move {
                let res = client
                    .read()
                    .await
                    .get(&policy, &key, bins_flag(bins))
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                // Check if filter expression didn't match
                // When a filter expression doesn't match, Aerospike returns an empty record
                if res.bins.is_empty() && has_filter_expression {
                    return Err(PyException::new_err("Filter expression did not match any records"));
                }

                Ok(Record { _as: res })
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

            // Extract Operation objects from Python list
            // Support Operation, ListOperation, MapOperation, and BitOperation
            // Store both operation type and optional context
            struct OpWithCtx {
                op: OperationType,
                ctx: Option<Vec<aerospike_core::operations::cdt_context::CdtContext>>,
            }
            let mut rust_ops: Vec<OpWithCtx> = Vec::new();
            for op_obj in operations {
                Python::attach(|py| {
                    // Try to extract as each operation type
                    if let Ok(py_op) = op_obj.extract::<PyRef<Operation>>(py) {
                        rust_ops.push(OpWithCtx {
                            op: py_op.op.clone(),
                            ctx: None,
                        });
                    } else if let Ok(py_op) = op_obj.extract::<PyRef<ListOperation>>(py) {
                        let ctx = py_op.ctx.as_ref().map(|ctx_vec| {
                            ctx_vec.iter().map(|c| c.ctx.clone()).collect()
                        });
                        rust_ops.push(OpWithCtx {
                            op: py_op.op.clone(),
                            ctx,
                        });
                    } else if let Ok(py_op) = op_obj.extract::<PyRef<MapOperation>>(py) {
                        let ctx = py_op.ctx.as_ref().map(|ctx_vec| {
                            ctx_vec.iter().map(|c| c.ctx.clone()).collect()
                        });
                        rust_ops.push(OpWithCtx {
                            op: py_op.op.clone(),
                            ctx,
                        });
                    } else if let Ok(py_op) = op_obj.extract::<PyRef<BitOperation>>(py) {
                        rust_ops.push(OpWithCtx {
                            op: py_op.op.clone(),
                            ctx: None,
                        });
                    } else {
                        return Err(PyTypeError::new_err(
                            "Operation must be Operation, ListOperation, MapOperation, or BitOperation"
                        ));
                    }
                    Ok::<(), PyErr>(())
                })?;
            }

            // Move rust_ops into the async block for conversion
            pyo3_asyncio::future_into_py(py, async move {
                use aerospike_core::operations;

                // First pass: collect all bins/values that need to live as long as the operations
                let mut bin_storage: Vec<aerospike_core::Bin> = Vec::new();
                let mut value_storage: Vec<aerospike_core::Value> = Vec::new();
                let mut map_storage: Vec<HashMap<aerospike_core::Value, aerospike_core::Value>> = Vec::new();
                let mut list_storage: Vec<Vec<aerospike_core::Value>> = Vec::new();
                for op_with_ctx in &rust_ops {
                    match &op_with_ctx.op {
                        OperationType::Put(bin_name, value) |
                        OperationType::Add(bin_name, value) |
                        OperationType::Append(bin_name, value) |
                        OperationType::Prepend(bin_name, value) => {
                            let bin = aerospike_core::Bin::new(bin_name.clone(), value.clone().into());
                            bin_storage.push(bin);
                        }
                        OperationType::ListSet(_, _, value) => {
                            // Store the value for list_set operation
                            value_storage.push(value.clone().into());
                        }
                        OperationType::ListAppend(_, value, _) => {
                            // Store the value for list_append operation
                            value_storage.push(value.clone().into());
                        }
                        OperationType::ListAppendItems(_, values, _) => {
                            // Store all values for list_append_items operation
                            for value in values {
                                value_storage.push(value.clone().into());
                            }
                        }
                        OperationType::ListInsert(_, _, value, _) => {
                            // Store the value for list_insert operation
                            value_storage.push(value.clone().into());
                        }
                        OperationType::ListInsertItems(_, _, values, _) => {
                            // Store all values for list_insert_items operation
                            for value in values {
                                value_storage.push(value.clone().into());
                            }
                        }
                        OperationType::ListGetByValue(_, value, _) => {
                            // Store value for list get_by_value operation
                            value_storage.push(value.clone().into());
                        }
                        OperationType::ListGetByValueRange(_, begin, end, _) => {
                            // Store begin and end values for list get_by_value_range operation
                            value_storage.push(begin.clone().into());
                            value_storage.push(end.clone().into());
                        }
                        OperationType::ListGetByValueList(_, values, _) => {
                            // Store list of values for list get_by_value_list operation
                            let mut value_list = Vec::new();
                            for value in values {
                                value_list.push(value.clone().into());
                            }
                            list_storage.push(value_list);
                        }
                        OperationType::ListGetByValueRelativeRankRange(_, value, _, _, _) => {
                            // Store value for list get_by_value_relative_rank_range operation
                            value_storage.push(value.clone().into());
                        }
                        OperationType::ListRemoveByValue(_, value, _) => {
                            // Store value for list remove_by_value operation
                            value_storage.push(value.clone().into());
                        }
                        OperationType::ListRemoveByValueList(_, values, _) => {
                            // Store list of values for list remove_by_value_list operation
                            let mut value_list = Vec::new();
                            for value in values {
                                value_list.push(value.clone().into());
                            }
                            list_storage.push(value_list);
                        }
                        OperationType::ListRemoveByValueRange(_, begin, end, _) => {
                            // Store begin and end values for list remove_by_value_range operation
                            value_storage.push(begin.clone().into());
                            value_storage.push(end.clone().into());
                        }
                        OperationType::ListRemoveByValueRelativeRankRange(_, value, _, _, _) => {
                            // Store value for list remove_by_value_relative_rank_range operation
                            value_storage.push(value.clone().into());
                        }
                        OperationType::MapPut(_, key, value, _) => {
                            // Store key and value for map_put operation
                            value_storage.push(key.clone().into());
                            value_storage.push(value.clone().into());
                        }
                        OperationType::MapPutItems(_, items, _) => {
                            // Store all keys and values for map_put_items operation
                            use std::collections::HashMap;
                            let mut map = HashMap::new();
                            for (key, value) in items {
                                map.insert(key.clone().into(), value.clone().into());
                            }
                            map_storage.push(map);
                        }
                        OperationType::MapIncrementValue(_, key, value, _) | OperationType::MapDecrementValue(_, key, value, _) => {
                            // Store key and increment/decrement value for map increment/decrement operations
                            value_storage.push(key.clone().into());
                            value_storage.push(aerospike_core::Value::Int(*value));
                        }
                        OperationType::MapGetByKey(_, key, _) | OperationType::MapRemoveByKey(_, key, _) => {
                            // Store key for map get_by_key and remove_by_key operations
                            value_storage.push(key.clone().into());
                        }
                        OperationType::MapGetByKeyRange(_, begin, end, _) | OperationType::MapRemoveByKeyRange(_, begin, end, _) => {
                            // Store begin and end keys for map get_by_key_range and remove_by_key_range operations
                            value_storage.push(begin.clone().into());
                            value_storage.push(end.clone().into());
                        }
                        OperationType::MapGetByValue(_, value, _) | OperationType::MapRemoveByValue(_, value, _) => {
                            // Store value for map get_by_value and remove_by_value operations
                            value_storage.push(value.clone().into());
                        }
                        OperationType::MapGetByValueRange(_, begin, end, _) | OperationType::MapRemoveByValueRange(_, begin, end, _) => {
                            // Store begin and end values for map get_by_value_range and remove_by_value_range operations
                            value_storage.push(begin.clone().into());
                            value_storage.push(end.clone().into());
                        }
                        OperationType::MapGetByKeyList(_, keys, _) | OperationType::MapRemoveByKeyList(_, keys, _) => {
                            // Store list of keys for map get_by_key_list and remove_by_key_list operations
                            let mut key_list = Vec::new();
                            for key in keys {
                                key_list.push(key.clone().into());
                            }
                            list_storage.push(key_list);
                        }
                        OperationType::MapGetByValueList(_, values, _) | OperationType::MapRemoveByValueList(_, values, _) => {
                            // Store list of values for map get_by_value_list and remove_by_value_list operations
                            let mut value_list = Vec::new();
                            for value in values {
                                value_list.push(value.clone().into());
                            }
                            list_storage.push(value_list);
                        }
                        OperationType::MapGetByKeyRelativeIndexRange(_, key, _, _, _) | OperationType::MapRemoveByKeyRelativeIndexRange(_, key, _, _, _) => {
                            // Store key for map get_by_key_relative_index_range and remove_by_key_relative_index_range operations
                            value_storage.push(key.clone().into());
                        }
                        OperationType::MapGetByValueRelativeRankRange(_, value, _, _, _) | OperationType::MapRemoveByValueRelativeRankRange(_, value, _, _, _) => {
                            // Store value for map get_by_value_relative_rank_range and remove_by_value_relative_rank_range operations
                            value_storage.push(value.clone().into());
                        }
                        OperationType::BitInsert(_, _, value, _) | OperationType::BitSet(_, _, _, value, _) |
                        OperationType::BitOr(_, _, _, value, _) | OperationType::BitXor(_, _, _, value, _) |
                        OperationType::BitAnd(_, _, _, value, _) => {
                            // Store value for bit operations that require a value
                            value_storage.push(value.clone().into());
                        }
                        // Operations that don't require storage in first pass
                        OperationType::Get() | OperationType::GetBin(_) | OperationType::GetHeader() |
                        OperationType::Delete() | OperationType::Touch() |
                        OperationType::ListGet(_, _) | OperationType::ListSize(_) | OperationType::ListPop(_, _) |
                        OperationType::ListClear(_) | OperationType::ListGetRange(_, _, _) |
                        OperationType::ListRemove(_, _) | OperationType::ListRemoveRange(_, _, _) |
                        OperationType::ListGetRangeFrom(_, _) | OperationType::ListPopRange(_, _, _) |
                        OperationType::ListPopRangeFrom(_, _) | OperationType::ListRemoveRangeFrom(_, _) |
                        OperationType::ListTrim(_, _, _) | OperationType::ListIncrement(_, _, _, _) |
                        OperationType::ListSort(_, _) | OperationType::ListSetOrder(_, _) |
                        OperationType::ListGetByIndex(_, _, _) | OperationType::ListGetByIndexRange(_, _, _, _) |
                        OperationType::ListGetByRank(_, _, _) | OperationType::ListGetByRankRange(_, _, _, _) |
                        OperationType::ListRemoveByIndex(_, _, _) | OperationType::ListRemoveByIndexRange(_, _, _, _) |
                        OperationType::ListRemoveByRank(_, _, _) | OperationType::ListRemoveByRankRange(_, _, _, _) |
                        OperationType::ListCreate(_, _, _, _) |
                        OperationType::MapSize(_) | OperationType::MapClear(_) |
                        OperationType::MapGetByIndex(_, _, _) | OperationType::MapRemoveByIndex(_, _, _) |
                        OperationType::MapGetByIndexRange(_, _, _, _) | OperationType::MapRemoveByIndexRange(_, _, _, _) |
                        OperationType::MapGetByIndexRangeFrom(_, _, _) | OperationType::MapRemoveByIndexRangeFrom(_, _, _) |
                        OperationType::MapGetByRank(_, _, _) | OperationType::MapRemoveByRank(_, _, _) |
                        OperationType::MapGetByRankRange(_, _, _, _) | OperationType::MapRemoveByRankRange(_, _, _, _) |
                        OperationType::MapGetByRankRangeFrom(_, _, _) | OperationType::MapRemoveByRankRangeFrom(_, _, _) |
                        OperationType::MapSetMapPolicy(_, _) | OperationType::MapCreate(_, _) |
                        OperationType::BitResize(_, _, _, _) | OperationType::BitRemove(_, _, _, _) |
                        OperationType::BitNot(_, _, _, _) | OperationType::BitLShift(_, _, _, _, _) |
                        OperationType::BitRShift(_, _, _, _, _) | OperationType::BitAdd(_, _, _, _, _, _, _) |
                        OperationType::BitSubtract(_, _, _, _, _, _, _) | OperationType::BitSetInt(_, _, _, _, _) |
                        OperationType::BitGet(_, _, _) | OperationType::BitCount(_, _, _) |
                        OperationType::BitLScan(_, _, _, _) | OperationType::BitRScan(_, _, _, _) |
                        OperationType::BitGetInt(_, _, _, _) => {
                        }
                    }
                }

                // Second pass: convert operations, using references to stored bins/values
                let mut bin_idx = 0;
                let mut value_idx = 0;
                let mut map_idx = 0;
                let mut list_idx = 0;
                let mut core_ops: Vec<operations::Operation> = Vec::new();

                for op_with_ctx in &rust_ops {
                    let core_op = match &op_with_ctx.op {
                        OperationType::Get() => {
                            // Use the operations module's get() function to create a Get operation
                            operations::get()
                        }
                        OperationType::GetBin(bin_name) => {
                            // Use the operations module's get_bin() function to get a specific bin
                            operations::get_bin(bin_name)
                        }
                        OperationType::GetHeader() => {
                            // Use the operations module's get_header() function
                            operations::get_header()
                        }
                        OperationType::Put(_, _) => {
                            // Use a reference to the stored bin
                            let op = operations::put(&bin_storage[bin_idx]);
                            bin_idx += 1;
                            op
                        }
                        OperationType::Add(_, _) => {
                            // Use a reference to the stored bin
                            let op = operations::add(&bin_storage[bin_idx]);
                            bin_idx += 1;
                            op
                        }
                        OperationType::Append(_, _) => {
                            // Use a reference to the stored bin
                            let op = operations::append(&bin_storage[bin_idx]);
                            bin_idx += 1;
                            op
                        }
                        OperationType::Prepend(_, _) => {
                            // Use a reference to the stored bin
                            let op = operations::prepend(&bin_storage[bin_idx]);
                            bin_idx += 1;
                            op
                        }
                        OperationType::Delete() => {
                            // Use the operations module's delete() function
                            operations::delete()
                        }
                        OperationType::Touch() => {
                            // Use the operations module's touch() function
                            operations::touch()
                        }
                        OperationType::ListGet(bin_name, index) => {
                            // Use the operations module's list get() function
                            use aerospike_core::operations::lists;
                            lists::get(bin_name, *index)
                        }
                        OperationType::ListSize(bin_name) => {
                            // Use the operations module's list size() function
                            use aerospike_core::operations::lists;
                            lists::size(bin_name)
                        }
                        OperationType::ListPop(bin_name, index) => {
                            // Use the operations module's list pop() function
                            use aerospike_core::operations::lists;
                            lists::pop(bin_name, *index)
                        }
                        OperationType::ListClear(bin_name) => {
                            // Use the operations module's list clear() function
                            use aerospike_core::operations::lists;
                            lists::clear(bin_name)
                        }
                        OperationType::ListGetRange(bin_name, index, count) => {
                            // Use the operations module's list get_range() function
                            use aerospike_core::operations::lists;
                            lists::get_range(bin_name, *index, *count)
                        }
                        OperationType::ListSet(bin_name, index, _) => {
                            // Use the operations module's list set() function with stored value
                            use aerospike_core::operations::lists;
                            let op = lists::set(bin_name, *index, &value_storage[value_idx]);
                            value_idx += 1;
                            op
                        }
                        OperationType::ListRemove(bin_name, index) => {
                            // Use the operations module's list remove() function
                            use aerospike_core::operations::lists;
                            lists::remove(bin_name, *index)
                        }
                        OperationType::ListRemoveRange(bin_name, index, count) => {
                            // Use the operations module's list remove_range() function
                            use aerospike_core::operations::lists;
                            lists::remove_range(bin_name, *index, *count)
                        }
                        OperationType::ListGetRangeFrom(bin_name, index) => {
                            // Use the operations module's list get_range_from() function
                            use aerospike_core::operations::lists;
                            lists::get_range_from(bin_name, *index)
                        }
                        OperationType::ListPopRange(bin_name, index, count) => {
                            // Use the operations module's list pop_range() function
                            use aerospike_core::operations::lists;
                            lists::pop_range(bin_name, *index, *count)
                        }
                        OperationType::ListPopRangeFrom(bin_name, index) => {
                            // Use the operations module's list pop_range_from() function
                            use aerospike_core::operations::lists;
                            lists::pop_range_from(bin_name, *index)
                        }
                        OperationType::ListRemoveRangeFrom(bin_name, index) => {
                            // Use the operations module's list remove_range_from() function
                            use aerospike_core::operations::lists;
                            lists::remove_range_from(bin_name, *index)
                        }
                        OperationType::ListTrim(bin_name, index, count) => {
                            // Use the operations module's list trim() function
                            use aerospike_core::operations::lists;
                            lists::trim(bin_name, *index, *count)
                        }
                        OperationType::ListAppend(bin_name, _, policy) => {
                            // Use the operations module's list append() function with stored value and policy
                            use aerospike_core::operations::lists;
                            let op = lists::append(&policy._as, bin_name, &value_storage[value_idx]);
                            value_idx += 1;
                            op
                        }
                        OperationType::ListAppendItems(bin_name, values, policy) => {
                            // Use the operations module's list append_items() function with stored values and policy
                            use aerospike_core::operations::lists;
                            let values_slice: &[aerospike_core::Value] = &value_storage[value_idx..value_idx + values.len()];
                            let op = lists::append_items(&policy._as, bin_name, values_slice);
                            value_idx += values.len();
                            op
                        }
                        OperationType::ListInsert(bin_name, index, _, policy) => {
                            // Use the operations module's list insert() function with stored value and policy
                            use aerospike_core::operations::lists;
                            let op = lists::insert(&policy._as, bin_name, *index, &value_storage[value_idx]);
                            value_idx += 1;
                            op
                        }
                        OperationType::ListInsertItems(bin_name, index, values, policy) => {
                            // Use the operations module's list insert_items() function with stored values and policy
                            use aerospike_core::operations::lists;
                            let values_slice: &[aerospike_core::Value] = &value_storage[value_idx..value_idx + values.len()];
                            let op = lists::insert_items(&policy._as, bin_name, *index, values_slice);
                            value_idx += values.len();
                            op
                        }
                        OperationType::ListIncrement(bin_name, index, value, policy) => {
                            // Use the operations module's list increment() function with policy
                            use aerospike_core::operations::lists;
                            lists::increment(&policy._as, bin_name, *index, *value)
                        }
                        OperationType::ListSort(bin_name, flags) => {
                            // Use the operations module's list sort() function
                            use aerospike_core::operations::lists;
                            let core_flags: aerospike_core::operations::lists::ListSortFlags = flags.into();
                            lists::sort(bin_name, core_flags)
                        }
                        OperationType::ListSetOrder(bin_name, order) => {
                            // Use the operations module's list set_order() function
                            use aerospike_core::operations::lists;
                            let core_order: aerospike_core::operations::lists::ListOrderType = order.into();
                            lists::set_order(bin_name, core_order, &[])
                        }
                        OperationType::ListGetByIndex(bin_name, index, return_type) => {
                            // Use the operations module's list get_by_index() function with return type
                            use aerospike_core::operations::lists;
                            let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                            lists::get_by_index(bin_name, *index, core_return_type)
                        }
                        OperationType::ListGetByIndexRange(bin_name, index, count, return_type) => {
                            // Use the operations module's list get_by_index_range() or get_by_index_range_count() function
                            use aerospike_core::operations::lists;
                            let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                            match count {
                                Some(c) => lists::get_by_index_range_count(bin_name, *index, *c, core_return_type),
                                None => lists::get_by_index_range(bin_name, *index, core_return_type),
                            }
                        }
                        OperationType::ListGetByRank(bin_name, rank, return_type) => {
                            // Use the operations module's list get_by_rank() function with return type
                            use aerospike_core::operations::lists;
                            let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                            lists::get_by_rank(bin_name, *rank, core_return_type)
                        }
                        OperationType::ListGetByRankRange(bin_name, rank, count, return_type) => {
                            // Use the operations module's list get_by_rank_range() or get_by_rank_range_count() function
                            use aerospike_core::operations::lists;
                            let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                            match count {
                                Some(c) => lists::get_by_rank_range_count(bin_name, *rank, *c, core_return_type),
                                None => lists::get_by_rank_range(bin_name, *rank, core_return_type),
                            }
                        }
                        OperationType::ListGetByValue(bin_name, _, return_type) => {
                            // Use the operations module's list get_by_value() function with stored value and return type
                            use aerospike_core::operations::lists;
                            let value = &value_storage[value_idx];
                            let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                            let op = lists::get_by_value(bin_name, value, core_return_type);
                            value_idx += 1;
                            op
                        }
                        OperationType::ListGetByValueRange(bin_name, _, _, return_type) => {
                            // Use the operations module's list get_by_value_range() function with stored values and return type
                            use aerospike_core::operations::lists;
                            let begin = &value_storage[value_idx];
                            let end = &value_storage[value_idx + 1];
                            let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                            let op = lists::get_by_value_range(bin_name, begin, end, core_return_type);
                            value_idx += 2;
                            op
                        }
                        OperationType::ListGetByValueList(bin_name, _, return_type) => {
                            // Use the operations module's list get_by_value_list() function with stored list and return type
                            use aerospike_core::operations::lists;
                            let values = &list_storage[list_idx];
                            let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                            let op = lists::get_by_value_list(bin_name, values, core_return_type);
                            list_idx += 1;
                            op
                        }
                        OperationType::ListGetByValueRelativeRankRange(bin_name, _, rank, count, return_type) => {
                            // Use the operations module's list get_by_value_relative_rank_range() function
                            use aerospike_core::operations::lists;
                            let value = &value_storage[value_idx];
                            let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                            let op = match count {
                                Some(c) => lists::get_by_value_relative_rank_range_count(bin_name, value, *rank, *c, core_return_type),
                                None => lists::get_by_value_relative_rank_range(bin_name, value, *rank, core_return_type),
                            };
                            value_idx += 1;
                            op
                        }
                        OperationType::ListRemoveByIndex(bin_name, index, return_type) => {
                            // Use the operations module's list remove_by_index() function with return type
                            use aerospike_core::operations::lists;
                            let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                            lists::remove_by_index(bin_name, *index, core_return_type)
                        }
                        OperationType::ListRemoveByIndexRange(bin_name, index, count, return_type) => {
                            // Use the operations module's list remove_by_index_range() or remove_by_index_range_count() function
                            use aerospike_core::operations::lists;
                            let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                            match count {
                                Some(c) => lists::remove_by_index_range_count(bin_name, *index, *c, core_return_type),
                                None => lists::remove_by_index_range(bin_name, *index, core_return_type),
                            }
                        }
                        OperationType::ListRemoveByRank(bin_name, rank, return_type) => {
                            // Use the operations module's list remove_by_rank() function with return type
                            use aerospike_core::operations::lists;
                            let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                            lists::remove_by_rank(bin_name, *rank, core_return_type)
                        }
                        OperationType::ListRemoveByRankRange(bin_name, rank, count, return_type) => {
                            // Use the operations module's list remove_by_rank_range() or remove_by_rank_range_count() function
                            use aerospike_core::operations::lists;
                            let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                            match count {
                                Some(c) => lists::remove_by_rank_range_count(bin_name, *rank, *c, core_return_type),
                                None => lists::remove_by_rank_range(bin_name, *rank, core_return_type),
                            }
                        }
                        OperationType::ListRemoveByValue(bin_name, _, return_type) => {
                            // Use the operations module's list remove_by_value() function with stored value and return type
                            use aerospike_core::operations::lists;
                            let value = &value_storage[value_idx];
                            let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                            let op = lists::remove_by_value(bin_name, value, core_return_type);
                            value_idx += 1;
                            op
                        }
                        OperationType::ListRemoveByValueList(bin_name, _, return_type) => {
                            // Use the operations module's list remove_by_value_list() function with stored list and return type
                            use aerospike_core::operations::lists;
                            let values = &list_storage[list_idx];
                            let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                            let op = lists::remove_by_value_list(bin_name, values, core_return_type);
                            list_idx += 1;
                            op
                        }
                        OperationType::ListRemoveByValueRange(bin_name, _, _, return_type) => {
                            // Use the operations module's list remove_by_value_range() function with stored values and return type
                            // Note: parameter order is (bin, return_type, begin, end)
                            use aerospike_core::operations::lists;
                            let begin = &value_storage[value_idx];
                            let end = &value_storage[value_idx + 1];
                            let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                            let op = lists::remove_by_value_range(bin_name, core_return_type, begin, end);
                            value_idx += 2;
                            op
                        }
                        OperationType::ListRemoveByValueRelativeRankRange(bin_name, _, rank, count, return_type) => {
                            // Use the operations module's list remove_by_value_relative_rank_range() function
                            // Note: parameter order is (bin, return_type, value, rank) for no-count version
                            use aerospike_core::operations::lists;
                            let value = &value_storage[value_idx];
                            let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                            let op = match count {
                                Some(c) => lists::remove_by_value_relative_rank_range_count(bin_name, core_return_type, value, *rank, *c),
                                None => lists::remove_by_value_relative_rank_range(bin_name, core_return_type, value, *rank),
                            };
                            value_idx += 1;
                            op
                        }
                        OperationType::ListCreate(bin_name, order, pad, _persist_index) => {
                            // Use the operations module's list create() function
                            // Note: Rust core client doesn't support persist_index parameter
                            use aerospike_core::operations::lists;
                            let core_order: aerospike_core::operations::lists::ListOrderType = order.into();
                            lists::create(bin_name, core_order, *pad)
                        }
                        OperationType::MapSize(bin_name) => {
                            // Use the operations module's map size() function
                            use aerospike_core::operations::maps;
                            maps::size(bin_name)
                        }
                        OperationType::MapClear(bin_name) => {
                            // Use the operations module's map clear() function
                            use aerospike_core::operations::maps;
                            maps::clear(bin_name)
                        }
                        OperationType::MapPut(bin_name, _, _, policy) => {
                            // Use the operations module's map put() function with stored key, value, and policy
                            use aerospike_core::operations::maps;
                            let key = &value_storage[value_idx];
                            let value = &value_storage[value_idx + 1];
                            let op = maps::put(&policy._as, bin_name, key, value);
                            value_idx += 2;
                            op
                        }
                        OperationType::MapPutItems(bin_name, _, policy) => {
                            // Use the operations module's map put_items() function with stored items and policy
                            use aerospike_core::operations::maps;
                            let op = maps::put_items(&policy._as, bin_name, &map_storage[map_idx]);
                            map_idx += 1;
                            op
                        }
                        OperationType::MapIncrementValue(bin_name, _, _value, policy) => {
                            // Use the operations module's map increment_value() function with stored key, value, and policy
                            use aerospike_core::operations::maps;
                            let key = &value_storage[value_idx];
                            let incr_value = &value_storage[value_idx + 1];
                            let op = maps::increment_value(&policy._as, bin_name, key, incr_value);
                            value_idx += 2;
                            op
                        }
                        OperationType::MapDecrementValue(bin_name, _, _value, policy) => {
                            // Use the operations module's map decrement_value() function with stored key, value, and policy
                            use aerospike_core::operations::maps;
                            let key = &value_storage[value_idx];
                            let decr_value = &value_storage[value_idx + 1];
                            let op = maps::decrement_value(&policy._as, bin_name, key, decr_value);
                            value_idx += 2;
                            op
                        }
                        OperationType::MapGetByKey(bin_name, _, return_type) => {
                            // Use the operations module's map get_by_key() function with stored key and return type
                            use aerospike_core::operations::maps;
                            let key = &value_storage[value_idx];
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            let op = maps::get_by_key(bin_name, key, core_return_type);
                            value_idx += 1;
                            op
                        }
                        OperationType::MapRemoveByKey(bin_name, _, return_type) => {
                            // Use the operations module's map remove_by_key() function with stored key and return type
                            use aerospike_core::operations::maps;
                            let key = &value_storage[value_idx];
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            let op = maps::remove_by_key(bin_name, key, core_return_type);
                            value_idx += 1;
                            op
                        }
                        OperationType::MapGetByKeyRange(bin_name, _, _, return_type) => {
                            // Use the operations module's map get_by_key_range() function with stored keys and return type
                            use aerospike_core::operations::maps;
                            let begin = &value_storage[value_idx];
                            let end = &value_storage[value_idx + 1];
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            let op = maps::get_by_key_range(bin_name, begin, end, core_return_type);
                            value_idx += 2;
                            op
                        }
                        OperationType::MapRemoveByKeyRange(bin_name, _, _, return_type) => {
                            // Use the operations module's map remove_by_key_range() function with stored keys and return type
                            use aerospike_core::operations::maps;
                            let begin = &value_storage[value_idx];
                            let end = &value_storage[value_idx + 1];
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            let op = maps::remove_by_key_range(bin_name, begin, end, core_return_type);
                            value_idx += 2;
                            op
                        }
                        OperationType::MapGetByIndex(bin_name, index, return_type) => {
                            // Use the operations module's map get_by_index() function with return type
                            use aerospike_core::operations::maps;
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            maps::get_by_index(bin_name, *index, core_return_type)
                        }
                        OperationType::MapRemoveByIndex(bin_name, index, return_type) => {
                            // Use the operations module's map remove_by_index() function with return type
                            use aerospike_core::operations::maps;
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            maps::remove_by_index(bin_name, *index, core_return_type)
                        }
                        OperationType::MapGetByIndexRange(bin_name, index, count, return_type) => {
                            // Use the operations module's map get_by_index_range() function with return type
                            use aerospike_core::operations::maps;
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            maps::get_by_index_range(bin_name, *index, *count, core_return_type)
                        }
                        OperationType::MapRemoveByIndexRange(bin_name, index, count, return_type) => {
                            // Use the operations module's map remove_by_index_range() function with return type
                            use aerospike_core::operations::maps;
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            maps::remove_by_index_range(bin_name, *index, *count, core_return_type)
                        }
                        OperationType::MapGetByIndexRangeFrom(bin_name, index, return_type) => {
                            // Use the operations module's map get_by_index_range_from() function with return type
                            use aerospike_core::operations::maps;
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            maps::get_by_index_range_from(bin_name, *index, core_return_type)
                        }
                        OperationType::MapRemoveByIndexRangeFrom(bin_name, index, return_type) => {
                            // Use the operations module's map remove_by_index_range_from() function with return type
                            use aerospike_core::operations::maps;
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            maps::remove_by_index_range_from(bin_name, *index, core_return_type)
                        }
                        OperationType::MapGetByRank(bin_name, rank, return_type) => {
                            // Use the operations module's map get_by_rank() function with return type
                            use aerospike_core::operations::maps;
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            maps::get_by_rank(bin_name, *rank, core_return_type)
                        }
                        OperationType::MapRemoveByRank(bin_name, rank, return_type) => {
                            // Use the operations module's map remove_by_rank() function with return type
                            use aerospike_core::operations::maps;
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            maps::remove_by_rank(bin_name, *rank, core_return_type)
                        }
                        OperationType::MapGetByRankRange(bin_name, rank, count, return_type) => {
                            // Use the operations module's map get_by_rank_range() function with return type
                            use aerospike_core::operations::maps;
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            maps::get_by_rank_range(bin_name, *rank, *count, core_return_type)
                        }
                        OperationType::MapRemoveByRankRange(bin_name, rank, count, return_type) => {
                            // Use the operations module's map remove_by_rank_range() function with return type
                            use aerospike_core::operations::maps;
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            maps::remove_by_rank_range(bin_name, *rank, *count, core_return_type)
                        }
                        OperationType::MapGetByRankRangeFrom(bin_name, rank, return_type) => {
                            // Use the operations module's map get_by_rank_range_from() function with return type
                            use aerospike_core::operations::maps;
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            maps::get_by_rank_range_from(bin_name, *rank, core_return_type)
                        }
                        OperationType::MapRemoveByRankRangeFrom(bin_name, rank, return_type) => {
                            // Use the operations module's map remove_by_rank_range_from() function with return type
                            use aerospike_core::operations::maps;
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            maps::remove_by_rank_range_from(bin_name, *rank, core_return_type)
                        }
                        OperationType::MapGetByValue(bin_name, _, return_type) => {
                            // Use the operations module's map get_by_value() function with stored value and return type
                            use aerospike_core::operations::maps;
                            let value = &value_storage[value_idx];
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            let op = maps::get_by_value(bin_name, value, core_return_type);
                            value_idx += 1;
                            op
                        }
                        OperationType::MapRemoveByValue(bin_name, _, return_type) => {
                            // Use the operations module's map remove_by_value() function with stored value and return type
                            use aerospike_core::operations::maps;
                            let value = &value_storage[value_idx];
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            let op = maps::remove_by_value(bin_name, value, core_return_type);
                            value_idx += 1;
                            op
                        }
                        OperationType::MapGetByValueRange(bin_name, _, _, return_type) => {
                            // Use the operations module's map get_by_value_range() function with stored values and return type
                            use aerospike_core::operations::maps;
                            let begin = &value_storage[value_idx];
                            let end = &value_storage[value_idx + 1];
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            let op = maps::get_by_value_range(bin_name, begin, end, core_return_type);
                            value_idx += 2;
                            op
                        }
                        OperationType::MapRemoveByValueRange(bin_name, _, _, return_type) => {
                            // Use the operations module's map remove_by_value_range() function with stored values and return type
                            use aerospike_core::operations::maps;
                            let begin = &value_storage[value_idx];
                            let end = &value_storage[value_idx + 1];
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            let op = maps::remove_by_value_range(bin_name, begin, end, core_return_type);
                            value_idx += 2;
                            op
                        }
                        OperationType::MapGetByKeyList(bin_name, _, return_type) => {
                            // Use the operations module's map get_by_key_list() function with stored key list and return type
                            use aerospike_core::operations::maps;
                            let keys = &list_storage[list_idx];
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            let op = maps::get_by_key_list(bin_name, keys, core_return_type);
                            list_idx += 1;
                            op
                        }
                        OperationType::MapRemoveByKeyList(bin_name, _, return_type) => {
                            // Use the operations module's map remove_by_key_list() function with stored key list and return type
                            use aerospike_core::operations::maps;
                            let keys = &list_storage[list_idx];
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            let op = maps::remove_by_key_list(bin_name, keys, core_return_type);
                            list_idx += 1;
                            op
                        }
                        OperationType::MapGetByValueList(bin_name, _, return_type) => {
                            // Use the operations module's map get_by_value_list() function with stored value list and return type
                            use aerospike_core::operations::maps;
                            let values = &list_storage[list_idx];
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            let op = maps::get_by_value_list(bin_name, values, core_return_type);
                            list_idx += 1;
                            op
                        }
                        OperationType::MapRemoveByValueList(bin_name, _, return_type) => {
                            // Use the operations module's map remove_by_value_list() function with stored value list and return type
                            use aerospike_core::operations::maps;
                            let values = &list_storage[list_idx];
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            let op = maps::remove_by_value_list(bin_name, values, core_return_type);
                            list_idx += 1;
                            op
                        }
                        OperationType::MapSetMapPolicy(bin_name, policy) => {
                            // Use the operations module's map set_order() function
                            // Note: Rust core client only has set_order, not full setMapPolicy
                            // This sets the map order from the policy
                            use aerospike_core::operations::maps;
                            let core_order = policy._as.order;
                            maps::set_order(bin_name, core_order)
                        }
                        OperationType::MapGetByKeyRelativeIndexRange(bin_name, _, index, count, return_type) => {
                            // Use the operations module's map get_by_key_relative_index_range() function
                            use aerospike_core::operations::maps;
                            let key = &value_storage[value_idx];
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            let op = match count {
                                Some(c) => maps::get_by_key_relative_index_range_count(bin_name, key, *index, *c, core_return_type),
                                None => maps::get_by_key_relative_index_range(bin_name, key, *index, core_return_type),
                            };
                            value_idx += 1;
                            op
                        }
                        OperationType::MapGetByValueRelativeRankRange(bin_name, _, rank, count, return_type) => {
                            // Use the operations module's map get_by_value_relative_rank_range() function
                            use aerospike_core::operations::maps;
                            let value = &value_storage[value_idx];
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            let op = match count {
                                Some(c) => maps::get_by_value_relative_rank_range_count(bin_name, value, *rank, *c, core_return_type),
                                None => maps::get_by_value_relative_rank_range(bin_name, value, *rank, core_return_type),
                            };
                            value_idx += 1;
                            op
                        }
                        OperationType::MapRemoveByKeyRelativeIndexRange(bin_name, _, index, count, return_type) => {
                            // Use the operations module's map remove_by_key_relative_index_range() function
                            use aerospike_core::operations::maps;
                            let key = &value_storage[value_idx];
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            let op = match count {
                                Some(c) => maps::remove_by_key_relative_index_range_count(bin_name, key, *index, *c, core_return_type),
                                None => maps::remove_by_key_relative_index_range(bin_name, key, *index, core_return_type),
                            };
                            value_idx += 1;
                            op
                        }
                        OperationType::MapRemoveByValueRelativeRankRange(bin_name, _, rank, count, return_type) => {
                            // Use the operations module's map remove_by_value_relative_rank_range() function
                            use aerospike_core::operations::maps;
                            let value = &value_storage[value_idx];
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            let op = match count {
                                Some(c) => maps::remove_by_value_relative_rank_range_count(bin_name, value, *rank, *c, core_return_type),
                                None => maps::remove_by_value_relative_rank_range(bin_name, value, *rank, core_return_type),
                            };
                            value_idx += 1;
                            op
                        }
                        OperationType::MapCreate(bin_name, order) => {
                            // Use the operations module's map set_order() function
                            // Note: Rust core client uses set_order instead of create for maps
                            use aerospike_core::operations::maps;
                            let core_order: aerospike_core::operations::maps::MapOrder = order.into();
                            maps::set_order(bin_name, core_order)
                        }
                        OperationType::BitResize(bin_name, byte_size, resize_flags, policy) => {
                            use aerospike_core::operations::bitwise;
                            let flags = resize_flags.map(|f| f.into());
                            bitwise::resize(bin_name, *byte_size, flags, &policy._as)
                        }
                        OperationType::BitInsert(bin_name, byte_offset, _, policy) => {
                            use aerospike_core::operations::bitwise;
                            let value = &value_storage[value_idx];
                            let op = bitwise::insert(bin_name, *byte_offset, value, &policy._as);
                            value_idx += 1;
                            op
                        }
                        OperationType::BitRemove(bin_name, byte_offset, byte_size, policy) => {
                            use aerospike_core::operations::bitwise;
                            bitwise::remove(bin_name, *byte_offset, *byte_size, &policy._as)
                        }
                        OperationType::BitSet(bin_name, bit_offset, bit_size, _, policy) => {
                            use aerospike_core::operations::bitwise;
                            let value = &value_storage[value_idx];
                            let op = bitwise::set(bin_name, *bit_offset, *bit_size, value, &policy._as);
                            value_idx += 1;
                            op
                        }
                        OperationType::BitOr(bin_name, bit_offset, bit_size, _, policy) => {
                            use aerospike_core::operations::bitwise;
                            let value = &value_storage[value_idx];
                            let op = bitwise::or(bin_name, *bit_offset, *bit_size, value, &policy._as);
                            value_idx += 1;
                            op
                        }
                        OperationType::BitXor(bin_name, bit_offset, bit_size, _, policy) => {
                            use aerospike_core::operations::bitwise;
                            let value = &value_storage[value_idx];
                            let op = bitwise::xor(bin_name, *bit_offset, *bit_size, value, &policy._as);
                            value_idx += 1;
                            op
                        }
                        OperationType::BitAnd(bin_name, bit_offset, bit_size, _, policy) => {
                            use aerospike_core::operations::bitwise;
                            let value = &value_storage[value_idx];
                            let op = bitwise::and(bin_name, *bit_offset, *bit_size, value, &policy._as);
                            value_idx += 1;
                            op
                        }
                        OperationType::BitNot(bin_name, bit_offset, bit_size, policy) => {
                            use aerospike_core::operations::bitwise;
                            bitwise::not(bin_name, *bit_offset, *bit_size, &policy._as)
                        }
                        OperationType::BitLShift(bin_name, bit_offset, bit_size, shift, policy) => {
                            use aerospike_core::operations::bitwise;
                            bitwise::lshift(bin_name, *bit_offset, *bit_size, *shift, &policy._as)
                        }
                        OperationType::BitRShift(bin_name, bit_offset, bit_size, shift, policy) => {
                            use aerospike_core::operations::bitwise;
                            bitwise::rshift(bin_name, *bit_offset, *bit_size, *shift, &policy._as)
                        }
                        OperationType::BitAdd(bin_name, bit_offset, bit_size, value, signed, action, policy) => {
                            use aerospike_core::operations::bitwise;
                            let core_action: aerospike_core::operations::bitwise::BitwiseOverflowActions = (*action).into();
                            bitwise::add(bin_name, *bit_offset, *bit_size, *value, *signed, core_action, &policy._as)
                        }
                        OperationType::BitSubtract(bin_name, bit_offset, bit_size, value, signed, action, policy) => {
                            use aerospike_core::operations::bitwise;
                            let core_action: aerospike_core::operations::bitwise::BitwiseOverflowActions = (*action).into();
                            bitwise::subtract(bin_name, *bit_offset, *bit_size, *value, *signed, core_action, &policy._as)
                        }
                        OperationType::BitSetInt(bin_name, bit_offset, bit_size, value, policy) => {
                            use aerospike_core::operations::bitwise;
                            bitwise::set_int(bin_name, *bit_offset, *bit_size, *value, &policy._as)
                        }
                        OperationType::BitGet(bin_name, bit_offset, bit_size) => {
                            use aerospike_core::operations::bitwise;
                            bitwise::get(bin_name, *bit_offset, *bit_size)
                        }
                        OperationType::BitCount(bin_name, bit_offset, bit_size) => {
                            use aerospike_core::operations::bitwise;
                            bitwise::count(bin_name, *bit_offset, *bit_size)
                        }
                        OperationType::BitLScan(bin_name, bit_offset, bit_size, value) => {
                            use aerospike_core::operations::bitwise;
                            bitwise::lscan(bin_name, *bit_offset, *bit_size, *value)
                        }
                        OperationType::BitRScan(bin_name, bit_offset, bit_size, value) => {
                            use aerospike_core::operations::bitwise;
                            bitwise::rscan(bin_name, *bit_offset, *bit_size, *value)
                        }
                        OperationType::BitGetInt(bin_name, bit_offset, bit_size, signed) => {
                            use aerospike_core::operations::bitwise;
                            bitwise::get_int(bin_name, *bit_offset, *bit_size, *signed)
                        }
                    };
                    
                    // Apply context if present
                    let final_op = if let Some(ctx) = &op_with_ctx.ctx {
                        core_op.set_context(ctx.as_slice())
                    } else {
                        core_op
                    };
                    core_ops.push(final_op);
                }

                // Execute the operations
                let res = client
                    .read()
                    .await
                    .operate(&policy, &key, &core_ops)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(Record { _as: res })
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

            pyo3_asyncio::future_into_py(py, async move {
                client
                    .read()
                    .await
                    .add(&policy, &key, &bins)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|py| Ok(py.None()))
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

            pyo3_asyncio::future_into_py(py, async move {
                client
                    .read()
                    .await
                    .append(&policy, &key, &bins)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|py| Ok(py.None()))
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

            pyo3_asyncio::future_into_py(py, async move {
                client
                    .read()
                    .await
                    .prepend(&policy, &key, &bins)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|py| Ok(py.None()))
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

            pyo3_asyncio::future_into_py(py, async move {
                let res = client
                    .read()
                    .await
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

            pyo3_asyncio::future_into_py(py, async move {
                client
                    .read()
                    .await
                    .touch(&policy, &key)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|py| Ok(py.None()))
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

            pyo3_asyncio::future_into_py(py, async move {
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
                        .read()
                        .await
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
                    .read()
                    .await
                    .truncate(&admin_policy, &namespace, &set_name, before_nanos)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|py| Ok(py.None()))
            })
        }

        /// Create a secondary index on a bin containing scalar values. This asynchronous server call
        /// returns before the command is complete.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        #[pyo3(signature = (namespace, set_name, bin_name, index_name, index_type, cit = None, *, policy = None))]
        pub fn create_index<'a>(
            &self,
            namespace: String,
            set_name: String,
            bin_name: String,
            index_name: String,
            index_type: IndexType,
            cit: Option<CollectionIndexType>,
            policy: Option<AdminPolicy>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();
            let admin_policy = policy.map(|p| p._as).unwrap_or_else(|| aerospike_core::AdminPolicy::default());

            let cit = (&cit.unwrap_or(CollectionIndexType::Default)).into();
            let index_type = (&index_type).into();

            pyo3_asyncio::future_into_py(py, async move {
                client
                    .read()
                    .await
                    .create_index_on_bin(
                        &admin_policy,
                        &namespace,
                        &set_name,
                        &bin_name,
                        &index_name,
                        index_type,
                        cit,
                        None,
                    )
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|py| Ok(py.None()))
            })
        }

        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
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
                client
                    .read()
                    .await
                    .drop_index(&admin_policy, &namespace, &set_name, &index_name)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|py| Ok(py.None()))
            })
        }

        /// Read all records in the specified namespace and set and return a record iterator. The scan
        /// executor puts records on a queue in separate threads. The calling thread concurrently pops
        /// records off the queue through the record iterator. Up to `policy.max_concurrent_nodes`
        /// nodes are scanned in parallel. If concurrent nodes is set to zero, the server nodes are
        /// read in series.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        pub fn scan<'a>(
            &self,
            policy: &ScanPolicy,
            partition_filter: PartitionFilter,
            namespace: String,
            set_name: String,
            bins: Option<Vec<String>>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let policy = policy._as.clone();
            let client = self._as.clone();

            pyo3_asyncio::future_into_py(py, async move {
                let res = client
                    .read()
                    .await
                    .scan(
                        &policy,
                        partition_filter._as,
                        &namespace,
                        &set_name,
                        bins_flag(bins),
                    )
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(Recordset {
                    _as: res,
                    _stream: Arc::new(Mutex::new(None)),
                })
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
                    .read()
                    .await
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
                    .read()
                    .await
                    .create_user(&admin_policy, &user, &password, &roles)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|py| Ok(py.None()))
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
                    .read()
                    .await
                    .drop_user(&admin_policy, &user)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|py| Ok(py.None()))
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
                    .read()
                    .await
                    .change_password(&admin_policy, &user, &password)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|py| Ok(py.None()))
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
                    .read()
                    .await
                    .grant_roles(&admin_policy, &user, &roles)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|py| Ok(py.None()))
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
                    .read()
                    .await
                    .revoke_roles(&admin_policy, &user, &roles)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|py| Ok(py.None()))
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
                    .read()
                    .await
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
                    .read()
                    .await
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
                    .read()
                    .await
                    .create_role(&admin_policy, &role_name, &privileges, &allowlist, read_quota, write_quota)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|py| Ok(py.None()))
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
                    .read()
                    .await
                    .drop_role(&admin_policy, &role_name)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|py| Ok(py.None()))
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
                    .read()
                    .await
                    .grant_privileges(&admin_policy, &role_name, &privileges)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|py| Ok(py.None()))
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
                    .read()
                    .await
                    .revoke_privileges(&admin_policy, &role_name, &privileges)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|py| Ok(py.None()))
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
                    .read()
                    .await
                    .set_allowlist(&admin_policy, &role_name, &allowlist)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|py| Ok(py.None()))
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
                    .read()
                    .await
                    .set_quotas(&admin_policy, &role_name, read_quota, write_quota)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|py| Ok(py.None()))
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
                    .read()
                    .await
                    .node_names()
                    .await;

                Ok(node_names)
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
                    .read()
                    .await
                    .cluster
                    .get_random_node()
                    .await
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
                    .read()
                    .await
                    .nodes()
                    .await;

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
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  Blob
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(subclass, freelist = 1, sequence)]
    #[derive(Debug, Clone)]
    pub struct Blob {
        v: Vec<u8>,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl Blob {
        #[new]
        pub fn new(v: Vec<u8>) -> Self {
            Blob { v }
        }

        #[getter]
        pub fn get_value(&self) -> Vec<u8> {
            self.v.clone()
        }

        #[setter]
        pub fn set_value(&mut self, b: Vec<u8>) {
            self.v = b
        }

        /// Returns a string representation of the value.
        pub fn as_string(&self) -> String {
            PythonValue::Blob(self.v.clone()).as_string()
        }

        fn __getitem__(&mut self, idx: usize) -> PyResult<u8> {
            if idx >= self.v.len() {
                return Err(PyIndexError::new_err("index out of bounds"));
            }
            Ok(self.v[idx])
        }

        fn __setitem__(&mut self, idx: usize, v: u8) -> PyResult<()> {
            if idx >= self.v.len() {
                return Err(PyIndexError::new_err("index out of bounds"));
            }
            self.v[idx] = v;
            Ok(())
        }

        fn __hash__(&self) -> u64 {
            let mut s = DefaultHasher::new();
            self.v.hash(&mut s);
            s.finish()
        }

        fn __richcmp__<'a>(&self, other: &Bound<'a, PyAny>, op: CompareOp) -> bool {
            match op {
                CompareOp::Eq => {
                    let l: PyResult<Blob> = other.extract();
                    if let Ok(l) = l {
                        return self.v == l.v;
                    }

                    let l: PyResult<Vec<u8>> = other.extract();
                    if let Ok(l) = l {
                        return self.v == l;
                    }

                    false
                }
                CompareOp::Ne => {
                    let l: PyResult<Blob> = other.extract();
                    if let Ok(l) = l {
                        return self.v != l.v;
                    }

                    let l: PyResult<Vec<u8>> = other.extract();
                    if let Ok(l) = l {
                        return self.v != l;
                    }

                    true
                }
                _ => false,
            }
        }

        fn __add__(&self, other: &Bound<PyAny>) -> PyResult<Blob> {
            // Handle Blob + Blob
            if let Ok(other_blob) = other.extract::<Blob>() {
                let mut result = self.v.clone();
                result.extend_from_slice(&other_blob.v);
                return Ok(Blob::new(result));
            }

            // Handle Blob + Vec<u8>
            if let Ok(other_vec) = other.extract::<Vec<u8>>() {
                let mut result = self.v.clone();
                result.extend_from_slice(&other_vec);
                return Ok(Blob::new(result));
            }

            Err(PyTypeError::new_err("unsupported operand type(s) for +: 'Blob' and other type"))
        }

        fn __mul__(&self, other: &Bound<PyAny>) -> PyResult<Blob> {
            // Handle Blob * int
            if let Ok(count) = other.extract::<i32>() {
                if count < 0 {
                    return Err(PyValueError::new_err("can't multiply Blob by negative number"));
                }
                let mut result = Vec::new();
                for _ in 0..count {
                    result.extend_from_slice(&self.v);
                }
                return Ok(Blob::new(result));
            }

            Err(PyTypeError::new_err("unsupported operand type(s) for *: 'Blob' and other type"))
        }

        fn __iadd__(&mut self, other: &Bound<PyAny>) -> PyResult<()> {
            // Handle Blob += Blob
            if let Ok(other_blob) = other.extract::<Blob>() {
                self.v.extend_from_slice(&other_blob.v);
                return Ok(());
            }

            // Handle Blob += Vec<u8>
            if let Ok(other_vec) = other.extract::<Vec<u8>>() {
                self.v.extend_from_slice(&other_vec);
                return Ok(());
            }

            Err(PyTypeError::new_err("unsupported operand type(s) for +=: 'Blob' and other type"))
        }

        fn __imul__(&mut self, other: &Bound<PyAny>) -> PyResult<()> {
            // Handle Blob *= int
            if let Ok(count) = other.extract::<i32>() {
                if count < 0 {
                    return Err(PyValueError::new_err("can't multiply Blob by negative number"));
                }
                let original = self.v.clone();
                self.v.clear();
                for _ in 0..count {
                    self.v.extend_from_slice(&original);
                }
                return Ok(());
            }

            Err(PyTypeError::new_err("unsupported operand type(s) for *=: 'Blob' and other type"))
        }

        fn __delitem__(&mut self, idx: usize) -> PyResult<()> {
            if idx >= self.v.len() {
                return Err(PyIndexError::new_err("index out of bounds"));
            }
            self.v.remove(idx);
            Ok(())
        }

        fn __len__(&self) -> PyResult<usize> {
            Ok(self.v.len())
        }
    }

    impl fmt::Display for Blob {
        fn fmt(&self, f: &mut fmt::Formatter) -> std::result::Result<(), fmt::Error> {
            write!(f, "{}", self.as_string())
        }
    }

    // impl From<Blob> for PythonValue {
    //     fn from(input: Blob) -> Self {
    //         PythonValue::Blob(input.v.clone())
    //     }
    // }

    // impl Into<PythonValue> for Blob {
    //     fn into(self) -> PythonValue {
    //         PythonValue::Blob(self.v)
    //     }
    // }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  Map
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(subclass, freelist = 1, sequence)]
    #[derive(Debug, Clone)]
    pub struct Map {
        v: HashMap<PythonValue, PythonValue>,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl Map {
        #[new]
        pub fn new(v: HashMap<PythonValue, PythonValue>) -> Self {
            Map { v }
        }

        #[getter]
        pub fn get_value(&self) -> HashMap<PythonValue, PythonValue> {
            self.v.clone()
        }

        #[setter]
        pub fn set_value(&mut self, b: HashMap<PythonValue, PythonValue>) {
            self.v = b
        }

        /// Returns a string representation of the value.
        pub fn as_string(&self) -> String {
            PythonValue::HashMap(self.v.clone()).as_string()
        }

        // TODO: Change HashMap into BTreeMap and use that
        // This requires Rust Client implementation first
        // fn __hash__(&self) -> u64 {
        //     let mut s = DefaultHasher::new();
        //     self.v.hash(&mut s);
        //     s.finish()
        // }

        fn __richcmp__<'a>(&self, other: &Bound<'a, PyAny>, op: CompareOp) -> bool {
            match op {
                CompareOp::Eq => {
                    let l: PyResult<Map> = other.extract();
                    if let Ok(l) = l {
                        return self.v == l.v;
                    }

                    let l: PyResult<HashMap<PythonValue, PythonValue>> = other.extract();
                    if let Ok(l) = l {
                        return self.v == l;
                    }

                    false
                }
                CompareOp::Ne => {
                    let l: PyResult<Map> = other.extract();
                    if let Ok(l) = l {
                        return self.v != l.v;
                    }

                    let l: PyResult<HashMap<PythonValue, PythonValue>> = other.extract();
                    if let Ok(l) = l {
                        return self.v != l;
                    }

                    true
                }
                _ => false,
            }
        }

        fn __str__(&self) -> PyResult<String> {
            // Convert HashMap to JSON-like string format
            let mut items = Vec::new();
            for (k, v) in &self.v {
                let key_str = match k {
                    PythonValue::String(s) => format!("\"{}\"", s),
                    _ => format!("{:?}", k),
                };
                let val_str = match v {
                    PythonValue::String(s) => format!("\"{}\"", s),
                    PythonValue::Int(i) => i.to_string(),
                    PythonValue::UInt(ui) => ui.to_string(),
                    PythonValue::Bool(b) => b.to_string(),
                    PythonValue::Float(f) => f.to_string(),
                    PythonValue::Nil => "None".to_string(),
                    _ => format!("{:?}", v),
                };
                items.push(format!("{}: {}", key_str, val_str));
            }
            Ok(format!("{{{}}}", items.join(", ")))
        }

        fn __repr__(&self) -> PyResult<String> {
            let s = self.__str__()?;
            Ok(format!("Map({})", s))
        }
    }

    impl fmt::Display for Map {
        fn fmt(&self, f: &mut fmt::Formatter) -> std::result::Result<(), fmt::Error> {
            write!(f, "{}", self.as_string())
        }
    }

    // impl From<HashMap> for PythonValue {
    //     fn from(input: HashMap) -> Self {
    //         PythonValue::HashMap(input.v.clone())
    //     }
    // }

    // impl Into<PythonValue> for HashMap {
    //     fn into(self) -> PythonValue {
    //         PythonValue::HashMap(self.v)
    //     }
    // }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  List
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    fn format_python_value(value: &PythonValue) -> String {
        match value {
            PythonValue::String(s) => format!("\"{}\"", s),
            PythonValue::Int(i) => i.to_string(),
            PythonValue::UInt(ui) => ui.to_string(),
            PythonValue::Bool(b) => if *b { "True".to_string() } else { "False".to_string() },
            PythonValue::Float(f) => f.to_string(),
            PythonValue::Nil => "None".to_string(),
            PythonValue::List(l) => {
                let mut items = Vec::new();
                for item in l {
                    items.push(format_python_value(item));
                }
                format!("[{}]", items.join(", "))
            },
            PythonValue::HashMap(h) => {
                let mut items = Vec::new();
                // Sort by key to ensure consistent ordering
                let mut sorted_entries: Vec<_> = h.iter().collect();
                sorted_entries.sort_by_key(|(k, _)| format_python_value(k));

                for (k, v) in sorted_entries {
                    let key_str = format_python_value(k);
                    let val_str = format_python_value(v);
                    items.push(format!("{}: {}", key_str, val_str));
                }
                format!("{{{}}}", items.join(", "))
            },
            _ => format!("{:?}", value),
        }
    }

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(subclass, freelist = 1, sequence)]
    #[derive(Debug, Clone)]
    pub struct List {
        v: Vec<PythonValue>,
        index: usize,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl List {
        #[new]
        pub fn new(v: Vec<PythonValue>) -> Self {
            List { v, index: 0 }
        }

        #[getter]
        pub fn get_value(&self) -> Vec<PythonValue> {
            self.v.clone()
        }

        #[setter]
        pub fn set_value(&mut self, geo: Vec<PythonValue>) {
            self.v = geo
        }

        /// Returns a string representation of the value.
        pub fn as_string(&self) -> String {
            PythonValue::List(self.v.clone()).as_string()
        }

        fn __str__(&self) -> PyResult<String> {
            // Convert internal representation to Python list format
            let mut items = Vec::new();
            for item in &self.v {
                let item_str = format_python_value(item);
                items.push(item_str);
            }
            Ok(format!("[{}]", items.join(", ")))
        }

        fn __repr__(&self) -> PyResult<String> {
            let s = self.__str__()?;
            Ok(format!("List({})", s))
        }

        fn __getitem__(&mut self, idx: usize) -> PyResult<PythonValue> {
            if idx >= self.v.len() {
                return Err(PyIndexError::new_err("index out of bounds"));
            }
            Ok(self.v[idx].clone())
        }

        fn __setitem__(&mut self, idx: usize, v: PythonValue) -> PyResult<()> {
            if idx >= self.v.len() {
                return Err(PyIndexError::new_err("index out of bounds"));
            }
            self.v[idx] = v;
            Ok(())
        }

        fn __delitem__(&mut self, idx: usize) -> PyResult<()> {
            if idx >= self.v.len() {
                return Err(PyIndexError::new_err("index out of bounds"))
            }
            self.v.remove(idx);
            Ok(())
        }

        fn __concat__(&self, mut other: List) -> PyResult<List> {
            let mut new_list = self.v.clone();
            new_list.append(&mut other.v);
            Ok(List { v: new_list, index: 0 })
        }

        fn __inplace_concat__(&mut self, mut other: List) -> PyResult<List> {
            self.v.append(&mut other.v);
            Ok(self.clone())
        }

        fn __repeat__(&self, times: usize) -> PyResult<List> {
            let og = self.v.clone();
            let len = self.v.len();
            let new_list: Vec<_> = og.into_iter().cycle().take(len * times).collect();
            Ok(List { v: new_list, index: 0 })
        }

        fn __inplace_repeat__(&mut self, times: usize) -> PyResult<List> {
            self.__repeat__(times)
        }
        fn __hash__(&self) -> u64 {
            let mut s = DefaultHasher::new();
            self.v.hash(&mut s);
            s.finish()
        }

        fn __len__(&self) -> usize {
            self.v.len()
        }
        fn __richcmp__<'a>(&self, other: &Bound<'a, PyAny>, op: CompareOp) -> bool {
            match op {
                CompareOp::Eq => {
                    let l: PyResult<List> = other.extract();
                    if let Ok(l) = l {
                        return self.v == l.v;
                    }

                    let l: PyResult<Vec<PythonValue>> = other.extract();
                    if let Ok(l) = l {
                        return self.v == l;
                    }

                    false
                }
                CompareOp::Ne => {
                    let l: PyResult<List> = other.extract();
                    if let Ok(l) = l {
                        return self.v != l.v;
                    }

                    let l: PyResult<Vec<PythonValue>> = other.extract();
                    if let Ok(l) = l {
                        return self.v != l;
                    }

                    true
                }
                _ => false,
            }
        }

        fn __iter__(&self) -> Self {
            self.clone()
        }

        fn __next__<'a>(&mut self, py: Python<'a>) -> Option<Py<PyAny>> {
            let res = self.v.get(self.index);
            self.index += 1;
            res.map(|v| v.clone().into_pyobject(py).unwrap().unbind())
        }
    }

    impl fmt::Display for List {
        fn fmt(&self, f: &mut fmt::Formatter) -> std::result::Result<(), fmt::Error> {
            write!(f, "{}", self.as_string())
        }
    }

    // impl From<List> for PythonValue {
    //     fn from(input: List) -> Self {
    //         PythonValue::List(input.v.clone())
    //     }
    // }

    // impl Into<PythonValue> for List {
    //     fn into(self) -> PythonValue {
    //         PythonValue::List(self.v)
    //     }
    // }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  GeoJSON
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(subclass, freelist = 1)]
    #[derive(Debug, Clone)]
    pub struct GeoJSON {
        v: String,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl GeoJSON {
        #[new]
        pub fn new<'a>(py: Python<'a>, v: &Bound<'a, PyAny>) -> PyResult<Self> {
            // Accept both String and dict inputs
            if let Ok(s) = v.extract::<String>() {
                return Ok(GeoJSON { v: s });
            }

            // If it's already a GeoJSON object, extract its value
            if let Ok(geo) = v.extract::<GeoJSON>() {
                return Ok(geo);
            }

            // Try to extract as dict and serialize to JSON
            if let Ok(dict) = v.downcast::<PyDict>() {
                // Use Python's json module to serialize the dict
                let json_module = PyModule::import(py, "json")?;
                let json_dumps = json_module.getattr("dumps")?;
                let json_string: String = json_dumps.call1((dict,))?.extract()?;
                return Ok(GeoJSON { v: json_string });
            }


            Err(PyTypeError::new_err(
                "GeoJSON constructor requires a string, dict, or GeoJSON object"
            ))
        }

        #[getter]
        pub fn get_value(&self) -> String {
            self.v.clone()
        }

        #[setter]
        pub fn set_value(&mut self, geo: String) {
            self.v = geo
        }

        /// Returns a string representation of the value.
        pub fn as_string(&self) -> String {
            PythonValue::GeoJSON(self.v.clone()).as_string()
        }

        fn __richcmp__<'a>(&self, other: &Bound<'a, PyAny>, op: CompareOp) -> bool {
            match op {
                CompareOp::Eq => {
                    let l: PyResult<GeoJSON> = other.extract();
                    if let Ok(l) = l {
                        return self.v == l.v;
                    }

                    let l: PyResult<String> = other.extract();
                    if let Ok(l) = l {
                        return self.v == l;
                    }

                    false
                }
                CompareOp::Ne => {
                    let l: PyResult<GeoJSON> = other.extract();
                    if let Ok(l) = l {
                        return self.v != l.v;
                    }

                    let l: PyResult<String> = other.extract();
                    if let Ok(l) = l {
                        return self.v != l;
                    }

                    true
                }
                _ => false,
            }
        }

        fn __str__(&self) -> PyResult<String> {
            Ok(self.v.clone())
        }

        fn __repr__(&self) -> PyResult<String> {
            Ok(format!("GeoJSON({})", self.v))
        }
    }

    impl fmt::Display for GeoJSON {
        fn fmt(&self, f: &mut fmt::Formatter) -> std::result::Result<(), fmt::Error> {
            write!(f, "{}", self.as_string())
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  HLL
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(subclass, freelist = 1, sequence)]
    #[derive(Debug, Clone)]
    pub struct HLL {
        v: Vec<u8>,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl HLL {
        #[new]
        pub fn new(v: Vec<u8>) -> Self {
            HLL { v }
        }

        #[getter]
        pub fn get_value(&self) -> Vec<u8> {
            self.v.clone()
        }

        #[setter]
        pub fn set_value(&mut self, hll: Vec<u8>) {
            self.v = hll
        }

        /// Returns a string representation of the value.
        pub fn as_string(&self) -> String {
            PythonValue::HLL(self.v.clone()).as_string()
        }

        fn __richcmp__<'a>(&self, other: &Bound<'a, PyAny>, op: CompareOp) -> bool {
            match op {
                CompareOp::Eq => {
                    let l: PyResult<HLL> = other.extract();
                    if let Ok(l) = l {
                        return self.v == l.v;
                    }

                    let l: PyResult<Vec<u8>> = other.extract();
                    if let Ok(l) = l {
                        return self.v == l;
                    }

                    false
                }
                CompareOp::Ne => {
                    let l: PyResult<HLL> = other.extract();
                    if let Ok(l) = l {
                        return self.v != l.v;
                    }

                    let l: PyResult<Vec<u8>> = other.extract();
                    if let Ok(l) = l {
                        return self.v != l;
                    }

                    true
                }
                _ => false,
            }
        }
    }

    impl fmt::Display for HLL {
        fn fmt(&self, f: &mut fmt::Formatter) -> std::result::Result<(), fmt::Error> {
            write!(f, "{}", self.as_string())
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  PythonValue
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    // Container for bin values stored in the Aerospike database.
    #[derive(Debug, Clone, PartialEq, Eq)]
    #[allow(clippy::upper_case_acronyms)]
    pub enum PythonValue {
        /// Empty value.
        Nil,
        /// Boolean value.
        Bool(bool),
        /// Integer value. All integers are represented as 64-bit numerics in Aerospike.
        Int(i64),
        /// Unsigned integer value. The largest integer value that can be stored in a record bin is
        /// `i64::max_value()`; however the list and map data types can store integer values (and keys)
        /// up to `u64::max_value()`.
        ///
        /// # Panics
        ///
        /// Attempting to store an `u64` value as a record bin value will cause a panic. Use casting to
        /// store and retrieve `u64` values.
        UInt(u64),
        /// Floating point value. All floating point values are stored in 64-bit IEEE-754 format in
        /// Aerospike. Aerospike server v3.6.0 and later support double data type.
        Float(ordered_float::OrderedFloat<f64>),
        /// String value.
        String(String),
        /// Byte array value.
        Blob(Vec<u8>),
        /// List data type is an ordered collection of values. Lists can contain values of any
        /// supported data type. List data order is maintained on writes and reads.
        List(Vec<PythonValue>),
        /// Map data type is a collection of key-value pairs. Each key can only appear once in a
        /// collection and is associated with a value. Map keys and values can be any supported data
        /// type.
        HashMap(HashMap<PythonValue, PythonValue>),
        /// GeoJSON data type are JSON formatted strings to encode geospatial information.
        GeoJSON(String),

        /// HLL value
        HLL(Vec<u8>),
    }

    #[allow(clippy::derived_hash_with_manual_eq)]
    impl Hash for PythonValue {
        fn hash<H: Hasher>(&self, state: &mut H) {
            match *self {
                PythonValue::Nil => {
                    let v: Option<u8> = None;
                    v.hash(state);
                }
                PythonValue::Bool(ref val) => val.hash(state),
                PythonValue::Int(ref val) => val.hash(state),
                PythonValue::UInt(ref val) => val.hash(state),
                PythonValue::Float(ref val) => val.hash(state),
                PythonValue::String(ref val) | PythonValue::GeoJSON(ref val) => val.hash(state),
                PythonValue::Blob(ref val) | PythonValue::HLL(ref val) => val.hash(state),
                PythonValue::List(ref val) => val.hash(state),
                PythonValue::HashMap(_) => panic!("HashMaps cannot be used as map keys."),
                // PythonValue::OrderedMap(ref val) => val.hash(state),
            }
        }
    }

    impl PythonValue {
        /// Returns a string representation of the value.
        pub fn as_string(&self) -> String {
            match *self {
                PythonValue::Nil => "<null>".to_string(),
                PythonValue::Int(ref val) => val.to_string(),
                PythonValue::UInt(ref val) => val.to_string(),
                PythonValue::Bool(ref val) => val.to_string(),
                PythonValue::Float(ref val) => val.to_string(),
                PythonValue::String(ref val) => val.to_string(),
                PythonValue::GeoJSON(ref val) => format!("GeoJSON('{}')", val),
                PythonValue::Blob(ref val) => format!("{:?}", val),
                PythonValue::HLL(ref val) => format!("HLL('{:?}')", val),
                PythonValue::List(ref val) => format!("{:?}", val),
                PythonValue::HashMap(ref val) => format!("{:?}", val),
                // PythonValue::OrderedMap(ref val) => format!("{:?}", val),
            }
        }
    }

    impl fmt::Display for PythonValue {
        fn fmt(&self, f: &mut fmt::Formatter) -> std::result::Result<(), fmt::Error> {
            write!(f, "{}", self.as_string())
        }
    }

    impl<'py> IntoPyObject<'py> for PythonValue {
        type Target = PyAny; // the Python type
        type Output = Bound<'py, Self::Target>; // in most cases this will be `Bound`
        type Error = std::convert::Infallible;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self {
                PythonValue::Nil => Ok(py.None().into_bound(py)),
                PythonValue::Bool(b) => Ok(PyBool::new(py, b).into_bound_py_any(py).unwrap()),
                PythonValue::Int(i) => Ok(i.into_pyobject(py).map(|v| v.into_any()).unwrap()),
                PythonValue::UInt(ui) => Ok(ui.into_pyobject(py).map(|v| v.into_any()).unwrap()),
                PythonValue::Float(f) => Ok(f.into_pyobject(py).map(|v| v.into_any()).unwrap()),
                PythonValue::String(s) => Ok(s.into_pyobject(py).map(|v| v.into_any()).unwrap()),
                PythonValue::Blob(b) => Ok(PyBytes::new(py, &b).into_any()),
                PythonValue::List(l) => {
                    let py_list = PyList::empty(py);
                    for item in l {
                        let py_item = item.into_pyobject(py).unwrap();
                        py_list.append(py_item).unwrap();
                    }
                    Ok(py_list.into_any())
                }
                PythonValue::HashMap(h) => {
                    let py_dict = PyDict::new(py);
                    for (k, v) in h {
                        let py_key = k.into_pyobject(py).unwrap();
                        let py_val = v.into_pyobject(py).unwrap();
                        py_dict.set_item(py_key, py_val).unwrap();
                    }
                    Ok(py_dict.into_any())
                }
                PythonValue::GeoJSON(s) => {
                    let geo = GeoJSON { v: s };
                    Ok(geo.into_pyobject(py).map(|v| v.into_any()).unwrap())
                }
                PythonValue::HLL(b) => Ok(HLL::new(b).into_pyobject(py).map(|v| v.into_any()).unwrap()),
            }
        }
    }

    impl<'source> FromPyObject<'source> for PythonValue {
        fn extract_bound(ob: &Bound<'_, PyAny>) -> PyResult<Self> {
            // Handle None first - check if the object is None
            if ob.is_none() {
                return Ok(PythonValue::Nil);
            }

            let b: PyResult<bool> = ob.extract();
            if let Ok(b) = b {
                return Ok(PythonValue::Bool(b));
            }

            // Try to extract as integer - handle both i64 and large u64 values
            // First try i64 (most common case)
            let i: PyResult<i64> = ob.extract();
            if let Ok(i) = i {
                return Ok(PythonValue::Int(i));
            }

            // For large positive integers that don't fit in i64, try u64
            // Check if the value can be extracted as u64 and is beyond i64::MAX
            let ui: PyResult<u64> = ob.extract();
            if let Ok(ui) = ui {
                // Only use UInt if it's beyond i64::MAX
                if ui > i64::MAX as u64 {
                    return Ok(PythonValue::UInt(ui));
                }
                // Otherwise, convert to i64 (should fit since ui <= i64::MAX)
                return Ok(PythonValue::Int(ui as i64));
            }

            let f1: PyResult<f64> = ob.extract();
            if let Ok(f1) = f1 {
                return Ok(PythonValue::Float(ordered_float::OrderedFloat(f1)));
            }

            let s: PyResult<String> = ob.extract();
            if let Ok(s) = s {
                return Ok(PythonValue::String(s));
            }

            // Try to extract as bytearray
            if let Ok(ba) = ob.downcast::<PyByteArray>() {
                return Ok(PythonValue::Blob(ba.to_vec()));
            }

            // Try to extract as bytes
            if let Ok(bytes) = ob.downcast::<PyBytes>() {
                return Ok(PythonValue::Blob(bytes.as_bytes().to_vec()));
            }

            let b: PyResult<Blob> = ob.extract();
            if let Ok(b) = b {
                return Ok(PythonValue::Blob(b.v));
            }

            let l: PyResult<Vec<PythonValue>> = ob.extract();
            if let Ok(l) = l {
                return Ok(PythonValue::List(l));
            }

            let l: PyResult<List> = ob.extract();
            if let Ok(l) = l {
                return Ok(PythonValue::List(l.v));
            }

            let hm: PyResult<HashMap<PythonValue, PythonValue>> = ob.extract();
            if let Ok(hm) = hm {
                return Ok(PythonValue::HashMap(hm));
            }

            let geo: PyResult<GeoJSON> = ob.extract();
            if let Ok(geo) = geo {
                return Ok(PythonValue::GeoJSON(geo.v));
            }

            let hll: PyResult<HLL> = ob.extract();
            if let Ok(hll) = hll {
                return Ok(PythonValue::HLL(hll.v));
            }

            Err(PyTypeError::new_err("invalid value type"))
        }
    }

    impl From<HashMap<String, aerospike_core::Value>> for PythonValue {
        fn from(h: HashMap<String, aerospike_core::Value>) -> Self {
            let mut hash = HashMap::<PythonValue, PythonValue>::with_capacity(h.len());
            h.iter().for_each(|(k, v)| {
                hash.insert(PythonValue::String(k.into()), v.clone().into());
            });
            PythonValue::HashMap(hash)
        }
    }

    impl From<PythonValue> for aerospike_core::Value {
        fn from(other: PythonValue) -> Self {
            match other {
                PythonValue::Nil => aerospike_core::Value::Nil,
                PythonValue::Bool(b) => aerospike_core::Value::Bool(b),
                PythonValue::Int(i) => aerospike_core::Value::Int(i),
                PythonValue::UInt(ui) => aerospike_core::Value::UInt(ui),
                PythonValue::Float(f) => aerospike_core::Value::Float(f64::from(f).into()),
                PythonValue::String(s) => aerospike_core::Value::String(s),
                PythonValue::Blob(b) => aerospike_core::Value::Blob(b),
                PythonValue::List(l) => {
                    let mut nl = Vec::<aerospike_core::Value>::with_capacity(l.len());
                    l.iter().for_each(|v| nl.push(v.clone().into()));
                    aerospike_core::Value::List(nl)
                }
                PythonValue::HashMap(h) => {
                    let mut arr = HashMap::with_capacity(h.len());
                    h.iter().for_each(|(k, v)| {
                        arr.insert(k.clone().into(), v.clone().into());
                    });
                    aerospike_core::Value::HashMap(arr)
                }
                PythonValue::GeoJSON(gj) => aerospike_core::Value::GeoJSON(gj),
                PythonValue::HLL(b) => aerospike_core::Value::HLL(b),
            }
        }
    }

    impl From<aerospike_core::Value> for PythonValue {
        fn from(other: aerospike_core::Value) -> Self {
            match other {
                aerospike_core::Value::Nil => PythonValue::Nil,
                aerospike_core::Value::Bool(b) => PythonValue::Bool(b),
                aerospike_core::Value::Int(i) => PythonValue::Int(i),
                aerospike_core::Value::UInt(ui) => PythonValue::UInt(ui),
                aerospike_core::Value::Float(fv) => {
                    PythonValue::Float(ordered_float::OrderedFloat(fv.into()))
                }
                aerospike_core::Value::String(s) => PythonValue::String(s),
                aerospike_core::Value::Blob(b) => PythonValue::Blob(b),
                aerospike_core::Value::List(l) => {
                    let mut nl = Vec::<PythonValue>::with_capacity(l.len());
                    l.iter().for_each(|v| nl.push(v.clone().into()));
                    PythonValue::List(nl)
                }
                aerospike_core::Value::MultiResult(mv) => {
                    // MultiResult is returned when server executes multiple operations for the same bin
                    // Convert to a list of PythonValues (matching Java client behavior - no flattening)
                    let mut nl = Vec::<PythonValue>::with_capacity(mv.len());
                    mv.iter().for_each(|v| nl.push(v.clone().into()));
                    PythonValue::List(nl)
                }
                aerospike_core::Value::HashMap(h) => {
                    let mut arr = HashMap::with_capacity(h.len());
                    h.iter().for_each(|(k, v)| {
                        arr.insert(k.clone().into(), v.clone().into());
                    });
                    PythonValue::HashMap(arr)
                }
                aerospike_core::Value::OrderedMap(om) => {
                    let mut arr = HashMap::with_capacity(om.len());
                    om.iter().for_each(|(k, v)| {
                        arr.insert(k.clone().into(), v.clone().into());
                    });
                    PythonValue::HashMap(arr)
                }
                aerospike_core::Value::GeoJSON(gj) => PythonValue::GeoJSON(gj),
                aerospike_core::Value::HLL(b) => PythonValue::HLL(b),
                aerospike_core::Value::Infinity => {
                    PythonValue::Float(ordered_float::OrderedFloat(f64::INFINITY))
                }
                aerospike_core::Value::Wildcard => {
                    PythonValue::String("*".to_string())
                }
            }
        }
    }

    impl PyStubType for PythonValue {
        fn type_output() -> TypeInfo {
            TypeInfo::any()
        }
    }

    // impl From<aerospike_core::Bin> for Bin {
    //     fn from(other: aerospike_core::Bin) -> Self {
    //         Bin { _as: other }
    //     }
    // }

    impl From<aerospike_core::Key> for Key {
        fn from(other: aerospike_core::Key) -> Self {
            Key { _as: other }
        }
    }

    impl From<aerospike_core::Record> for Record {
        fn from(other: aerospike_core::Record) -> Self {
            Record { _as: other }
        }
    }

    // impl From<Bin> for aerospike_core::Bin {
    //     fn from(other: Bin) -> Self {
    //         other._as
    //     }
    // }

    // impl From<Arc<aerospike_core::Recordset>> for Recordset {
    //     fn from(other: Arc<aerospike_core::Recordset>) -> Self {
    //         Recordset { _as: other }
    //     }
    // }

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

// Simple logger implementation that writes to stderr
struct SimpleLogger;

impl log::Log for SimpleLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        // Check RUST_LOG environment variable
        if let Ok(rust_log) = std::env::var("RUST_LOG") {
            let level = match rust_log.to_lowercase().as_str() {
                s if s.contains("trace") => log::Level::Trace,
                s if s.contains("debug") => log::Level::Debug,
                s if s.contains("info") => log::Level::Info,
                s if s.contains("warn") => log::Level::Warn,
                s if s.contains("error") => log::Level::Error,
                _ => log::Level::Error,
            };
            metadata.level() <= level
        } else {
            false
        }
    }

    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            eprintln!("[{}] {}: {}", record.level(), record.target(), record.args());
        }
    }

    fn flush(&self) {}
}

static LOGGER: SimpleLogger = SimpleLogger;

#[pymodule]
fn _aerospike_async_native(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Initialize logger if RUST_LOG is set
    if std::env::var("RUST_LOG").is_ok() {
        let _ = log::set_logger(&LOGGER).map(|()| log::set_max_level(log::LevelFilter::Trace));
    }

    // Add all main classes to the top level for easy importing
    m.add_class::<Client>()?;
    m.add_class::<QueryDuration>()?;
    m.add_class::<Replica>()?;
    m.add_class::<Expiration>()?;
    m.add_class::<CommitLevel>()?;
    m.add_class::<ConsistencyLevel>()?;
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

    m.add_class::<BasePolicy>()?;
    m.add_class::<AdminPolicy>()?;
    m.add_class::<ReadPolicy>()?;

    // Add helper functions
    m.add_function(wrap_pyfunction!(null, m)?)?;
    m.add_function(wrap_pyfunction!(geojson, m)?)?;
    m.add_class::<ClientPolicy>()?;
    m.add_class::<WritePolicy>()?;
    m.add_class::<ScanPolicy>()?;
    m.add_class::<QueryPolicy>()?;
    m.add_class::<ListOrderType>()?;
    m.add_class::<ListWriteFlags>()?;
    m.add_class::<ListPolicy>()?;
    m.add_class::<MapOrder>()?;
    m.add_class::<MapWriteMode>()?;
    m.add_class::<MapReturnType>()?;
    m.add_class::<MapPolicy>()?;
    m.add_class::<BitwiseResizeFlags>()?;
    m.add_class::<BitwiseWriteFlags>()?;
    m.add_class::<BitwiseOverflowActions>()?;
    m.add_class::<BitPolicy>()?;
    m.add_class::<PartitionStatus>()?;
    m.add_class::<PartitionFilter>()?;

    m.add_function(wrap_pyfunction!(new_client, m)?)?;

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
    exceptions_module.add("ResultCode", py.get_type::<ResultCode>())?;
    m.add_submodule(&exceptions_module)?;

    Ok(())
}
