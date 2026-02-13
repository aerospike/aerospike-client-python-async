// #![deny(warnings)]
extern crate pyo3;

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::Arc;

use pyo3::basic::CompareOp;
use pyo3::exceptions::{PyException, PyIndexError, PyKeyError, PyValueError};
use pyo3::exceptions::{PyStopAsyncIteration, PyTypeError};
use pyo3::types::{PyBool, PyByteArray, PyBytes, PyDict, PyList};
use pyo3::{prelude::*, Borrowed, IntoPyObjectExt};

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
    in_doubt: bool,
}

#[gen_stub_pymethods]
#[pymethods]
impl ServerError {
    #[new]
    #[pyo3(signature = (_message, result_code, in_doubt=false))]
    fn new(_message: String, result_code: ResultCode, in_doubt: bool) -> PyResult<Self> {
        // Note: message is handled by the base PyException, we only store result_code and in_doubt
        Ok(ServerError { result_code: result_code.0, in_doubt })
    }

    #[getter]
    fn result_code(&self) -> ResultCode {
        ResultCode(self.result_code)
    }

    #[getter]
    fn in_doubt(&self) -> bool {
        self.in_doubt
    }
}

// Helper function to create ServerError as a PyErr
fn create_server_error(message: String, result_code: CoreResultCode, in_doubt: bool) -> PyErr {
    Python::attach(|py| -> PyErr {
        let server_error_type = py.get_type::<ServerError>();
        let result_code_wrapper = ResultCode(result_code);
        match server_error_type.call1((message.clone(), result_code_wrapper, in_doubt)) {
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
                create_server_error(message, result_code, in_doubt)
            },
            Error::UdfBadResponse(string) => UDFBadResponse::new_err(string),
            Error::Timeout(string) => TimeoutError::new_err(string),
            Error::Chain(first, second) => {
                // For Chain errors, look for the most specific error type
                // Check first error
                match first.as_ref() {
                    Error::ServerError(result_code, in_doubt, node) => {
                        let message = format!("Code: {:?}, In Doubt: {}, Node: {}", result_code, in_doubt, node);
                        create_server_error(message, *result_code, *in_doubt)
                    },
                    Error::BadResponse(msg) => {
                        BadResponse::new_err(msg.clone())
                    },
                    Error::ClientError(msg) => {
                        // Check second error for more specific type
                        match second.as_ref() {
                            Error::ServerError(result_code, in_doubt, node) => {
                                let message = format!("Code: {:?}, In Doubt: {}, Node: {}", result_code, in_doubt, node);
                                create_server_error(message, *result_code, *in_doubt)
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
                                create_server_error(message, *result_code, *in_doubt)
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


    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  UDFLang
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

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

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  TaskStatus
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(module = "_aerospike_async_native")]
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
    #[pyclass(subclass, freelist = 1)]
    #[derive(Clone)]
    pub struct RegisterTask {
        _as: aerospike_core::RegisterTask,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl RegisterTask {
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[TaskStatus]", imports=("typing")))]
        pub fn query_status<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
            let task = self._as.clone();
            pyo3_asyncio::future_into_py(py, async move {
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
            let task = self._as.clone();
            pyo3_asyncio::future_into_py(py, async move {
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
    #[pyclass(subclass, freelist = 1)]
    #[derive(Clone)]
    pub struct UdfRemoveTask {
        _as: aerospike_core::UdfRemoveTask,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl UdfRemoveTask {
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[TaskStatus]", imports=("typing")))]
        pub fn query_status<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
            let task = self._as.clone();
            pyo3_asyncio::future_into_py(py, async move {
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
            let task = self._as.clone();
            pyo3_asyncio::future_into_py(py, async move {
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
    #[pyclass(subclass, freelist = 1)]
    #[derive(Clone)]
    pub struct IndexTask {
        _as: aerospike_core::IndexTask,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl IndexTask {
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[TaskStatus]", imports=("typing")))]
        pub fn query_status<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
            let task = self._as.clone();
            pyo3_asyncio::future_into_py(py, async move {
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
            let task = self._as.clone();
            pyo3_asyncio::future_into_py(py, async move {
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
    #[pyclass(subclass, freelist = 1)]
    #[derive(Clone)]
    pub struct DropIndexTask {
        _as: aerospike_core::DropIndexTask,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl DropIndexTask {
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[TaskStatus]", imports=("typing")))]
        pub fn query_status<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
            let task = self._as.clone();
            pyo3_asyncio::future_into_py(py, async move {
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
            let task = self._as.clone();
            pyo3_asyncio::future_into_py(py, async move {
                wait_till_complete_impl(task, sleep_time, max_attempts).await
            })
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  Version
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(subclass, freelist = 1)]
    #[derive(Clone)]
    pub struct Version {
        _as: aerospike_core::Version,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl Version {
        #[getter]
        pub fn major(&self) -> u64 {
            self._as.major
        }

        #[getter]
        pub fn minor(&self) -> u64 {
            self._as.minor
        }

        #[getter]
        pub fn patch(&self) -> u64 {
            self._as.patch
        }

        #[getter]
        pub fn build(&self) -> u64 {
            self._as.build
        }

        /// Returns true if server supports partition scans (>= 4.9.0.3).
        pub fn supports_partition_scan(&self) -> bool {
            self._as.supports_partition_scan()
        }

        /// Returns true if server supports query-show command (>= 5.7.0.0).
        pub fn supports_query_show(&self) -> bool {
            self._as.supports_query_show()
        }

        /// Returns true if server supports batch-index commands (>= 6.0.0.0).
        pub fn supports_batch_any(&self) -> bool {
            self._as.supports_batch_any()
        }

        /// Returns true if server supports partition queries (>= 6.0.0.0).
        pub fn supports_partition_query(&self) -> bool {
            self._as.supports_partition_query()
        }

        /// Returns true if server supports app-id (>= 8.1.0.0).
        pub fn supports_app_id(&self) -> bool {
            self._as.supports_app_id()
        }

        pub fn __str__(&self) -> String {
            format!("{}.{}.{}.{}", self._as.major, self._as.minor, self._as.patch, self._as.build)
        }

        pub fn __repr__(&self) -> String {
            format!("Version({}.{}.{}.{})", self._as.major, self._as.minor, self._as.patch, self._as.build)
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  Node
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(subclass, freelist = 1)]
    #[derive(Clone)]
    pub struct Node {
        _as: std::sync::Arc<aerospike_core::Node>,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl Node {
        /// Returns the node name.
        #[getter]
        pub fn name(&self) -> &str {
            self._as.name()
        }

        /// Returns the node address.
        #[getter]
        pub fn address(&self) -> &str {
            self._as.address()
        }

        /// Returns true if the node is active.
        #[getter]
        pub fn is_active(&self) -> bool {
            self._as.is_active()
        }

        /// Returns the server version.
        #[getter]
        pub fn version(&self) -> Version {
            Version { _as: self._as.version().clone() }
        }

        /// Returns the node host as a tuple (hostname, port).
        #[getter]
        pub fn host(&self) -> (String, u16) {
            let h = self._as.host();
            (h.name, h.port)
        }

        /// Returns the count of connection failures for this node.
        #[getter]
        pub fn failures(&self) -> usize {
            self._as.failures()
        }

        /// Returns the partition generation number.
        #[getter]
        pub fn partition_generation(&self) -> isize {
            self._as.partition_generation()
        }

        /// Returns the rebalance generation number.
        #[getter]
        pub fn rebalance_generation(&self) -> isize {
            self._as.rebalance_generation()
        }

        /// Returns a list of host aliases for this node.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.List[typing.Tuple[str, int]]]", imports=("typing")))]
        pub fn aliases<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
            let node = std::sync::Arc::clone(&self._as);
            pyo3_asyncio::future_into_py(py, async move {
                let aliases = node.aliases().await;
                let result: Vec<(String, u16)> = aliases.into_iter().map(|h| (h.name, h.port)).collect();
                Ok(result)
            })
        }

        /// Execute an info command on this node.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Dict[str, str]]", imports=("typing")))]
        #[pyo3(signature = (command, *, policy = None))]
        pub fn info<'a>(
            &self,
            command: String,
            policy: Option<AdminPolicy>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let node = std::sync::Arc::clone(&self._as);
            let admin_policy =
                policy.map(|p| p._as).unwrap_or_else(|| aerospike_core::AdminPolicy::default());

            pyo3_asyncio::future_into_py(py, async move {
                let response = node
                    .info(&admin_policy, &[&command])
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;
                Ok(response)
            })
        }

        pub fn __str__(&self) -> String {
            format!("Node(name={}, address={})", self._as.name(), self._as.address())
        }

        pub fn __repr__(&self) -> String {
            format!("Node(name='{}', address='{}', active={})", self._as.name(), self._as.address(), self._as.is_active())
        }
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

    /// ListReturnType - supports bitwise OR for combining with INVERTED flag.
    /// 
    /// Example:
    ///     combined = ListReturnType.VALUE | ListReturnType.INVERTED
    // Note: pyo3_stub_gen generates minimal stubs for structs with #[classattr] constants.
    // Full stubs are added in postprocess_stubs.py
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(name = "ListReturnType", module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct ListReturnType(u32);

    #[pymethods]
    impl ListReturnType {
        /// Do not return a result.
        #[classattr]
        const NONE: Self = Self(0);
        /// Return index offset order.
        #[classattr]
        const INDEX: Self = Self(1);
        /// Return reverse index offset order.
        #[classattr]
        const REVERSE_INDEX: Self = Self(2);
        /// Return value order.
        #[classattr]
        const RANK: Self = Self(3);
        /// Return reverse value order.
        #[classattr]
        const REVERSE_RANK: Self = Self(4);
        /// Return count of items selected.
        #[classattr]
        const COUNT: Self = Self(5);
        /// Return value for single key read and value list for range read.
        #[classattr]
        const VALUE: Self = Self(6);
        /// Return true if count > 0.
        #[classattr]
        const EXISTS: Self = Self(7);
        /// Invert meaning of list command and return values.
        /// Can be OR'd with other return types: VALUE | INVERTED
        #[classattr]
        const INVERTED: Self = Self(0x10000);

        /// Bitwise OR - allows combining return type with INVERTED flag
        fn __or__(&self, other: &Self) -> Self {
            Self(self.0 | other.0)
        }

        /// Bitwise AND
        fn __and__(&self, other: &Self) -> Self {
            Self(self.0 & other.0)
        }

        /// Convert to integer
        fn __int__(&self) -> u32 {
            self.0
        }

        fn __richcmp__(&self, other: &ListReturnType, op: pyo3::class::basic::CompareOp) -> pyo3::PyResult<bool> {
            match op {
                pyo3::class::basic::CompareOp::Eq => Ok(self == other),
                pyo3::class::basic::CompareOp::Ne => Ok(self != other),
                pyo3::class::basic::CompareOp::Lt => Ok(self.0 < other.0),
                pyo3::class::basic::CompareOp::Le => Ok(self.0 <= other.0),
                pyo3::class::basic::CompareOp::Gt => Ok(self.0 > other.0),
                pyo3::class::basic::CompareOp::Ge => Ok(self.0 >= other.0),
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
            let base = self.0 & 0xFFFF;
            let inverted = (self.0 & 0x10000) != 0;
            let base_name = match base {
                0 => "NONE",
                1 => "INDEX",
                2 => "REVERSE_INDEX",
                3 => "RANK",
                4 => "REVERSE_RANK",
                5 => "COUNT",
                6 => "VALUE",
                7 => "EXISTS",
                _ => "UNKNOWN",
            };
            if inverted && base != 0 {
                format!("ListReturnType.{} | ListReturnType.INVERTED", base_name)
            } else if inverted {
                "ListReturnType.INVERTED".to_string()
            } else {
                format!("ListReturnType.{}", base_name)
            }
        }
    }

    /// Newtype wrapper for passing ListReturnType bitmask to core functions.
    /// Allows us to implement ToListReturnTypeBitmask for our custom struct.
    pub struct ListReturnTypeBitmask(i64);
    
    impl aerospike_core::operations::lists::ToListReturnTypeBitmask for ListReturnTypeBitmask {
        fn to_bitmask(self) -> i64 {
            self.0
        }
    }
    
    impl From<&ListReturnType> for ListReturnTypeBitmask {
        fn from(input: &ListReturnType) -> Self {
            // The u32 value already encodes base type (lower 16 bits) and inverted flag (bit 16)
            // Core library expects same bitmask layout, so we can just convert to i64
            ListReturnTypeBitmask(input.0 as i64)
        }
    }
    
    // Keep the enum conversion for backward compatibility with non-inverted cases
    impl From<&ListReturnType> for aerospike_core::operations::lists::ListReturnType {
        fn from(input: &ListReturnType) -> Self {
            // Only valid for non-inverted values
            let base = input.0 & 0xFFFF;
            match base {
                0 => aerospike_core::operations::lists::ListReturnType::None,
                1 => aerospike_core::operations::lists::ListReturnType::Index,
                2 => aerospike_core::operations::lists::ListReturnType::ReverseIndex,
                3 => aerospike_core::operations::lists::ListReturnType::Rank,
                4 => aerospike_core::operations::lists::ListReturnType::ReverseRank,
                5 => aerospike_core::operations::lists::ListReturnType::Count,
                6 => aerospike_core::operations::lists::ListReturnType::Values,
                7 => aerospike_core::operations::lists::ListReturnType::Exists,
                _ => aerospike_core::operations::lists::ListReturnType::None,
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

    /// MapReturnType - supports bitwise OR for combining with INVERTED flag.
    /// 
    /// Example:
    ///     combined = MapReturnType.VALUE | MapReturnType.INVERTED
    // Note: pyo3_stub_gen generates minimal stubs for structs with #[classattr] constants.
    // Full stubs are added in postprocess_stubs.py
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(name = "MapReturnType", module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct MapReturnType(u32);

    #[pymethods]
    impl MapReturnType {
        /// Do not return a result.
        #[classattr]
        const NONE: Self = Self(0);
        /// Return key index order.
        #[classattr]
        const INDEX: Self = Self(1);
        /// Return reverse key order.
        #[classattr]
        const REVERSE_INDEX: Self = Self(2);
        /// Return value order.
        #[classattr]
        const RANK: Self = Self(3);
        /// Return reverse value order.
        #[classattr]
        const REVERSE_RANK: Self = Self(4);
        /// Return count of items selected.
        #[classattr]
        const COUNT: Self = Self(5);
        /// Return key for single key read and key list for range read.
        #[classattr]
        const KEY: Self = Self(6);
        /// Return value for single key read and value list for range read.
        #[classattr]
        const VALUE: Self = Self(7);
        /// Return key/value items.
        #[classattr]
        const KEY_VALUE: Self = Self(8);
        /// Returns true if count > 0.
        #[classattr]
        const EXISTS: Self = Self(9);
        /// Returns an unordered map.
        #[classattr]
        const UNORDERED_MAP: Self = Self(10);
        /// Returns an ordered map.
        #[classattr]
        const ORDERED_MAP: Self = Self(11);
        /// Invert meaning of map command and return values.
        /// Can be OR'd with other return types: VALUE | INVERTED
        #[classattr]
        const INVERTED: Self = Self(0x10000);

        /// Bitwise OR - allows combining return type with INVERTED flag
        fn __or__(&self, other: &Self) -> Self {
            Self(self.0 | other.0)
        }

        /// Bitwise AND
        fn __and__(&self, other: &Self) -> Self {
            Self(self.0 & other.0)
        }

        /// Convert to integer
        fn __int__(&self) -> u32 {
            self.0
        }

        fn __richcmp__(&self, other: &MapReturnType, op: pyo3::class::basic::CompareOp) -> pyo3::PyResult<bool> {
            match op {
                pyo3::class::basic::CompareOp::Eq => Ok(self == other),
                pyo3::class::basic::CompareOp::Ne => Ok(self != other),
                pyo3::class::basic::CompareOp::Lt => Ok(self.0 < other.0),
                pyo3::class::basic::CompareOp::Le => Ok(self.0 <= other.0),
                pyo3::class::basic::CompareOp::Gt => Ok(self.0 > other.0),
                pyo3::class::basic::CompareOp::Ge => Ok(self.0 >= other.0),
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
            let base = self.0 & 0xFFFF;
            let inverted = (self.0 & 0x10000) != 0;
            let base_name = match base {
                0 => "NONE",
                1 => "INDEX",
                2 => "REVERSE_INDEX",
                3 => "RANK",
                4 => "REVERSE_RANK",
                5 => "COUNT",
                6 => "KEY",
                7 => "VALUE",
                8 => "KEY_VALUE",
                9 => "EXISTS",
                10 => "UNORDERED_MAP",
                11 => "ORDERED_MAP",
                _ => "UNKNOWN",
            };
            if inverted && base != 0 {
                format!("MapReturnType.{} | MapReturnType.INVERTED", base_name)
            } else if inverted {
                "MapReturnType.INVERTED".to_string()
            } else {
                format!("MapReturnType.{}", base_name)
            }
        }
    }

    /// Newtype wrapper for passing MapReturnType bitmask to core functions.
    /// Allows us to implement ToMapReturnTypeBitmask for our custom struct.
    pub struct MapReturnTypeBitmask(i64);
    
    impl aerospike_core::operations::maps::ToMapReturnTypeBitmask for MapReturnTypeBitmask {
        fn to_bitmask(self) -> i64 {
            self.0
        }
    }
    
    impl From<&MapReturnType> for MapReturnTypeBitmask {
        fn from(input: &MapReturnType) -> Self {
            // The u32 value already encodes base type (lower 16 bits) and inverted flag (bit 16)
            // Core library expects same bitmask layout, so we can just convert to i64
            MapReturnTypeBitmask(input.0 as i64)
        }
    }
    
    // Keep the enum conversion for backward compatibility with non-inverted cases
    impl From<&MapReturnType> for aerospike_core::operations::maps::MapReturnType {
        fn from(input: &MapReturnType) -> Self {
            // Only valid for non-inverted values
            let base = input.0 & 0xFFFF;
            match base {
                0 => aerospike_core::operations::maps::MapReturnType::None,
                1 => aerospike_core::operations::maps::MapReturnType::Index,
                2 => aerospike_core::operations::maps::MapReturnType::ReverseIndex,
                3 => aerospike_core::operations::maps::MapReturnType::Rank,
                4 => aerospike_core::operations::maps::MapReturnType::ReverseRank,
                5 => aerospike_core::operations::maps::MapReturnType::Count,
                6 => aerospike_core::operations::maps::MapReturnType::Key,
                7 => aerospike_core::operations::maps::MapReturnType::Value,
                8 => aerospike_core::operations::maps::MapReturnType::KeyValue,
                9 => aerospike_core::operations::maps::MapReturnType::Exists,
                10 => aerospike_core::operations::maps::MapReturnType::UnorderedMap,
                11 => aerospike_core::operations::maps::MapReturnType::OrderedMap,
                _ => aerospike_core::operations::maps::MapReturnType::None,
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
        /// Compare two CTX objects for equality.
        fn __eq__(&self, other: &CTX) -> bool {
            self.ctx == other.ctx
        }

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
        /// Converts HashMap to BTreeMap (OrderedMap) for exact byte-level matching to ensure consistent serialization.
        #[staticmethod]
        pub fn map_value(value: PythonValue) -> Self {
            let core_value = match value {
                PythonValue::HashMap(h) => {
                    // For map_value context, always use BTreeMap (sorted) for exact byte-level matching
                    // HashMap iteration order is non-deterministic, so we sort to ensure consistent serialization
                    let mut btree_map: BTreeMap<aerospike_core::Value, aerospike_core::Value> = BTreeMap::new();
                    for (k, v) in h {
                        btree_map.insert(aerospike_core::Value::from(k), aerospike_core::Value::from(v));
                    }
                    aerospike_core::Value::OrderedMap(btree_map)
                }
                _ => aerospike_core::Value::from(value),
            };
            CTX {
                ctx: aerospike_core::operations::cdt_context::ctx_map_value(core_value),
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
    #[derive(Clone, Debug)]
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
        /// Create boolean bin expression.
        pub fn bool_bin(name: String) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::bool_bin(name),
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
        /// Create expression that returns the record size. Usually evaluates quickly because
        /// record metadata is cached in memory. Requires server version 7.0+.
        pub fn record_size() -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::record_size(),
            }
        }

        #[staticmethod]
        /// Create function that returns record size on disk. If server storage-engine is
        /// memory, then zero is returned. Deprecated: use record_size() for server version 7.0+.
        /// Implemented via record_size() for server 7.0+.
        pub fn device_size() -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::record_size(),
            }
        }

        #[staticmethod]
        /// Create expression that returns record size in memory. Deprecated: use record_size() for server 7.0+.
        /// Implemented via record_size() for server 7.0+.
        pub fn memory_size() -> Self {
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
        /// Converts HashMap to BTreeMap (OrderedMap) for exact byte-level matching.
        /// map_val accepts both HashMap and BTreeMap (OrderedMap) via the MapLike trait.
        /// We use BTreeMap to ensure deterministic key ordering for serialization matching.
        pub fn map_val(val: PythonValue) -> Self {
            match val {
                PythonValue::HashMap(h) => {
                    // Convert to BTreeMap for deterministic key ordering. Rust HashMap iteration
                    // order is non-deterministic, so we sort keys to ensure exact byte-level matching
                    // with the server's stored format (which uses sorted keys for KEY_ORDERED maps).
                    let mut btree_map: BTreeMap<aerospike_core::Value, aerospike_core::Value> = BTreeMap::new();
                    for (k, v) in h {
                        btree_map.insert(aerospike_core::Value::from(k), aerospike_core::Value::from(v));
                    }

                    // BTreeMap implements MapLike, so we can pass it directly
                    FilterExpression {
                        _as: aerospike_core::expressions::map_val(btree_map),
                    }
                }
                _ => panic!("map_val requires a map value (HashMap)"),
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

        /// Return the debug representation of the inner expression (used for equality).
        /// Exposed for inspection; same string used by __eq__.
        pub fn _debug_inner(&self) -> String {
            format!("{:?}", self._as)
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

        //--------------------------------------------------
        // List CDT Expressions
        //--------------------------------------------------

        #[staticmethod]
        /// Create expression that returns list size.
        /// Supports nested CDT operations via optional CTX contexts.
        pub fn list_size(bin: FilterExpression, ctx: Vec<CTX>) -> Self {
            use aerospike_core::expressions::lists;
            let ctx_vec: Vec<aerospike_core::operations::cdt_context::CdtContext> =
                ctx.iter().map(|c| (&c.ctx).clone()).collect();
            FilterExpression {
                _as: lists::size(bin._as, &ctx_vec),
            }
        }

        #[staticmethod]
        /// Create expression that selects list item identified by index and returns
        /// selected data specified by returnType.
        /// Supports nested CDT operations via optional CTX contexts.
        pub fn list_get_by_index(
            return_type: ListReturnType,
            value_type: ExpType,
            index: FilterExpression,
            bin: FilterExpression,
            ctx: Vec<CTX>,
        ) -> Self {
            use aerospike_core::expressions::lists;
            let ctx_vec: Vec<aerospike_core::operations::cdt_context::CdtContext> =
                ctx.iter().map(|c| (&c.ctx).clone()).collect();
            let core_return_type: aerospike_core::operations::lists::ListReturnType = (&return_type).into();
            FilterExpression {
                _as: lists::get_by_index(
                    core_return_type,
                    (&value_type).into(),
                    index._as,
                    bin._as,
                    &ctx_vec,
                ),
            }
        }

        #[staticmethod]
        /// Create expression that selects list item identified by rank and returns
        /// selected data specified by returnType.
        /// Supports nested CDT operations via optional CTX contexts.
        pub fn list_get_by_rank(
            return_type: ListReturnType,
            value_type: ExpType,
            rank: FilterExpression,
            bin: FilterExpression,
            ctx: Vec<CTX>,
        ) -> Self {
            use aerospike_core::expressions::lists;
            let ctx_vec: Vec<aerospike_core::operations::cdt_context::CdtContext> =
                ctx.iter().map(|c| (&c.ctx).clone()).collect();
            let core_return_type: aerospike_core::operations::lists::ListReturnType = (&return_type).into();
            FilterExpression {
                _as: lists::get_by_rank(
                    core_return_type,
                    (&value_type).into(),
                    rank._as,
                    bin._as,
                    &ctx_vec,
                ),
            }
        }

        #[staticmethod]
        /// Create expression that selects list items identified by value and returns selected data
        /// specified by returnType.
        /// Supports nested CDT operations via optional CTX contexts.
        pub fn list_get_by_value(
            return_type: ListReturnType,
            value: FilterExpression,
            bin: FilterExpression,
            ctx: Vec<CTX>,
        ) -> Self {
            use aerospike_core::expressions::lists;
            let ctx_vec: Vec<aerospike_core::operations::cdt_context::CdtContext> =
                ctx.iter().map(|c| (&c.ctx).clone()).collect();
            let core_return_type: aerospike_core::operations::lists::ListReturnType = (&return_type).into();
            FilterExpression {
                _as: lists::get_by_value(
                    core_return_type,
                    value._as,
                    bin._as,
                    &ctx_vec,
                ),
            }
        }

        #[staticmethod]
        /// Create expression that selects list items identified by value range (valueBegin inclusive, valueEnd exclusive)
        /// and returns selected data specified by returnType.
        /// If valueBegin is None, the range is less than valueEnd. If valueEnd is None, the range is greater than equal to valueBegin.
        /// Supports nested CDT operations via optional CTX contexts.
        pub fn list_get_by_value_range(
            return_type: ListReturnType,
            value_begin: Option<FilterExpression>,
            value_end: Option<FilterExpression>,
            bin: FilterExpression,
            ctx: Vec<CTX>,
        ) -> Self {
            use aerospike_core::expressions::lists;
            let ctx_vec: Vec<aerospike_core::operations::cdt_context::CdtContext> =
                ctx.iter().map(|c| (&c.ctx).clone()).collect();
            let core_return_type: aerospike_core::operations::lists::ListReturnType = (&return_type).into();
            FilterExpression {
                _as: lists::get_by_value_range(
                    core_return_type,
                    value_begin.as_ref().map(|v| v._as.clone()),
                    value_end.as_ref().map(|v| v._as.clone()),
                    bin._as,
                    &ctx_vec,
                ),
            }
        }

        #[staticmethod]
        /// Create expression that selects list items identified by values and returns selected data
        /// specified by returnType.
        /// Supports nested CDT operations via optional CTX contexts.
        pub fn list_get_by_value_list(
            return_type: ListReturnType,
            values: FilterExpression,
            bin: FilterExpression,
            ctx: Vec<CTX>,
        ) -> Self {
            use aerospike_core::expressions::lists;
            let ctx_vec: Vec<aerospike_core::operations::cdt_context::CdtContext> =
                ctx.iter().map(|c| (&c.ctx).clone()).collect();
            let core_return_type: aerospike_core::operations::lists::ListReturnType = (&return_type).into();
            FilterExpression {
                _as: lists::get_by_value_list(
                    core_return_type,
                    values._as,
                    bin._as,
                    &ctx_vec,
                ),
            }
        }

        #[staticmethod]
        /// Create expression that selects list items starting at specified index to the end of list
        /// and returns selected data specified by returnType.
        /// Supports nested CDT operations via optional CTX contexts.
        pub fn list_get_by_index_range(
            return_type: ListReturnType,
            index: FilterExpression,
            bin: FilterExpression,
            ctx: Vec<CTX>,
        ) -> Self {
            use aerospike_core::expressions::lists;
            let ctx_vec: Vec<aerospike_core::operations::cdt_context::CdtContext> =
                ctx.iter().map(|c| (&c.ctx).clone()).collect();
            let core_return_type: aerospike_core::operations::lists::ListReturnType = (&return_type).into();
            FilterExpression {
                _as: lists::get_by_index_range(
                    core_return_type,
                    index._as,
                    bin._as,
                    &ctx_vec,
                ),
            }
        }

        #[staticmethod]
        /// Create expression that selects "count" list items starting at specified index
        /// and returns selected data specified by returnType.
        /// Supports nested CDT operations via optional CTX contexts.
        pub fn list_get_by_index_range_count(
            return_type: ListReturnType,
            index: FilterExpression,
            count: FilterExpression,
            bin: FilterExpression,
            ctx: Vec<CTX>,
        ) -> Self {
            use aerospike_core::expressions::lists;
            let ctx_vec: Vec<aerospike_core::operations::cdt_context::CdtContext> =
                ctx.iter().map(|c| (&c.ctx).clone()).collect();
            let core_return_type: aerospike_core::operations::lists::ListReturnType = (&return_type).into();
            FilterExpression {
                _as: lists::get_by_index_range_count(
                    core_return_type,
                    index._as,
                    count._as,
                    bin._as,
                    &ctx_vec,
                ),
            }
        }

        #[staticmethod]
        /// Create expression that selects list items starting at specified rank to the last ranked item
        /// and returns selected data specified by returnType.
        /// Supports nested CDT operations via optional CTX contexts.
        pub fn list_get_by_rank_range(
            return_type: ListReturnType,
            rank: FilterExpression,
            bin: FilterExpression,
            ctx: Vec<CTX>,
        ) -> Self {
            use aerospike_core::expressions::lists;
            let ctx_vec: Vec<aerospike_core::operations::cdt_context::CdtContext> =
                ctx.iter().map(|c| (&c.ctx).clone()).collect();
            let core_return_type: aerospike_core::operations::lists::ListReturnType = (&return_type).into();
            FilterExpression {
                _as: lists::get_by_rank_range(
                    core_return_type,
                    rank._as,
                    bin._as,
                    &ctx_vec,
                ),
            }
        }

        #[staticmethod]
        /// Create expression that selects "count" list items starting at specified rank and returns
        /// selected data specified by returnType.
        /// Supports nested CDT operations via optional CTX contexts.
        pub fn list_get_by_rank_range_count(
            return_type: ListReturnType,
            rank: FilterExpression,
            count: FilterExpression,
            bin: FilterExpression,
            ctx: Vec<CTX>,
        ) -> Self {
            use aerospike_core::expressions::lists;
            let ctx_vec: Vec<aerospike_core::operations::cdt_context::CdtContext> =
                ctx.iter().map(|c| (&c.ctx).clone()).collect();
            let core_return_type: aerospike_core::operations::lists::ListReturnType = (&return_type).into();
            FilterExpression {
                _as: lists::get_by_rank_range_count(
                    core_return_type,
                    rank._as,
                    count._as,
                    bin._as,
                    &ctx_vec,
                ),
            }
        }

        #[staticmethod]
        /// Create expression that selects list items nearest to value and greater by relative rank
        /// and returns selected data specified by returnType.
        /// Supports nested CDT operations via optional CTX contexts.
        pub fn list_get_by_value_relative_rank_range(
            return_type: ListReturnType,
            value: FilterExpression,
            rank: FilterExpression,
            bin: FilterExpression,
            ctx: Vec<CTX>,
        ) -> Self {
            use aerospike_core::expressions::lists;
            let ctx_vec: Vec<aerospike_core::operations::cdt_context::CdtContext> =
                ctx.iter().map(|c| (&c.ctx).clone()).collect();
            let core_return_type: aerospike_core::operations::lists::ListReturnType = (&return_type).into();
            FilterExpression {
                _as: lists::get_by_value_relative_rank_range(
                    core_return_type,
                    value._as,
                    rank._as,
                    bin._as,
                    &ctx_vec,
                ),
            }
        }

        #[staticmethod]
        /// Create expression that selects list items nearest to value and greater by relative rank with a count limit
        /// and returns selected data specified by returnType.
        /// Supports nested CDT operations via optional CTX contexts.
        pub fn list_get_by_value_relative_rank_range_count(
            return_type: ListReturnType,
            value: FilterExpression,
            rank: FilterExpression,
            count: FilterExpression,
            bin: FilterExpression,
            ctx: Vec<CTX>,
        ) -> Self {
            use aerospike_core::expressions::lists;
            let ctx_vec: Vec<aerospike_core::operations::cdt_context::CdtContext> =
                ctx.iter().map(|c| (&c.ctx).clone()).collect();
            let core_return_type: aerospike_core::operations::lists::ListReturnType = (&return_type).into();
            FilterExpression {
                _as: lists::get_by_value_relative_rank_range_count(
                    core_return_type,
                    value._as,
                    rank._as,
                    count._as,
                    bin._as,
                    &ctx_vec,
                ),
            }
        }

        //--------------------------------------------------
        // Map CDT Expressions
        //--------------------------------------------------

        #[staticmethod]
        /// Create expression that returns map size.
        /// Supports nested CDT operations via optional CTX contexts.
        pub fn map_size(bin: FilterExpression, ctx: Vec<CTX>) -> Self {
            use aerospike_core::expressions::maps;
            let ctx_vec: Vec<aerospike_core::operations::cdt_context::CdtContext> =
                ctx.iter().map(|c| (&c.ctx).clone()).collect();
            FilterExpression {
                _as: maps::size(bin._as, &ctx_vec),
            }
        }

        #[staticmethod]
        /// Create expression that selects map item identified by key and returns selected data
        /// specified by returnType.
        /// Supports nested CDT operations via optional CTX contexts.
        pub fn map_get_by_key(
            return_type: MapReturnType,
            value_type: ExpType,
            key: FilterExpression,
            bin: FilterExpression,
            ctx: Vec<CTX>,
        ) -> Self {
            use aerospike_core::expressions::maps;
            let ctx_vec: Vec<aerospike_core::operations::cdt_context::CdtContext> =
                ctx.iter().map(|c| (&c.ctx).clone()).collect();
            let core_return_type: aerospike_core::operations::maps::MapReturnType = (&return_type).into();
            FilterExpression {
                _as: maps::get_by_key(
                    core_return_type,
                    (&value_type).into(),
                    key._as,
                    bin._as,
                    &ctx_vec,
                ),
            }
        }

        #[staticmethod]
        /// Create expression that selects map item identified by rank and returns selected data
        /// specified by returnType.
        /// Supports nested CDT operations via optional CTX contexts.
        pub fn map_get_by_rank(
            return_type: MapReturnType,
            value_type: ExpType,
            rank: FilterExpression,
            bin: FilterExpression,
            ctx: Vec<CTX>,
        ) -> Self {
            use aerospike_core::expressions::maps;
            let ctx_vec: Vec<aerospike_core::operations::cdt_context::CdtContext> =
                ctx.iter().map(|c| (&c.ctx).clone()).collect();
            let core_return_type: aerospike_core::operations::maps::MapReturnType = (&return_type).into();
            FilterExpression {
                _as: maps::get_by_rank(
                    core_return_type,
                    (&value_type).into(),
                    rank._as,
                    bin._as,
                    &ctx_vec,
                ),
            }
        }

        #[staticmethod]
        /// Create expression that selects map item identified by index and returns selected data
        /// specified by returnType.
        /// Supports nested CDT operations via optional CTX contexts.
        pub fn map_get_by_index(
            return_type: MapReturnType,
            value_type: ExpType,
            index: FilterExpression,
            bin: FilterExpression,
            ctx: Vec<CTX>,
        ) -> Self {
            use aerospike_core::expressions::maps;
            let ctx_vec: Vec<aerospike_core::operations::cdt_context::CdtContext> =
                ctx.iter().map(|c| (&c.ctx).clone()).collect();
            let core_return_type: aerospike_core::operations::maps::MapReturnType = (&return_type).into();
            FilterExpression {
                _as: maps::get_by_index(
                    core_return_type,
                    (&value_type).into(),
                    index._as,
                    bin._as,
                    &ctx_vec,
                ),
            }
        }

        #[staticmethod]
        /// Create expression that selects map items identified by value and returns selected data
        /// specified by returnType.
        /// Supports nested CDT operations via optional CTX contexts.
        pub fn map_get_by_value(
            return_type: MapReturnType,
            value: FilterExpression,
            bin: FilterExpression,
            ctx: Vec<CTX>,
        ) -> Self {
            use aerospike_core::expressions::maps;
            let ctx_vec: Vec<aerospike_core::operations::cdt_context::CdtContext> =
                ctx.iter().map(|c| (&c.ctx).clone()).collect();
            let core_return_type: aerospike_core::operations::maps::MapReturnType = (&return_type).into();
            FilterExpression {
                _as: maps::get_by_value(
                    core_return_type,
                    value._as,
                    bin._as,
                    &ctx_vec,
                ),
            }
        }

        #[staticmethod]
        /// Create expression that selects map items identified by value range (valueBegin inclusive, valueEnd exclusive)
        /// and returns selected data specified by returnType.
        /// If valueBegin is None, the range is less than valueEnd. If valueEnd is None, the range is greater than equal to valueBegin.
        /// Supports nested CDT operations via optional CTX contexts.
        pub fn map_get_by_value_range(
            return_type: MapReturnType,
            value_begin: Option<FilterExpression>,
            value_end: Option<FilterExpression>,
            bin: FilterExpression,
            ctx: Vec<CTX>,
        ) -> Self {
            use aerospike_core::expressions::maps;
            let ctx_vec: Vec<aerospike_core::operations::cdt_context::CdtContext> =
                ctx.iter().map(|c| (&c.ctx).clone()).collect();
            let core_return_type: aerospike_core::operations::maps::MapReturnType = (&return_type).into();
            FilterExpression {
                _as: maps::get_by_value_range(
                    core_return_type,
                    value_begin.as_ref().map(|v| v._as.clone()),
                    value_end.as_ref().map(|v| v._as.clone()),
                    bin._as,
                    &ctx_vec,
                ),
            }
        }

        #[staticmethod]
        /// Create expression that selects map items identified by values and returns selected data
        /// specified by returnType.
        /// Supports nested CDT operations via optional CTX contexts.
        pub fn map_get_by_value_list(
            return_type: MapReturnType,
            values: FilterExpression,
            bin: FilterExpression,
            ctx: Vec<CTX>,
        ) -> Self {
            use aerospike_core::expressions::maps;
            let ctx_vec: Vec<aerospike_core::operations::cdt_context::CdtContext> =
                ctx.iter().map(|c| (&c.ctx).clone()).collect();
            let core_return_type: aerospike_core::operations::maps::MapReturnType = (&return_type).into();
            FilterExpression {
                _as: maps::get_by_value_list(
                    core_return_type,
                    values._as,
                    bin._as,
                    &ctx_vec,
                ),
            }
        }

        #[staticmethod]
        /// Create expression that selects map items identified by key range (keyBegin inclusive, keyEnd exclusive)
        /// and returns selected data specified by returnType.
        /// If keyBegin is None, the range is less than keyEnd. If keyEnd is None, the range is greater than equal to keyBegin.
        /// Supports nested CDT operations via optional CTX contexts.
        pub fn map_get_by_key_range(
            return_type: MapReturnType,
            key_begin: Option<FilterExpression>,
            key_end: Option<FilterExpression>,
            bin: FilterExpression,
            ctx: Vec<CTX>,
        ) -> Self {
            use aerospike_core::expressions::maps;
            let ctx_vec: Vec<aerospike_core::operations::cdt_context::CdtContext> =
                ctx.iter().map(|c| (&c.ctx).clone()).collect();
            let core_return_type: aerospike_core::operations::maps::MapReturnType = (&return_type).into();
            FilterExpression {
                _as: maps::get_by_key_range(
                    core_return_type,
                    key_begin.as_ref().map(|v| v._as.clone()),
                    key_end.as_ref().map(|v| v._as.clone()),
                    bin._as,
                    &ctx_vec,
                ),
            }
        }

        #[staticmethod]
        /// Create expression that selects map items identified by keys and returns selected data
        /// specified by returnType.
        /// Supports nested CDT operations via optional CTX contexts.
        pub fn map_get_by_key_list(
            return_type: MapReturnType,
            keys: FilterExpression,
            bin: FilterExpression,
            ctx: Vec<CTX>,
        ) -> Self {
            use aerospike_core::expressions::maps;
            let ctx_vec: Vec<aerospike_core::operations::cdt_context::CdtContext> =
                ctx.iter().map(|c| (&c.ctx).clone()).collect();
            let core_return_type: aerospike_core::operations::maps::MapReturnType = (&return_type).into();
            FilterExpression {
                _as: maps::get_by_key_list(
                    core_return_type,
                    keys._as,
                    bin._as,
                    &ctx_vec,
                ),
            }
        }

        #[staticmethod]
        /// Create expression that selects map items nearest to key and greater by index
        /// and returns selected data specified by returnType.
        /// Supports nested CDT operations via optional CTX contexts.
        pub fn map_get_by_key_relative_index_range(
            return_type: MapReturnType,
            key: FilterExpression,
            index: FilterExpression,
            bin: FilterExpression,
            ctx: Vec<CTX>,
        ) -> Self {
            use aerospike_core::expressions::maps;
            let ctx_vec: Vec<aerospike_core::operations::cdt_context::CdtContext> =
                ctx.iter().map(|c| (&c.ctx).clone()).collect();
            let core_return_type: aerospike_core::operations::maps::MapReturnType = (&return_type).into();
            FilterExpression {
                _as: maps::get_by_key_relative_index_range(
                    core_return_type,
                    key._as,
                    index._as,
                    bin._as,
                    &ctx_vec,
                ),
            }
        }

        #[staticmethod]
        /// Create expression that selects map items nearest to key and greater by index with a count limit
        /// and returns selected data specified by returnType.
        /// Supports nested CDT operations via optional CTX contexts.
        pub fn map_get_by_key_relative_index_range_count(
            return_type: MapReturnType,
            key: FilterExpression,
            index: FilterExpression,
            count: FilterExpression,
            bin: FilterExpression,
            ctx: Vec<CTX>,
        ) -> Self {
            use aerospike_core::expressions::maps;
            let ctx_vec: Vec<aerospike_core::operations::cdt_context::CdtContext> =
                ctx.iter().map(|c| (&c.ctx).clone()).collect();
            let core_return_type: aerospike_core::operations::maps::MapReturnType = (&return_type).into();
            FilterExpression {
                _as: maps::get_by_key_relative_index_range_count(
                    core_return_type,
                    key._as,
                    index._as,
                    count._as,
                    bin._as,
                    &ctx_vec,
                ),
            }
        }

        #[staticmethod]
        /// Create expression that selects map items nearest to value and greater by relative rank
        /// and returns selected data specified by returnType.
        /// Supports nested CDT operations via optional CTX contexts.
        pub fn map_get_by_value_relative_rank_range(
            return_type: MapReturnType,
            value: FilterExpression,
            rank: FilterExpression,
            bin: FilterExpression,
            ctx: Vec<CTX>,
        ) -> Self {
            use aerospike_core::expressions::maps;
            let ctx_vec: Vec<aerospike_core::operations::cdt_context::CdtContext> =
                ctx.iter().map(|c| (&c.ctx).clone()).collect();
            let core_return_type: aerospike_core::operations::maps::MapReturnType = (&return_type).into();
            FilterExpression {
                _as: maps::get_by_value_relative_rank_range(
                    core_return_type,
                    value._as,
                    rank._as,
                    bin._as,
                    &ctx_vec,
                ),
            }
        }

        #[staticmethod]
        /// Create expression that selects map items nearest to value and greater by relative rank with a count limit
        /// and returns selected data specified by returnType.
        /// Supports nested CDT operations via optional CTX contexts.
        pub fn map_get_by_value_relative_rank_range_count(
            return_type: MapReturnType,
            value: FilterExpression,
            rank: FilterExpression,
            count: FilterExpression,
            bin: FilterExpression,
            ctx: Vec<CTX>,
        ) -> Self {
            use aerospike_core::expressions::maps;
            let ctx_vec: Vec<aerospike_core::operations::cdt_context::CdtContext> =
                ctx.iter().map(|c| (&c.ctx).clone()).collect();
            let core_return_type: aerospike_core::operations::maps::MapReturnType = (&return_type).into();
            FilterExpression {
                _as: maps::get_by_value_relative_rank_range_count(
                    core_return_type,
                    value._as,
                    rank._as,
                    count._as,
                    bin._as,
                    &ctx_vec,
                ),
            }
        }

        #[staticmethod]
        /// Create expression that selects map items starting at specified index to the end of map
        /// and returns selected data specified by returnType.
        /// Supports nested CDT operations via optional CTX contexts.
        pub fn map_get_by_index_range(
            return_type: MapReturnType,
            index: FilterExpression,
            bin: FilterExpression,
            ctx: Vec<CTX>,
        ) -> Self {
            use aerospike_core::expressions::maps;
            let ctx_vec: Vec<aerospike_core::operations::cdt_context::CdtContext> =
                ctx.iter().map(|c| (&c.ctx).clone()).collect();
            let core_return_type: aerospike_core::operations::maps::MapReturnType = (&return_type).into();
            FilterExpression {
                _as: maps::get_by_index_range(
                    core_return_type,
                    index._as,
                    bin._as,
                    &ctx_vec,
                ),
            }
        }

        #[staticmethod]
        /// Create expression that selects "count" map items starting at specified index
        /// and returns selected data specified by returnType.
        /// Supports nested CDT operations via optional CTX contexts.
        pub fn map_get_by_index_range_count(
            return_type: MapReturnType,
            index: FilterExpression,
            count: FilterExpression,
            bin: FilterExpression,
            ctx: Vec<CTX>,
        ) -> Self {
            use aerospike_core::expressions::maps;
            let ctx_vec: Vec<aerospike_core::operations::cdt_context::CdtContext> =
                ctx.iter().map(|c| (&c.ctx).clone()).collect();
            let core_return_type: aerospike_core::operations::maps::MapReturnType = (&return_type).into();
            FilterExpression {
                _as: maps::get_by_index_range_count(
                    core_return_type,
                    index._as,
                    count._as,
                    bin._as,
                    &ctx_vec,
                ),
            }
        }

        #[staticmethod]
        /// Create expression that selects map items starting at specified rank to the last ranked item
        /// and returns selected data specified by returnType.
        /// Supports nested CDT operations via optional CTX contexts.
        pub fn map_get_by_rank_range(
            return_type: MapReturnType,
            rank: FilterExpression,
            bin: FilterExpression,
            ctx: Vec<CTX>,
        ) -> Self {
            use aerospike_core::expressions::maps;
            let ctx_vec: Vec<aerospike_core::operations::cdt_context::CdtContext> =
                ctx.iter().map(|c| (&c.ctx).clone()).collect();
            let core_return_type: aerospike_core::operations::maps::MapReturnType = (&return_type).into();
            FilterExpression {
                _as: maps::get_by_rank_range(
                    core_return_type,
                    rank._as,
                    bin._as,
                    &ctx_vec,
                ),
            }
        }

        #[staticmethod]
        /// Create expression that selects "count" map items starting at specified rank
        /// and returns selected data specified by returnType.
        /// Supports nested CDT operations via optional CTX contexts.
        pub fn map_get_by_rank_range_count(
            return_type: MapReturnType,
            rank: FilterExpression,
            count: FilterExpression,
            bin: FilterExpression,
            ctx: Vec<CTX>,
        ) -> Self {
            use aerospike_core::expressions::maps;
            let ctx_vec: Vec<aerospike_core::operations::cdt_context::CdtContext> =
                ctx.iter().map(|c| (&c.ctx).clone()).collect();
            let core_return_type: aerospike_core::operations::maps::MapReturnType = (&return_type).into();
            FilterExpression {
                _as: maps::get_by_rank_range_count(
                    core_return_type,
                    rank._as,
                    count._as,
                    bin._as,
                    &ctx_vec,
                ),
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
                    let mut py_partitions = Vec::new();
                    for arc_mutex_status in partitions.iter() {
                        // Use blocking_lock() which doesn't require a Tokio runtime handle
                        // This allows the property to work from Python asyncio context
                        let status_guard = arc_mutex_status.blocking_lock();
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
        _as: aerospike_core::BatchRecord,
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
        _as: aerospike_core::BatchPolicy,
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
        _as: aerospike_core::BatchReadPolicy,
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
        pub fn set_read_touch_ttl(&mut self, value: i32) {
            self._as.read_touch_ttl = match value {
                -1 => aerospike_core::ReadTouchTTL::DontReset,
                0 => aerospike_core::ReadTouchTTL::ServerDefault,
                pct if pct >= 1 && pct <= 100 => aerospike_core::ReadTouchTTL::Percent(pct as u8),
                _ => aerospike_core::ReadTouchTTL::ServerDefault,
            };
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
        _as: aerospike_core::BatchWritePolicy,
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
        _as: aerospike_core::BatchDeletePolicy,
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
        _as: aerospike_core::BatchUDFPolicy,
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
    //  AuthMode
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

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

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  TlsConfig
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[cfg(feature = "tls")]
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(
        name = "TlsConfig",
        module = "_aerospike_async_native",
        subclass,
        freelist = 100
    )]
    #[derive(Clone)]
    pub struct TlsConfig {
        _as: rustls::ClientConfig,
    }

    // Type alias to allow function signatures to compile when TLS is disabled
    #[cfg(not(feature = "tls"))]
    type TlsConfig = ();

    #[cfg(feature = "tls")]
    #[gen_stub_pymethods]
    #[pymethods]
    impl TlsConfig {
        /// Create a new TlsConfig from CA certificate file.
        ///
        /// Args:
        ///     cafile: Path to the CA certificate file (PEM format)
        ///
        /// Returns:
        ///     TlsConfig instance configured with the CA certificate
        #[new]
        #[pyo3(signature = (cafile))]
        pub fn new(cafile: String) -> PyResult<Self> {
            use rustls::{ClientConfig, RootCertStore};
            use std::fs::File;
            use std::io::BufReader;

            // Build root cert store with webpki roots and custom CA
            let mut root_store = RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.into(),
            };

            // Add custom CA certificates
            let ca_file = File::open(&cafile)
                .map_err(|e| PyErr::new::<IoError, _>(format!("Cannot open CA file {}: {}", cafile, e)))?;
            let mut ca_reader = BufReader::new(ca_file);
            let certs: Result<Vec<_>, _> = rustls_pemfile::certs(&mut ca_reader).collect();
            let certs = certs.map_err(|e| PyErr::new::<IoError, _>(format!("Cannot parse CA file {}: {}", cafile, e)))?;

            for cert in certs {
                root_store.add(cert).map_err(|e| PyErr::new::<IoError, _>(format!("Cannot add CA certificate: {}", e)))?;
            }

            // Build client config with root certificates
            let config = ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth();

            Ok(TlsConfig { _as: config })
        }

        /// Create a TlsConfig with client authentication from certificate and key files.
        ///
        /// Args:
        ///     cafile: Path to the CA certificate file (PEM format)
        ///     certfile: Path to the client certificate file (PEM format)
        ///     keyfile: Path to the client private key file (PEM format)
        ///
        /// Returns:
        ///     TlsConfig instance configured with CA and client certificates
        #[staticmethod]
        pub fn with_client_auth(cafile: String, certfile: String, keyfile: String) -> PyResult<Self> {
            use rustls::{ClientConfig, RootCertStore};
            use rustls::pki_types::{CertificateDer, PrivateKeyDer};
            use std::fs::File;
            use std::io::BufReader;

            // Build root cert store with webpki roots and custom CA
            let mut root_store = RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.into(),
            };

            // Add custom CA certificates
            let ca_file = File::open(&cafile)
                .map_err(|e| PyErr::new::<IoError, _>(format!("Cannot open CA file {}: {}", cafile, e)))?;
            let mut ca_reader = BufReader::new(ca_file);
            let certs: Result<Vec<_>, _> = rustls_pemfile::certs(&mut ca_reader).collect();
            let certs = certs.map_err(|e| PyErr::new::<IoError, _>(format!("Cannot parse CA file {}: {}", cafile, e)))?;

            for cert in certs {
                root_store.add(cert).map_err(|e| PyErr::new::<IoError, _>(format!("Cannot add CA certificate: {}", e)))?;
            }

            // Load client certificate
            let client_cert_file = File::open(&certfile)
                .map_err(|e| PyErr::new::<IoError, _>(format!("Cannot open client cert file {}: {}", certfile, e)))?;
            let mut client_cert_reader = BufReader::new(client_cert_file);
            let client_certs: Result<Vec<CertificateDer>, _> = rustls_pemfile::certs(&mut client_cert_reader).collect();
            let client_certs = client_certs.map_err(|e| PyErr::new::<IoError, _>(format!("Cannot parse client cert file {}: {}", certfile, e)))?;

            // Load client private key
            let key_file = File::open(&keyfile)
                .map_err(|e| PyErr::new::<IoError, _>(format!("Cannot open key file {}: {}", keyfile, e)))?;
            let mut key_reader = BufReader::new(key_file);
            let keys: Result<Vec<_>, _> = rustls_pemfile::pkcs8_private_keys(&mut key_reader).collect();
            let mut keys = keys.map_err(|e| PyErr::new::<IoError, _>(format!("Cannot parse key file {}: {}", keyfile, e)))?;

            let client_key = keys.pop()
                .ok_or_else(|| PyErr::new::<IoError, _>(format!("No private key found in {}", keyfile)))?;

            // Build client config with root certificates and client auth
            let config = ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_client_auth_cert(client_certs, PrivateKeyDer::Pkcs8(client_key))
                .map_err(|e| PyErr::new::<IoError, _>(format!("Cannot build TLS config with client auth: {}", e)))?;

            Ok(TlsConfig { _as: config })
        }
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

    /**********************************************************************************
     *
     * Record
     *
     **********************************************************************************/

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(subclass, freelist = 1)]
    #[derive(Clone)]
    pub struct Record {
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
        /// HLL init operation - creates or resets an HLL bin.
        HllInit(String, i64, i64, i64),
        /// HLL add operation - adds values to HLL.
        HllAdd(String, Vec<PythonValue>, i64, i64, i64),
        /// HLL get_count operation - returns estimated count.
        HllGetCount(String),
        /// HLL describe operation - returns index_bit_count and min_hash_bit_count.
        HllDescribe(String),
        /// HLL refresh_count operation - updates cached count.
        HllRefreshCount(String),
        /// HLL fold operation - folds HLL to specified index_bit_count.
        HllFold(String, i64),
        /// HLL get_union operation - returns union of HLL objects.
        HllGetUnion(String, Vec<PythonValue>),
        /// HLL get_union_count operation - returns estimated union count.
        HllGetUnionCount(String, Vec<PythonValue>),
        /// HLL get_intersect_count operation - returns estimated intersection count.
        HllGetIntersectCount(String, Vec<PythonValue>),
        /// HLL get_similarity operation - returns estimated similarity.
        HllGetSimilarity(String, Vec<PythonValue>),
        /// HLL set_union operation - sets union of HLL objects.
        HllSetUnion(String, Vec<PythonValue>, i64),
        /// Expression read operation - evaluates expression and returns result.
        ExpRead(String, FilterExpression, i64),
        /// Expression write operation - evaluates expression and writes result to bin.
        ExpWrite(String, FilterExpression, i64),
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
    //  HLLWriteFlags
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// HLL write flags for HLL operations.
    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(name = "HLLWriteFlags", module = "_aerospike_async_native", eq, eq_int)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum HLLWriteFlags {
        /// Default. Allow create or update.
        #[pyo3(name = "DEFAULT")]
        Default = 0,
        /// If the bin already exists, the operation will be denied.
        /// If the bin does not exist, a new bin will be created.
        #[pyo3(name = "CREATE_ONLY")]
        CreateOnly = 1,
        /// If the bin already exists, the bin will be overwritten.
        /// If the bin does not exist, the operation will be denied.
        #[pyo3(name = "UPDATE_ONLY")]
        UpdateOnly = 2,
        /// Do not raise error if operation is denied.
        #[pyo3(name = "NO_FAIL")]
        NoFail = 4,
        /// Allow the resulting set to be the minimum of provided index bits.
        #[pyo3(name = "ALLOW_FOLD")]
        AllowFold = 8,
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  ExpWriteFlags
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Expression write flags for expression operations.
    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(name = "ExpWriteFlags", module = "_aerospike_async_native", eq, eq_int)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ExpWriteFlags {
        /// Default. Allow create or update.
        #[pyo3(name = "DEFAULT")]
        Default = 0,
        /// If bin does not exist, a new bin will be created.
        /// If bin exists, the operation will be denied.
        #[pyo3(name = "CREATE_ONLY")]
        CreateOnly = 1,
        /// If bin exists, the bin will be overwritten.
        /// If bin does not exist, the operation will be denied.
        #[pyo3(name = "UPDATE_ONLY")]
        UpdateOnly = 2,
        /// If expression results in nil value, then delete the bin.
        #[pyo3(name = "ALLOW_DELETE")]
        AllowDelete = 4,
        /// Do not raise error if operation is denied.
        #[pyo3(name = "POLICY_NO_FAIL")]
        PolicyNoFail = 8,
        /// Ignore failures caused by the expression resolving to unknown or a non-bin type.
        #[pyo3(name = "EVAL_NO_FAIL")]
        EvalNoFail = 16,
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  ExpReadFlags
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Expression read flags for expression operations.
    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(name = "ExpReadFlags", module = "_aerospike_async_native", eq, eq_int)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ExpReadFlags {
        /// Default.
        #[pyo3(name = "DEFAULT")]
        Default = 0,
        /// Ignore failures caused by the expression resolving to unknown or a non-bin type.
        #[pyo3(name = "EVAL_NO_FAIL")]
        EvalNoFail = 16,
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  HllOperation
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// HLL (HyperLogLog) operations. Create HLL operations used by the client's `operate()` method.
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(subclass, freelist = 1000)]
    #[derive(Clone, Debug)]
    pub struct HllOperation {
        op: OperationType,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl HllOperation {
        /// Create HLL init operation.
        /// Server creates a new HLL or resets an existing HLL.
        /// Server does not return a value.
        #[staticmethod]
        #[pyo3(signature = (bin_name, index_bit_count, min_hash_bit_count = -1, flags = 0))]
        pub fn init(bin_name: String, index_bit_count: i64, min_hash_bit_count: i64, flags: i64) -> Self {
            HllOperation {
                op: OperationType::HllInit(bin_name, index_bit_count, min_hash_bit_count, flags),
            }
        }

        /// Create HLL add operation.
        /// Server adds values to HLL set. If HLL bin does not exist and index_bit_count is set,
        /// a new HLL bin will be created.
        /// Server returns number of entries that caused HLL to update a register.
        #[staticmethod]
        #[pyo3(signature = (bin_name, values, index_bit_count = -1, min_hash_bit_count = -1, flags = 0))]
        pub fn add(bin_name: String, values: Vec<PythonValue>, index_bit_count: i64, min_hash_bit_count: i64, flags: i64) -> Self {
            HllOperation {
                op: OperationType::HllAdd(bin_name, values, index_bit_count, min_hash_bit_count, flags),
            }
        }

        /// Create HLL get_count operation.
        /// Server returns estimated number of elements in the HLL bin.
        #[staticmethod]
        pub fn get_count(bin_name: String) -> Self {
            HllOperation {
                op: OperationType::HllGetCount(bin_name),
            }
        }

        /// Create HLL describe operation.
        /// Server returns index_bit_count and min_hash_bit_count used to create HLL bin
        /// in a list of longs. The list size is 2.
        #[staticmethod]
        pub fn describe(bin_name: String) -> Self {
            HllOperation {
                op: OperationType::HllDescribe(bin_name),
            }
        }

        /// Create HLL refresh_count operation.
        /// Server updates the cached count (if stale) and returns the count.
        #[staticmethod]
        pub fn refresh_count(bin_name: String) -> Self {
            HllOperation {
                op: OperationType::HllRefreshCount(bin_name),
            }
        }

        /// Create HLL fold operation.
        /// Server folds index_bit_count to the specified value.
        /// This can only be applied when min_hash_bit_count on the HLL bin is 0.
        /// Server does not return a value.
        #[staticmethod]
        pub fn fold(bin_name: String, index_bit_count: i64) -> Self {
            HllOperation {
                op: OperationType::HllFold(bin_name, index_bit_count),
            }
        }

        /// Create HLL get_union operation.
        /// Server returns an HLL object that is the union of all specified HLL objects
        /// in the list with the HLL bin.
        #[staticmethod]
        pub fn get_union(bin_name: String, hll_list: Vec<PythonValue>) -> Self {
            HllOperation {
                op: OperationType::HllGetUnion(bin_name, hll_list),
            }
        }

        /// Create HLL get_union_count operation.
        /// Server returns estimated number of elements that would be contained
        /// by the union of these HLL objects.
        #[staticmethod]
        pub fn get_union_count(bin_name: String, hll_list: Vec<PythonValue>) -> Self {
            HllOperation {
                op: OperationType::HllGetUnionCount(bin_name, hll_list),
            }
        }

        /// Create HLL get_intersect_count operation.
        /// Server returns estimated number of elements that would be contained
        /// by the intersection of these HLL objects.
        #[staticmethod]
        pub fn get_intersect_count(bin_name: String, hll_list: Vec<PythonValue>) -> Self {
            HllOperation {
                op: OperationType::HllGetIntersectCount(bin_name, hll_list),
            }
        }

        /// Create HLL get_similarity operation.
        /// Server returns estimated similarity of these HLL objects. Return type is a double.
        #[staticmethod]
        pub fn get_similarity(bin_name: String, hll_list: Vec<PythonValue>) -> Self {
            HllOperation {
                op: OperationType::HllGetSimilarity(bin_name, hll_list),
            }
        }

        /// Create HLL set_union operation.
        /// Server sets union of specified HLL objects with HLL bin.
        /// Server does not return a value.
        #[staticmethod]
        #[pyo3(signature = (bin_name, hll_list, flags = 0))]
        pub fn set_union(bin_name: String, hll_list: Vec<PythonValue>, flags: i64) -> Self {
            HllOperation {
                op: OperationType::HllSetUnion(bin_name, hll_list, flags),
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  ExpOperation
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Expression operations. Create expression operations used by the client's `operate()` method.
    /// Expression operations allow evaluating expressions on the server and optionally storing
    /// the result in a bin.
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(subclass, freelist = 1000)]
    #[derive(Clone, Debug)]
    pub struct ExpOperation {
        op: OperationType,
    }

    #[gen_stub_pymethods]
    #[pymethods]
    impl ExpOperation {
        /// Create expression read operation.
        ///
        /// Evaluates the expression and returns the result. The result is returned
        /// in the record bins with the specified name.
        ///
        /// Args:
        ///     name: Name to assign to the expression result in the returned record.
        ///     exp: Expression to evaluate.
        ///     flags: Expression read flags (default: ExpReadFlags.DEFAULT).
        ///
        /// Returns:
        ///     An ExpOperation to use with client.operate().
        #[staticmethod]
        #[pyo3(signature = (name, exp, flags = 0))]
        pub fn read(name: String, exp: FilterExpression, flags: i64) -> Self {
            ExpOperation {
                op: OperationType::ExpRead(name, exp, flags),
            }
        }

        /// Create expression write operation.
        ///
        /// Evaluates the expression and writes the result to the specified bin.
        ///
        /// Args:
        ///     bin_name: Name of bin to store expression result.
        ///     exp: Expression to evaluate.
        ///     flags: Expression write flags (default: ExpWriteFlags.DEFAULT).
        ///
        /// Returns:
        ///     An ExpOperation to use with client.operate().
        #[staticmethod]
        #[pyo3(signature = (bin_name, exp, flags = 0))]
        pub fn write(bin_name: String, exp: FilterExpression, flags: i64) -> Self {
            ExpOperation {
                op: OperationType::ExpWrite(bin_name, exp, flags),
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

        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Optional[PartitionFilter]]", imports=("typing", "aerospike_async")))]
        pub fn partition_filter<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
            let recordset = self._as.clone();

            pyo3_asyncio::future_into_py(py, async move {
                match recordset.partition_filter().await {
                    Some(pf) => Ok(Some(PartitionFilter { _as: pf })),
                    None => Ok(None),
                }
            })
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
                    } else if let Ok(py_op) = op_obj.extract::<PyRef<HllOperation>>(py) {
                        rust_ops.push(OpWithCtx {
                            op: py_op.op.clone(),
                            ctx: None,
                        });
                    } else if let Ok(py_op) = op_obj.extract::<PyRef<ExpOperation>>(py) {
                        rust_ops.push(OpWithCtx {
                            op: py_op.op.clone(),
                            ctx: None,
                        });
                    } else {
                        return Err(PyTypeError::new_err(
                            "Operation must be Operation, ListOperation, MapOperation, BitOperation, HllOperation, or ExpOperation"
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
                let mut hll_value_storage: Vec<Vec<aerospike_core::Value>> = Vec::new();
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
                        OperationType::BitGetInt(_, _, _, _) |
                        OperationType::HllInit(_, _, _, _) |
                        OperationType::HllGetCount(_) | OperationType::HllDescribe(_) |
                        OperationType::HllRefreshCount(_) | OperationType::HllFold(_, _) => {
                        }
                        OperationType::HllAdd(_, values, _, _, _) => {
                            let core_values: Vec<aerospike_core::Value> = values.iter().map(|v| v.clone().into()).collect();
                            hll_value_storage.push(core_values);
                        }
                        OperationType::HllGetUnion(_, hll_list) |
                        OperationType::HllGetUnionCount(_, hll_list) |
                        OperationType::HllGetIntersectCount(_, hll_list) |
                        OperationType::HllGetSimilarity(_, hll_list) => {
                            let core_values: Vec<aerospike_core::Value> = hll_list.iter().map(|v| v.clone().into()).collect();
                            hll_value_storage.push(core_values);
                        }
                        OperationType::HllSetUnion(_, hll_list, _) => {
                            let core_values: Vec<aerospike_core::Value> = hll_list.iter().map(|v| v.clone().into()).collect();
                            hll_value_storage.push(core_values);
                        }
                        // Expression operations don't need storage - Expression is cloned directly
                        OperationType::ExpRead(_, _, _) | OperationType::ExpWrite(_, _, _) => {}
                    }
                }

                // Second pass: convert operations, using references to stored bins/values
                let mut bin_idx = 0;
                let mut value_idx = 0;
                let mut map_idx = 0;
                let mut list_idx = 0;
                let mut hll_idx = 0;
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
                            let op = lists::set(bin_name, *index, value_storage[value_idx].clone());
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
                            let op = lists::append(&policy._as, bin_name, value_storage[value_idx].clone());
                            value_idx += 1;
                            op
                        }
                        OperationType::ListAppendItems(bin_name, values, policy) => {
                            // Use the operations module's list append_items() function with stored values and policy
                            use aerospike_core::operations::lists;
                            let values_slice: &[aerospike_core::Value] = &value_storage[value_idx..value_idx + values.len()];
                            let op = lists::append_items(&policy._as, bin_name, values_slice.to_vec());
                            value_idx += values.len();
                            op
                        }
                        OperationType::ListInsert(bin_name, index, _, policy) => {
                            // Use the operations module's list insert() function with stored value and policy
                            use aerospike_core::operations::lists;
                            let op = lists::insert(&policy._as, bin_name, *index, value_storage[value_idx].clone());
                            value_idx += 1;
                            op
                        }
                        OperationType::ListInsertItems(bin_name, index, values, policy) => {
                            // Use the operations module's list insert_items() function with stored values and policy
                            use aerospike_core::operations::lists;
                            let values_slice: &[aerospike_core::Value] = &value_storage[value_idx..value_idx + values.len()];
                            let op = lists::insert_items(&policy._as, bin_name, *index, values_slice.to_vec());
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
                            lists::set_order(bin_name, core_order, vec![])
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
                            let op = lists::get_by_value(bin_name, value.clone(), core_return_type);
                            value_idx += 1;
                            op
                        }
                        OperationType::ListGetByValueRange(bin_name, _, _, return_type) => {
                            // Use the operations module's list get_by_value_range() function with stored values and return type
                            use aerospike_core::operations::lists;
                            let begin = &value_storage[value_idx];
                            let end = &value_storage[value_idx + 1];
                            let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                            let op = lists::get_by_value_range(bin_name, begin.clone(), end.clone(), core_return_type);
                            value_idx += 2;
                            op
                        }
                        OperationType::ListGetByValueList(bin_name, _, return_type) => {
                            // Use the operations module's list get_by_value_list() function with stored list and return type
                            use aerospike_core::operations::lists;
                            let values = &list_storage[list_idx];
                            let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                            let op = lists::get_by_value_list(bin_name, values.to_vec(), core_return_type);
                            list_idx += 1;
                            op
                        }
                        OperationType::ListGetByValueRelativeRankRange(bin_name, _, rank, count, return_type) => {
                            // Use the operations module's list get_by_value_relative_rank_range() function
                            use aerospike_core::operations::lists;
                            let value = &value_storage[value_idx];
                            let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                            let op = match count {
                                Some(c) => lists::get_by_value_relative_rank_range_count(bin_name, value.clone(), *rank, *c, core_return_type),
                                None => lists::get_by_value_relative_rank_range(bin_name, value.clone(), *rank, core_return_type),
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
                            let op = lists::remove_by_value(bin_name, value.clone(), core_return_type);
                            value_idx += 1;
                            op
                        }
                        OperationType::ListRemoveByValueList(bin_name, _, return_type) => {
                            // Use the operations module's list remove_by_value_list() function with stored list and return type
                            use aerospike_core::operations::lists;
                            let values = &list_storage[list_idx];
                            let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                            let op = lists::remove_by_value_list(bin_name, values.to_vec(), core_return_type);
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
                            let op = lists::remove_by_value_range(bin_name, core_return_type, begin.clone(), end.clone());
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
                                Some(c) => lists::remove_by_value_relative_rank_range_count(bin_name, core_return_type, value.clone(), *rank, *c),
                                None => lists::remove_by_value_relative_rank_range(bin_name, core_return_type, value.clone(), *rank),
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
                            let op = maps::put(&policy._as, bin_name, key.clone(), value.clone());
                            value_idx += 2;
                            op
                        }
                        OperationType::MapPutItems(bin_name, _, policy) => {
                            // Use the operations module's map put_items() function with stored items and policy
                            use aerospike_core::operations::maps;
                            let op = maps::put_items(&policy._as, bin_name, map_storage[map_idx].clone());
                            map_idx += 1;
                            op
                        }
                        OperationType::MapIncrementValue(bin_name, _, _value, policy) => {
                            // Use the operations module's map increment_value() function with stored key, value, and policy
                            use aerospike_core::operations::maps;
                            let key = &value_storage[value_idx];
                            let incr_value = &value_storage[value_idx + 1];
                            let op = maps::increment_value(&policy._as, bin_name, key.clone(), incr_value.clone());
                            value_idx += 2;
                            op
                        }
                        OperationType::MapDecrementValue(bin_name, _, _value, policy) => {
                            // Use the operations module's map decrement_value() function with stored key, value, and policy
                            use aerospike_core::operations::maps;
                            let key = &value_storage[value_idx];
                            let decr_value = &value_storage[value_idx + 1];
                            let op = maps::decrement_value(&policy._as, bin_name, key.clone(), decr_value.clone());
                            value_idx += 2;
                            op
                        }
                        OperationType::MapGetByKey(bin_name, _, return_type) => {
                            // Use the operations module's map get_by_key() function with stored key and return type
                            use aerospike_core::operations::maps;
                            let key = &value_storage[value_idx];
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            let op = maps::get_by_key(bin_name, key.clone(), core_return_type);
                            value_idx += 1;
                            op
                        }
                        OperationType::MapRemoveByKey(bin_name, _, return_type) => {
                            // Use the operations module's map remove_by_key() function with stored key and return type
                            use aerospike_core::operations::maps;
                            let key = &value_storage[value_idx];
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            let op = maps::remove_by_key(bin_name, key.clone(), core_return_type);
                            value_idx += 1;
                            op
                        }
                        OperationType::MapGetByKeyRange(bin_name, _, _, return_type) => {
                            // Use the operations module's map get_by_key_range() function with stored keys and return type
                            use aerospike_core::operations::maps;
                            let begin = &value_storage[value_idx];
                            let end = &value_storage[value_idx + 1];
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            let op = maps::get_by_key_range(bin_name, begin.clone(), end.clone(), core_return_type);
                            value_idx += 2;
                            op
                        }
                        OperationType::MapRemoveByKeyRange(bin_name, _, _, return_type) => {
                            // Use the operations module's map remove_by_key_range() function with stored keys and return type
                            use aerospike_core::operations::maps;
                            let begin = &value_storage[value_idx];
                            let end = &value_storage[value_idx + 1];
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            let op = maps::remove_by_key_range(bin_name, begin.clone(), end.clone(), core_return_type);
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
                            let op = maps::get_by_value(bin_name, value.clone(), core_return_type);
                            value_idx += 1;
                            op
                        }
                        OperationType::MapRemoveByValue(bin_name, _, return_type) => {
                            // Use the operations module's map remove_by_value() function with stored value and return type
                            use aerospike_core::operations::maps;
                            let value = &value_storage[value_idx];
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            let op = maps::remove_by_value(bin_name, value.clone(), core_return_type);
                            value_idx += 1;
                            op
                        }
                        OperationType::MapGetByValueRange(bin_name, _, _, return_type) => {
                            // Use the operations module's map get_by_value_range() function with stored values and return type
                            use aerospike_core::operations::maps;
                            let begin = &value_storage[value_idx];
                            let end = &value_storage[value_idx + 1];
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            let op = maps::get_by_value_range(bin_name, begin.clone(), end.clone(), core_return_type);
                            value_idx += 2;
                            op
                        }
                        OperationType::MapRemoveByValueRange(bin_name, _, _, return_type) => {
                            // Use the operations module's map remove_by_value_range() function with stored values and return type
                            use aerospike_core::operations::maps;
                            let begin = &value_storage[value_idx];
                            let end = &value_storage[value_idx + 1];
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            let op = maps::remove_by_value_range(bin_name, begin.clone(), end.clone(), core_return_type);
                            value_idx += 2;
                            op
                        }
                        OperationType::MapGetByKeyList(bin_name, _, return_type) => {
                            // Use the operations module's map get_by_key_list() function with stored key list and return type
                            use aerospike_core::operations::maps;
                            let keys = &list_storage[list_idx];
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            let op = maps::get_by_key_list(bin_name, keys.to_vec(), core_return_type);
                            list_idx += 1;
                            op
                        }
                        OperationType::MapRemoveByKeyList(bin_name, _, return_type) => {
                            // Use the operations module's map remove_by_key_list() function with stored key list and return type
                            use aerospike_core::operations::maps;
                            let keys = &list_storage[list_idx];
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            let op = maps::remove_by_key_list(bin_name, keys.to_vec(), core_return_type);
                            list_idx += 1;
                            op
                        }
                        OperationType::MapGetByValueList(bin_name, _, return_type) => {
                            // Use the operations module's map get_by_value_list() function with stored value list and return type
                            use aerospike_core::operations::maps;
                            let values = &list_storage[list_idx];
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            let op = maps::get_by_value_list(bin_name, values.to_vec(), core_return_type);
                            list_idx += 1;
                            op
                        }
                        OperationType::MapRemoveByValueList(bin_name, _, return_type) => {
                            // Use the operations module's map remove_by_value_list() function with stored value list and return type
                            use aerospike_core::operations::maps;
                            let values = &list_storage[list_idx];
                            let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                            let op = maps::remove_by_value_list(bin_name, values.to_vec(), core_return_type);
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
                                Some(c) => maps::get_by_key_relative_index_range_count(bin_name, key.clone(), *index, *c, core_return_type),
                                None => maps::get_by_key_relative_index_range(bin_name, key.clone(), *index, core_return_type),
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
                                Some(c) => maps::get_by_value_relative_rank_range_count(bin_name, value.clone(), *rank, *c, core_return_type),
                                None => maps::get_by_value_relative_rank_range(bin_name, value.clone(), *rank, core_return_type),
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
                                Some(c) => maps::remove_by_key_relative_index_range_count(bin_name, key.clone(), *index, *c, core_return_type),
                                None => maps::remove_by_key_relative_index_range(bin_name, key.clone(), *index, core_return_type),
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
                                Some(c) => maps::remove_by_value_relative_rank_range_count(bin_name, value.clone(), *rank, *c, core_return_type),
                                None => maps::remove_by_value_relative_rank_range(bin_name, value.clone(), *rank, core_return_type),
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
                            let op = bitwise::insert(bin_name, *byte_offset, value.clone(), &policy._as);
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
                            let op = bitwise::set(bin_name, *bit_offset, *bit_size, value.clone(), &policy._as);
                            value_idx += 1;
                            op
                        }
                        OperationType::BitOr(bin_name, bit_offset, bit_size, _, policy) => {
                            use aerospike_core::operations::bitwise;
                            let value = &value_storage[value_idx];
                            let op = bitwise::or(bin_name, *bit_offset, *bit_size, value.clone(), &policy._as);
                            value_idx += 1;
                            op
                        }
                        OperationType::BitXor(bin_name, bit_offset, bit_size, _, policy) => {
                            use aerospike_core::operations::bitwise;
                            let value = &value_storage[value_idx];
                            let op = bitwise::xor(bin_name, *bit_offset, *bit_size, value.clone(), &policy._as);
                            value_idx += 1;
                            op
                        }
                        OperationType::BitAnd(bin_name, bit_offset, bit_size, _, policy) => {
                            use aerospike_core::operations::bitwise;
                            let value = &value_storage[value_idx];
                            let op = bitwise::and(bin_name, *bit_offset, *bit_size, value.clone(), &policy._as);
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
                        OperationType::HllInit(bin_name, index_bit_count, min_hash_bit_count, flags) => {
                            use aerospike_core::operations::hll;
                            let policy = hll::HLLPolicy { flags: *flags };
                            hll::init_with_min_hash(&policy, bin_name, *index_bit_count, *min_hash_bit_count)
                        }
                        OperationType::HllAdd(bin_name, _, index_bit_count, min_hash_bit_count, flags) => {
                            use aerospike_core::operations::hll;
                            let policy = hll::HLLPolicy { flags: *flags };
                            let values_ref = &hll_value_storage[hll_idx];
                            hll_idx += 1;
                            hll::add_with_index_and_min_hash(&policy, bin_name, values_ref.to_vec(), *index_bit_count, *min_hash_bit_count)
                        }
                        OperationType::HllGetCount(bin_name) => {
                            use aerospike_core::operations::hll;
                            hll::get_count(bin_name)
                        }
                        OperationType::HllDescribe(bin_name) => {
                            use aerospike_core::operations::hll;
                            hll::describe(bin_name)
                        }
                        OperationType::HllRefreshCount(bin_name) => {
                            use aerospike_core::operations::hll;
                            hll::refresh_count(bin_name)
                        }
                        OperationType::HllFold(bin_name, index_bit_count) => {
                            use aerospike_core::operations::hll;
                            hll::fold(bin_name, *index_bit_count)
                        }
                        OperationType::HllGetUnion(bin_name, _) => {
                            use aerospike_core::operations::hll;
                            let values_ref = &hll_value_storage[hll_idx];
                            hll_idx += 1;
                            hll::get_union(bin_name, values_ref.to_vec())
                        }
                        OperationType::HllGetUnionCount(bin_name, _) => {
                            use aerospike_core::operations::hll;
                            let values_ref = &hll_value_storage[hll_idx];
                            hll_idx += 1;
                            hll::get_union_count(bin_name, values_ref.to_vec())
                        }
                        OperationType::HllGetIntersectCount(bin_name, _) => {
                            use aerospike_core::operations::hll;
                            let values_ref = &hll_value_storage[hll_idx];
                            hll_idx += 1;
                            hll::get_intersect_count(bin_name, values_ref.to_vec())
                        }
                        OperationType::HllGetSimilarity(bin_name, _) => {
                            use aerospike_core::operations::hll;
                            let values_ref = &hll_value_storage[hll_idx];
                            hll_idx += 1;
                            hll::get_similarity(bin_name, values_ref.to_vec())
                        }
                        OperationType::HllSetUnion(bin_name, _, flags) => {
                            use aerospike_core::operations::hll;
                            let policy = hll::HLLPolicy { flags: *flags };
                            let values_ref = &hll_value_storage[hll_idx];
                            hll_idx += 1;
                            hll::set_union(&policy, bin_name, values_ref.to_vec())
                        }
                        OperationType::ExpRead(name, exp, flags) => {
                            use aerospike_core::operations::exp::{self, ExpReadFlags};
                            // Convert flags bitmask to core ExpReadFlags
                            let mut core_flags: Vec<ExpReadFlags> = Vec::new();
                            if *flags & 16 != 0 {
                                core_flags.push(ExpReadFlags::EvalNoFail);
                            }
                            if core_flags.is_empty() {
                                exp::read_exp(name, exp._as.clone(), ExpReadFlags::Default)
                            } else {
                                exp::read_exp(name, exp._as.clone(), core_flags)
                            }
                        }
                        OperationType::ExpWrite(bin_name, exp, flags) => {
                            use aerospike_core::operations::exp::{self, ExpWriteFlags};
                            // Convert flags bitmask to core ExpWriteFlags
                            let mut core_flags: Vec<ExpWriteFlags> = Vec::new();
                            if *flags & 1 != 0 {
                                core_flags.push(ExpWriteFlags::CreateOnly);
                            }
                            if *flags & 2 != 0 {
                                core_flags.push(ExpWriteFlags::UpdateOnly);
                            }
                            if *flags & 4 != 0 {
                                core_flags.push(ExpWriteFlags::AllowDelete);
                            }
                            if *flags & 8 != 0 {
                                core_flags.push(ExpWriteFlags::PolicyNoFail);
                            }
                            if *flags & 16 != 0 {
                                core_flags.push(ExpWriteFlags::EvalNoFail);
                            }
                            if core_flags.is_empty() {
                                exp::write_exp(bin_name, exp._as.clone(), ExpWriteFlags::Default)
                            } else {
                                exp::write_exp(bin_name, exp._as.clone(), core_flags)
                            }
                        }
                    };

                    // Apply context if present
                    let final_op = if let Some(ctx) = &op_with_ctx.ctx {
                        core_op.set_context(ctx.as_slice().to_vec())
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

        /// Read multiple records for specified keys in one batch call.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.List[BatchRecord]]", imports=("typing")))]
        #[pyo3(signature = (batch_policy, read_policy, keys, bins = None))]
        pub fn batch_read<'a>(
            &self,
            batch_policy: Option<&BatchPolicy>,
            read_policy: Option<&BatchReadPolicy>,
            keys: Vec<Py<PyAny>>,
            bins: Option<Vec<String>>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let batch_policy = batch_policy.map(|p| p._as.clone()).unwrap_or_default();
            let read_policy = read_policy.map(|p| p._as.clone()).unwrap_or_default();

            // Extract Key objects from Python list
            let mut rust_keys = Vec::new();
            for key_obj in keys {
                Python::attach(|py| {
                    let key = key_obj.extract::<PyRef<Key>>(py)?;
                    rust_keys.push(key._as.clone());
                    Ok::<(), PyErr>(())
                })?;
            }
            let keys = rust_keys;
            let client = self._as.clone();

            pyo3_asyncio::future_into_py(py, async move {
                use aerospike_core::BatchOperation;

                let mut batch_ops = Vec::new();
                for key in keys {
                    let op = BatchOperation::read(&read_policy, key, bins_flag(bins.clone()));
                    batch_ops.push(op);
                }

                let results = client
                    .read()
                    .await
                    .batch(&batch_policy, &batch_ops)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|_py| {
                    let py_results: Vec<BatchRecord> = results
                        .into_iter()
                        .map(|br| BatchRecord { _as: br })
                        .collect();
                    Ok(py_results)
                })
            })
        }

        /// Write multiple records for specified keys in one batch call.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.List[BatchRecord]]", imports=("typing")))]
        pub fn batch_write<'a>(
            &self,
            batch_policy: Option<&BatchPolicy>,
            write_policy: Option<&BatchWritePolicy>,
            keys: Vec<Py<PyAny>>,
            bins_list: Vec<Py<PyAny>>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            if keys.len() != bins_list.len() {
                return Err(PyValueError::new_err("keys and bins_list must have the same length"));
            }

            let batch_policy = batch_policy.map(|p| p._as.clone()).unwrap_or_default();
            let write_policy = write_policy.map(|p| p._as.clone()).unwrap_or_default();
            let client = self._as.clone();

            // Extract Key objects and PyDicts from Python lists
            let mut rust_keys = Vec::new();
            let mut bins_vecs = Vec::new();
            for (key_obj, bins_obj) in keys.into_iter().zip(bins_list.into_iter()) {
                Python::attach(|py| {
                    let key = key_obj.extract::<PyRef<Key>>(py)?;
                    rust_keys.push(key._as.clone());

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
                    Ok::<(), PyErr>(())
                })?;
            }
            let keys = rust_keys;

            pyo3_asyncio::future_into_py(py, async move {
                use aerospike_core::BatchOperation;
                use aerospike_core::operations;

                // Create all operations while bins_vecs is still in scope
                let mut all_ops: Vec<Vec<aerospike_core::operations::Operation>> = Vec::new();
                for bins in &bins_vecs {
                    let ops: Vec<aerospike_core::operations::Operation> = bins
                        .iter()
                        .map(|bin| operations::put(bin))
                        .collect();
                    all_ops.push(ops);
                }

                // Now create batch operations
                let mut batch_ops = Vec::new();
                for (key, ops) in keys.into_iter().zip(all_ops.into_iter()) {
                    let op = BatchOperation::write(&write_policy, key, ops);
                    batch_ops.push(op);
                }

                let results = client
                    .read()
                    .await
                    .batch(&batch_policy, &batch_ops)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|_py| {
                    let py_results: Vec<BatchRecord> = results
                        .into_iter()
                        .map(|br| BatchRecord { _as: br })
                        .collect();
                    Ok(py_results)
                })
            })
        }

        /// Perform read/write operations on multiple keys in one batch call.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.List[BatchRecord]]", imports=("typing")))]
        pub fn batch_operate<'a>(
            &self,
            batch_policy: Option<&BatchPolicy>,
            write_policy: Option<&BatchWritePolicy>,
            keys: Vec<Py<PyAny>>,
            operations_list: Vec<Vec<Py<PyAny>>>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            if keys.len() != operations_list.len() {
                return Err(PyValueError::new_err("keys and operations_list must have the same length"));
            }

            let batch_policy = batch_policy.map(|p| p._as.clone()).unwrap_or_default();
            let write_policy = write_policy.map(|p| p._as.clone()).unwrap_or_default();
            let client = self._as.clone();

            // Extract Key objects from Python list
            let mut rust_keys = Vec::new();
            for key_obj in keys {
                Python::attach(|py| {
                    let key = key_obj.extract::<PyRef<Key>>(py)?;
                    rust_keys.push(key._as.clone());
                    Ok::<(), PyErr>(())
                })?;
            }
            let keys = rust_keys;

            // Extract operations before moving into async block
            let mut rust_ops_list = Vec::new();
            for operations in operations_list {
                let mut rust_ops: Vec<OperationType> = Vec::new();
                for op_obj in operations {
                    Python::attach(|py| {
                        if let Ok(py_op) = op_obj.extract::<PyRef<Operation>>(py) {
                            rust_ops.push(py_op.op.clone());
                        } else if let Ok(py_op) = op_obj.extract::<PyRef<ListOperation>>(py) {
                            rust_ops.push(py_op.op.clone());
                        } else if let Ok(py_op) = op_obj.extract::<PyRef<MapOperation>>(py) {
                            rust_ops.push(py_op.op.clone());
                        } else if let Ok(py_op) = op_obj.extract::<PyRef<BitOperation>>(py) {
                            rust_ops.push(py_op.op.clone());
                        } else if let Ok(py_op) = op_obj.extract::<PyRef<HllOperation>>(py) {
                            rust_ops.push(py_op.op.clone());
                        } else {
                            return Err(PyTypeError::new_err(
                                "Operation must be Operation, ListOperation, MapOperation, BitOperation, or HllOperation"
                            ));
                        }
                        Ok::<(), PyErr>(())
                    })?;
                }
                rust_ops_list.push(rust_ops);
            }

            pyo3_asyncio::future_into_py(py, async move {
                use aerospike_core::BatchOperation;
                use aerospike_core::operations;

                let read_policy = aerospike_core::BatchReadPolicy::default();

                // First pass: collect all bins/values that need to live as long as the operations
                let mut bin_storage: Vec<aerospike_core::Bin> = Vec::new();
                let mut value_storage: Vec<aerospike_core::Value> = Vec::new();
                let mut map_storage: Vec<HashMap<aerospike_core::Value, aerospike_core::Value>> = Vec::new();
                let mut list_storage: Vec<Vec<aerospike_core::Value>> = Vec::new();

                for ops in &rust_ops_list {
                    for op in ops {
                        match op {
                            OperationType::Put(bin_name, value) |
                            OperationType::Add(bin_name, value) |
                            OperationType::Append(bin_name, value) |
                            OperationType::Prepend(bin_name, value) => {
                                let bin = aerospike_core::Bin::new(bin_name.clone(), value.clone().into());
                                bin_storage.push(bin);
                            }
                            OperationType::ListSet(_, _, value) => {
                                value_storage.push(value.clone().into());
                            }
                            OperationType::ListAppend(_, value, _) => {
                                value_storage.push(value.clone().into());
                            }
                            OperationType::ListAppendItems(_, values, _) => {
                                for value in values {
                                    value_storage.push(value.clone().into());
                                }
                            }
                            OperationType::ListInsert(_, _, value, _) => {
                                value_storage.push(value.clone().into());
                            }
                            OperationType::ListInsertItems(_, _, values, _) => {
                                for value in values {
                                    value_storage.push(value.clone().into());
                                }
                            }
                            OperationType::ListGetByValue(_, value, _) => {
                                value_storage.push(value.clone().into());
                            }
                            OperationType::ListGetByValueRange(_, begin, end, _) => {
                                value_storage.push(begin.clone().into());
                                value_storage.push(end.clone().into());
                            }
                            OperationType::ListGetByValueList(_, values, _) => {
                                let mut value_list = Vec::new();
                                for value in values {
                                    value_list.push(value.clone().into());
                                }
                                list_storage.push(value_list);
                            }
                            OperationType::ListGetByValueRelativeRankRange(_, value, _, _, _) => {
                                value_storage.push(value.clone().into());
                            }
                            OperationType::ListRemoveByValue(_, value, _) => {
                                value_storage.push(value.clone().into());
                            }
                            OperationType::ListRemoveByValueList(_, values, _) => {
                                let mut value_list = Vec::new();
                                for value in values {
                                    value_list.push(value.clone().into());
                                }
                                list_storage.push(value_list);
                            }
                            OperationType::ListRemoveByValueRange(_, begin, end, _) => {
                                value_storage.push(begin.clone().into());
                                value_storage.push(end.clone().into());
                            }
                            OperationType::ListRemoveByValueRelativeRankRange(_, value, _, _, _) => {
                                value_storage.push(value.clone().into());
                            }
                            OperationType::MapPut(_, key, value, _) => {
                                value_storage.push(key.clone().into());
                                value_storage.push(value.clone().into());
                            }
                            OperationType::MapPutItems(_, items, _) => {
                                let mut map = HashMap::new();
                                for (key, value) in items {
                                    map.insert(key.clone().into(), value.clone().into());
                                }
                                map_storage.push(map);
                            }
                            OperationType::MapIncrementValue(_, key, value, _) | OperationType::MapDecrementValue(_, key, value, _) => {
                                value_storage.push(key.clone().into());
                                value_storage.push(aerospike_core::Value::Int(*value));
                            }
                            OperationType::MapGetByKey(_, key, _) | OperationType::MapRemoveByKey(_, key, _) => {
                                value_storage.push(key.clone().into());
                            }
                            OperationType::MapGetByKeyRange(_, begin, end, _) | OperationType::MapRemoveByKeyRange(_, begin, end, _) => {
                                value_storage.push(begin.clone().into());
                                value_storage.push(end.clone().into());
                            }
                            OperationType::MapGetByValue(_, value, _) | OperationType::MapRemoveByValue(_, value, _) => {
                                value_storage.push(value.clone().into());
                            }
                            OperationType::MapGetByValueRange(_, begin, end, _) | OperationType::MapRemoveByValueRange(_, begin, end, _) => {
                                value_storage.push(begin.clone().into());
                                value_storage.push(end.clone().into());
                            }
                            OperationType::MapGetByKeyList(_, keys, _) | OperationType::MapRemoveByKeyList(_, keys, _) => {
                                let mut key_list = Vec::new();
                                for key in keys {
                                    key_list.push(key.clone().into());
                                }
                                list_storage.push(key_list);
                            }
                            OperationType::MapGetByValueList(_, values, _) | OperationType::MapRemoveByValueList(_, values, _) => {
                                let mut value_list = Vec::new();
                                for value in values {
                                    value_list.push(value.clone().into());
                                }
                                list_storage.push(value_list);
                            }
                            OperationType::MapGetByKeyRelativeIndexRange(_, key, _, _, _) | OperationType::MapRemoveByKeyRelativeIndexRange(_, key, _, _, _) => {
                                value_storage.push(key.clone().into());
                            }
                            OperationType::MapGetByValueRelativeRankRange(_, value, _, _, _) | OperationType::MapRemoveByValueRelativeRankRange(_, value, _, _, _) => {
                                value_storage.push(value.clone().into());
                            }
                            OperationType::BitInsert(_, _, value, _) | OperationType::BitSet(_, _, _, value, _) |
                            OperationType::BitOr(_, _, _, value, _) | OperationType::BitXor(_, _, _, value, _) |
                            OperationType::BitAnd(_, _, _, value, _) => {
                                value_storage.push(value.clone().into());
                            }
                            _ => {}
                        }
                    }
                }

                // Second pass: convert operations to Rust operations for each key
                let mut batch_ops = Vec::new();
                let mut bin_idx = 0;
                let mut value_idx = 0;
                let mut map_idx = 0;
                let mut list_idx = 0;

                for (key, ops) in keys.into_iter().zip(rust_ops_list.iter()) {
                    let mut core_ops = Vec::new();
                    let mut has_write_op = false;
                    // First, check if any operation is a write operation
                    for op in ops.iter() {
                        match op {
                            OperationType::Put(_, _) | OperationType::Add(_, _) | OperationType::Append(_, _) |
                            OperationType::Prepend(_, _) | OperationType::Delete() | OperationType::Touch() |
                            OperationType::ListSet(_, _, _) | OperationType::ListAppend(_, _, _) |
                            OperationType::ListAppendItems(_, _, _) | OperationType::ListInsert(_, _, _, _) |
                            OperationType::ListInsertItems(_, _, _, _) | OperationType::ListIncrement(_, _, _, _) |
                            OperationType::ListSort(_, _) | OperationType::ListSetOrder(_, _) |
                            OperationType::ListRemove(_, _) | OperationType::ListRemoveRange(_, _, _) |
                            OperationType::ListRemoveRangeFrom(_, _) | OperationType::ListPop(_, _) |
                            OperationType::ListPopRange(_, _, _) | OperationType::ListPopRangeFrom(_, _) |
                            OperationType::ListTrim(_, _, _) | OperationType::ListClear(_) |
                            OperationType::ListCreate(_, _, _, _) |
                            OperationType::ListRemoveByIndex(_, _, _) | OperationType::ListRemoveByIndexRange(_, _, _, _) |
                            OperationType::ListRemoveByRank(_, _, _) | OperationType::ListRemoveByRankRange(_, _, _, _) |
                            OperationType::ListRemoveByValue(_, _, _) | OperationType::ListRemoveByValueList(_, _, _) |
                            OperationType::ListRemoveByValueRange(_, _, _, _) |
                            OperationType::ListRemoveByValueRelativeRankRange(_, _, _, _, _) |
                            OperationType::MapPut(_, _, _, _) | OperationType::MapPutItems(_, _, _) |
                            OperationType::MapIncrementValue(_, _, _, _) | OperationType::MapDecrementValue(_, _, _, _) |
                            OperationType::MapClear(_) | OperationType::MapSetMapPolicy(_, _) |
                            OperationType::MapCreate(_, _) | OperationType::MapRemoveByKey(_, _, _) |
                            OperationType::MapRemoveByKeyRange(_, _, _, _) | OperationType::MapRemoveByIndex(_, _, _) |
                            OperationType::MapRemoveByIndexRange(_, _, _, _) |
                            OperationType::MapRemoveByIndexRangeFrom(_, _, _) | OperationType::MapRemoveByRank(_, _, _) |
                            OperationType::MapRemoveByRankRange(_, _, _, _) |
                            OperationType::MapRemoveByRankRangeFrom(_, _, _) | OperationType::MapRemoveByValue(_, _, _) |
                            OperationType::MapRemoveByValueRange(_, _, _, _) | OperationType::MapRemoveByKeyList(_, _, _) |
                            OperationType::MapRemoveByValueList(_, _, _) |
                            OperationType::MapRemoveByKeyRelativeIndexRange(_, _, _, _, _) |
                            OperationType::MapRemoveByValueRelativeRankRange(_, _, _, _, _) |
                            OperationType::BitResize(_, _, _, _) | OperationType::BitInsert(_, _, _, _) |
                            OperationType::BitRemove(_, _, _, _) | OperationType::BitSet(_, _, _, _, _) |
                            OperationType::BitOr(_, _, _, _, _) | OperationType::BitXor(_, _, _, _, _) |
                            OperationType::BitAnd(_, _, _, _, _) | OperationType::BitNot(_, _, _, _) |
                            OperationType::BitLShift(_, _, _, _, _) | OperationType::BitRShift(_, _, _, _, _) |
                            OperationType::BitAdd(_, _, _, _, _, _, _) | OperationType::BitSubtract(_, _, _, _, _, _, _) |
                            OperationType::BitSetInt(_, _, _, _, _) => {
                                has_write_op = true;
                            }
                            _ => {}
                        }
                    }
                    for op in ops {
                        let core_op = match op {
                            OperationType::Get() => {
                                operations::get()
                            }
                            OperationType::GetBin(bin_name) => {
                                operations::get_bin(&bin_name)
                            }
                            OperationType::GetHeader() => {
                                operations::get_header()
                            }
                            OperationType::Put(_, _) => {
                                let op = operations::put(&bin_storage[bin_idx]);
                                bin_idx += 1;
                                op
                            }
                            OperationType::Add(_, _) => {
                                let op = operations::add(&bin_storage[bin_idx]);
                                bin_idx += 1;
                                op
                            }
                            OperationType::Append(_, _) => {
                                let op = operations::append(&bin_storage[bin_idx]);
                                bin_idx += 1;
                                op
                            }
                            OperationType::Prepend(_, _) => {
                                let op = operations::prepend(&bin_storage[bin_idx]);
                                bin_idx += 1;
                                op
                            }
                            OperationType::Delete() => {
                                operations::delete()
                            }
                            OperationType::Touch() => {
                                operations::touch()
                            }
                            OperationType::ListGet(bin_name, index) => {
                                use aerospike_core::operations::lists;
                                lists::get(bin_name, *index)
                            }
                            OperationType::ListSize(bin_name) => {
                                use aerospike_core::operations::lists;
                                lists::size(bin_name)
                            }
                            OperationType::ListPop(bin_name, index) => {
                                use aerospike_core::operations::lists;
                                lists::pop(bin_name, *index)
                            }
                            OperationType::ListClear(bin_name) => {
                                use aerospike_core::operations::lists;
                                lists::clear(bin_name)
                            }
                            OperationType::ListGetRange(bin_name, index, count) => {
                                use aerospike_core::operations::lists;
                                lists::get_range(bin_name, *index, *count)
                            }
                            OperationType::ListSet(bin_name, index, _) => {
                                use aerospike_core::operations::lists;
                                let op = lists::set(bin_name, *index, value_storage[value_idx].clone());
                                value_idx += 1;
                                op
                            }
                            OperationType::ListRemove(bin_name, index) => {
                                use aerospike_core::operations::lists;
                                lists::remove(bin_name, *index)
                            }
                            OperationType::ListRemoveRange(bin_name, index, count) => {
                                use aerospike_core::operations::lists;
                                lists::remove_range(bin_name, *index, *count)
                            }
                            OperationType::ListGetRangeFrom(bin_name, index) => {
                                use aerospike_core::operations::lists;
                                lists::get_range_from(bin_name, *index)
                            }
                            OperationType::ListPopRange(bin_name, index, count) => {
                                use aerospike_core::operations::lists;
                                lists::pop_range(bin_name, *index, *count)
                            }
                            OperationType::ListPopRangeFrom(bin_name, index) => {
                                use aerospike_core::operations::lists;
                                lists::pop_range_from(bin_name, *index)
                            }
                            OperationType::ListRemoveRangeFrom(bin_name, index) => {
                                use aerospike_core::operations::lists;
                                lists::remove_range_from(bin_name, *index)
                            }
                            OperationType::ListTrim(bin_name, index, count) => {
                                use aerospike_core::operations::lists;
                                lists::trim(bin_name, *index, *count)
                            }
                            OperationType::ListAppend(bin_name, _, policy) => {
                                use aerospike_core::operations::lists;
                                let op = lists::append(&policy._as, bin_name, value_storage[value_idx].clone());
                                value_idx += 1;
                                op
                            }
                            OperationType::ListAppendItems(bin_name, values, policy) => {
                                use aerospike_core::operations::lists;
                                let values_slice: &[aerospike_core::Value] = &value_storage[value_idx..value_idx + values.len()];
                                let op = lists::append_items(&policy._as, bin_name, values_slice.to_vec());
                                value_idx += values.len();
                                op
                            }
                            OperationType::ListInsert(bin_name, index, _, policy) => {
                                use aerospike_core::operations::lists;
                                let op = lists::insert(&policy._as, bin_name, *index, value_storage[value_idx].clone());
                                value_idx += 1;
                                op
                            }
                            OperationType::ListInsertItems(bin_name, index, values, policy) => {
                                use aerospike_core::operations::lists;
                                let values_slice: &[aerospike_core::Value] = &value_storage[value_idx..value_idx + values.len()];
                                let op = lists::insert_items(&policy._as, bin_name, *index, values_slice.to_vec());
                                value_idx += values.len();
                                op
                            }
                            OperationType::ListIncrement(bin_name, index, value, policy) => {
                                use aerospike_core::operations::lists;
                                lists::increment(&policy._as, bin_name, *index, *value)
                            }
                            OperationType::ListSort(bin_name, flags) => {
                                use aerospike_core::operations::lists;
                                let core_flags: aerospike_core::operations::lists::ListSortFlags = flags.into();
                                lists::sort(bin_name, core_flags)
                            }
                            OperationType::ListSetOrder(bin_name, order) => {
                                use aerospike_core::operations::lists;
                                let core_order: aerospike_core::operations::lists::ListOrderType = order.into();
                                lists::set_order(bin_name, core_order, vec![])
                            }
                            OperationType::ListGetByIndex(bin_name, index, return_type) => {
                                use aerospike_core::operations::lists;
                                let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                                lists::get_by_index(bin_name, *index, core_return_type)
                            }
                            OperationType::ListGetByIndexRange(bin_name, index, count, return_type) => {
                                use aerospike_core::operations::lists;
                                let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                                match count {
                                    Some(c) => lists::get_by_index_range_count(bin_name, *index, *c, core_return_type),
                                    None => lists::get_by_index_range(bin_name, *index, core_return_type),
                                }
                            }
                            OperationType::ListGetByRank(bin_name, rank, return_type) => {
                                use aerospike_core::operations::lists;
                                let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                                lists::get_by_rank(bin_name, *rank, core_return_type)
                            }
                            OperationType::ListGetByRankRange(bin_name, rank, count, return_type) => {
                                use aerospike_core::operations::lists;
                                let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                                match count {
                                    Some(c) => lists::get_by_rank_range_count(bin_name, *rank, *c, core_return_type),
                                    None => lists::get_by_rank_range(bin_name, *rank, core_return_type),
                                }
                            }
                            OperationType::ListGetByValue(bin_name, _, return_type) => {
                                use aerospike_core::operations::lists;
                                let value = &value_storage[value_idx];
                                let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                                let op = lists::get_by_value(bin_name, value.clone(), core_return_type);
                                value_idx += 1;
                                op
                            }
                            OperationType::ListGetByValueRange(bin_name, _, _, return_type) => {
                                use aerospike_core::operations::lists;
                                let begin = &value_storage[value_idx];
                                let end = &value_storage[value_idx + 1];
                                let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                                let op = lists::get_by_value_range(bin_name, begin.clone(), end.clone(), core_return_type);
                                value_idx += 2;
                                op
                            }
                            OperationType::ListGetByValueList(bin_name, _, return_type) => {
                                use aerospike_core::operations::lists;
                                let values = &list_storage[list_idx];
                                let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                                let op = lists::get_by_value_list(bin_name, values.to_vec(), core_return_type);
                                list_idx += 1;
                                op
                            }
                            OperationType::ListGetByValueRelativeRankRange(bin_name, _, rank, count, return_type) => {
                                use aerospike_core::operations::lists;
                                let value = &value_storage[value_idx];
                                let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                                let op = match count {
                                    Some(c) => lists::get_by_value_relative_rank_range_count(bin_name, value.clone(), *rank, *c, core_return_type),
                                    None => lists::get_by_value_relative_rank_range(bin_name, value.clone(), *rank, core_return_type),
                                };
                                value_idx += 1;
                                op
                            }
                            OperationType::ListRemoveByIndex(bin_name, index, return_type) => {
                                use aerospike_core::operations::lists;
                                let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                                lists::remove_by_index(bin_name, *index, core_return_type)
                            }
                            OperationType::ListRemoveByIndexRange(bin_name, index, count, return_type) => {
                                use aerospike_core::operations::lists;
                                let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                                match count {
                                    Some(c) => lists::remove_by_index_range_count(bin_name, *index, *c, core_return_type),
                                    None => lists::remove_by_index_range(bin_name, *index, core_return_type),
                                }
                            }
                            OperationType::ListRemoveByRank(bin_name, rank, return_type) => {
                                use aerospike_core::operations::lists;
                                let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                                lists::remove_by_rank(bin_name, *rank, core_return_type)
                            }
                            OperationType::ListRemoveByRankRange(bin_name, rank, count, return_type) => {
                                use aerospike_core::operations::lists;
                                let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                                match count {
                                    Some(c) => lists::remove_by_rank_range_count(bin_name, *rank, *c, core_return_type),
                                    None => lists::remove_by_rank_range(bin_name, *rank, core_return_type),
                                }
                            }
                            OperationType::ListRemoveByValue(bin_name, _, return_type) => {
                                use aerospike_core::operations::lists;
                                let value = &value_storage[value_idx];
                                let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                                let op = lists::remove_by_value(bin_name, value.clone(), core_return_type);
                                value_idx += 1;
                                op
                            }
                            OperationType::ListRemoveByValueList(bin_name, _, return_type) => {
                                use aerospike_core::operations::lists;
                                let values = &list_storage[list_idx];
                                let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                                let op = lists::remove_by_value_list(bin_name, values.to_vec(), core_return_type);
                                list_idx += 1;
                                op
                            }
                            OperationType::ListRemoveByValueRange(bin_name, _, _, return_type) => {
                                use aerospike_core::operations::lists;
                                let begin = &value_storage[value_idx];
                                let end = &value_storage[value_idx + 1];
                                let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                                let op = lists::remove_by_value_range(bin_name, core_return_type, begin.clone(), end.clone());
                                value_idx += 2;
                                op
                            }
                            OperationType::ListRemoveByValueRelativeRankRange(bin_name, _, rank, count, return_type) => {
                                use aerospike_core::operations::lists;
                                let value = &value_storage[value_idx];
                                let core_return_type: aerospike_core::operations::lists::ListReturnType = return_type.into();
                                let op = match count {
                                    Some(c) => lists::remove_by_value_relative_rank_range_count(bin_name, core_return_type, value.clone(), *rank, *c),
                                    None => lists::remove_by_value_relative_rank_range(bin_name, core_return_type, value.clone(), *rank),
                                };
                                value_idx += 1;
                                op
                            }
                            OperationType::ListCreate(bin_name, order, pad, _persist_index) => {
                                use aerospike_core::operations::lists;
                                let core_order: aerospike_core::operations::lists::ListOrderType = order.into();
                                lists::create(bin_name, core_order, *pad)
                            }
                            OperationType::MapSize(bin_name) => {
                                use aerospike_core::operations::maps;
                                maps::size(bin_name)
                            }
                            OperationType::MapClear(bin_name) => {
                                use aerospike_core::operations::maps;
                                maps::clear(bin_name)
                            }
                            OperationType::MapPut(bin_name, _, _, policy) => {
                                use aerospike_core::operations::maps;
                                let key = &value_storage[value_idx];
                                let value = &value_storage[value_idx + 1];
                                let op = maps::put(&policy._as, bin_name, key.clone(), value.clone());
                                value_idx += 2;
                                op
                            }
                            OperationType::MapPutItems(bin_name, _, policy) => {
                                use aerospike_core::operations::maps;
                                let op = maps::put_items(&policy._as, bin_name, map_storage[map_idx].clone());
                                map_idx += 1;
                                op
                            }
                            OperationType::MapIncrementValue(bin_name, _, _value, policy) => {
                                use aerospike_core::operations::maps;
                                let key = &value_storage[value_idx];
                                let incr_value = &value_storage[value_idx + 1];
                                let op = maps::increment_value(&policy._as, bin_name, key.clone(), incr_value.clone());
                                value_idx += 2;
                                op
                            }
                            OperationType::MapDecrementValue(bin_name, _, _value, policy) => {
                                use aerospike_core::operations::maps;
                                let key = &value_storage[value_idx];
                                let decr_value = &value_storage[value_idx + 1];
                                let op = maps::decrement_value(&policy._as, bin_name, key.clone(), decr_value.clone());
                                value_idx += 2;
                                op
                            }
                            OperationType::MapGetByKey(bin_name, _, return_type) => {
                                use aerospike_core::operations::maps;
                                let key = &value_storage[value_idx];
                                let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                                let op = maps::get_by_key(bin_name, key.clone(), core_return_type);
                                value_idx += 1;
                                op
                            }
                            OperationType::MapRemoveByKey(bin_name, _, return_type) => {
                                use aerospike_core::operations::maps;
                                let key = &value_storage[value_idx];
                                let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                                let op = maps::remove_by_key(bin_name, key.clone(), core_return_type);
                                value_idx += 1;
                                op
                            }
                            OperationType::MapGetByKeyRange(bin_name, _, _, return_type) => {
                                use aerospike_core::operations::maps;
                                let begin = &value_storage[value_idx];
                                let end = &value_storage[value_idx + 1];
                                let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                                let op = maps::get_by_key_range(bin_name, begin.clone(), end.clone(), core_return_type);
                                value_idx += 2;
                                op
                            }
                            OperationType::MapRemoveByKeyRange(bin_name, _, _, return_type) => {
                                use aerospike_core::operations::maps;
                                let begin = &value_storage[value_idx];
                                let end = &value_storage[value_idx + 1];
                                let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                                let op = maps::remove_by_key_range(bin_name, begin.clone(), end.clone(), core_return_type);
                                value_idx += 2;
                                op
                            }
                            OperationType::MapGetByIndex(bin_name, index, return_type) => {
                                use aerospike_core::operations::maps;
                                let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                                maps::get_by_index(bin_name, *index, core_return_type)
                            }
                            OperationType::MapRemoveByIndex(bin_name, index, return_type) => {
                                use aerospike_core::operations::maps;
                                let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                                maps::remove_by_index(bin_name, *index, core_return_type)
                            }
                            OperationType::MapGetByIndexRange(bin_name, index, count, return_type) => {
                                use aerospike_core::operations::maps;
                                let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                                maps::get_by_index_range(bin_name, *index, *count, core_return_type)
                            }
                            OperationType::MapRemoveByIndexRange(bin_name, index, count, return_type) => {
                                use aerospike_core::operations::maps;
                                let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                                maps::remove_by_index_range(bin_name, *index, *count, core_return_type)
                            }
                            OperationType::MapGetByIndexRangeFrom(bin_name, index, return_type) => {
                                use aerospike_core::operations::maps;
                                let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                                maps::get_by_index_range_from(bin_name, *index, core_return_type)
                            }
                            OperationType::MapRemoveByIndexRangeFrom(bin_name, index, return_type) => {
                                use aerospike_core::operations::maps;
                                let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                                maps::remove_by_index_range_from(bin_name, *index, core_return_type)
                            }
                            OperationType::MapGetByRank(bin_name, rank, return_type) => {
                                use aerospike_core::operations::maps;
                                let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                                maps::get_by_rank(bin_name, *rank, core_return_type)
                            }
                            OperationType::MapRemoveByRank(bin_name, rank, return_type) => {
                                use aerospike_core::operations::maps;
                                let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                                maps::remove_by_rank(bin_name, *rank, core_return_type)
                            }
                            OperationType::MapGetByRankRange(bin_name, rank, count, return_type) => {
                                use aerospike_core::operations::maps;
                                let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                                maps::get_by_rank_range(bin_name, *rank, *count, core_return_type)
                            }
                            OperationType::MapRemoveByRankRange(bin_name, rank, count, return_type) => {
                                use aerospike_core::operations::maps;
                                let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                                maps::remove_by_rank_range(bin_name, *rank, *count, core_return_type)
                            }
                            OperationType::MapGetByRankRangeFrom(bin_name, rank, return_type) => {
                                use aerospike_core::operations::maps;
                                let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                                maps::get_by_rank_range_from(bin_name, *rank, core_return_type)
                            }
                            OperationType::MapRemoveByRankRangeFrom(bin_name, rank, return_type) => {
                                use aerospike_core::operations::maps;
                                let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                                maps::remove_by_rank_range_from(bin_name, *rank, core_return_type)
                            }
                            OperationType::MapGetByValue(bin_name, _, return_type) => {
                                use aerospike_core::operations::maps;
                                let value = &value_storage[value_idx];
                                let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                                let op = maps::get_by_value(bin_name, value.clone(), core_return_type);
                                value_idx += 1;
                                op
                            }
                            OperationType::MapRemoveByValue(bin_name, _, return_type) => {
                                use aerospike_core::operations::maps;
                                let value = &value_storage[value_idx];
                                let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                                let op = maps::remove_by_value(bin_name, value.clone(), core_return_type);
                                value_idx += 1;
                                op
                            }
                            OperationType::MapGetByValueRange(bin_name, _, _, return_type) => {
                                use aerospike_core::operations::maps;
                                let begin = &value_storage[value_idx];
                                let end = &value_storage[value_idx + 1];
                                let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                                let op = maps::get_by_value_range(bin_name, begin.clone(), end.clone(), core_return_type);
                                value_idx += 2;
                                op
                            }
                            OperationType::MapRemoveByValueRange(bin_name, _, _, return_type) => {
                                use aerospike_core::operations::maps;
                                let begin = &value_storage[value_idx];
                                let end = &value_storage[value_idx + 1];
                                let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                                let op = maps::remove_by_value_range(bin_name, begin.clone(), end.clone(), core_return_type);
                                value_idx += 2;
                                op
                            }
                            OperationType::MapGetByKeyList(bin_name, _, return_type) => {
                                use aerospike_core::operations::maps;
                                let keys = &list_storage[list_idx];
                                let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                                let op = maps::get_by_key_list(bin_name, keys.to_vec(), core_return_type);
                                list_idx += 1;
                                op
                            }
                            OperationType::MapRemoveByKeyList(bin_name, _, return_type) => {
                                use aerospike_core::operations::maps;
                                let keys = &list_storage[list_idx];
                                let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                                let op = maps::remove_by_key_list(bin_name, keys.to_vec(), core_return_type);
                                list_idx += 1;
                                op
                            }
                            OperationType::MapGetByValueList(bin_name, _, return_type) => {
                                use aerospike_core::operations::maps;
                                let values = &list_storage[list_idx];
                                let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                                let op = maps::get_by_value_list(bin_name, values.to_vec(), core_return_type);
                                list_idx += 1;
                                op
                            }
                            OperationType::MapRemoveByValueList(bin_name, _, return_type) => {
                                use aerospike_core::operations::maps;
                                let values = &list_storage[list_idx];
                                let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                                let op = maps::remove_by_value_list(bin_name, values.to_vec(), core_return_type);
                                list_idx += 1;
                                op
                            }
                            OperationType::MapSetMapPolicy(bin_name, policy) => {
                                use aerospike_core::operations::maps;
                                let core_order = policy._as.order;
                                maps::set_order(bin_name, core_order)
                            }
                            OperationType::MapGetByKeyRelativeIndexRange(bin_name, _, index, count, return_type) => {
                                use aerospike_core::operations::maps;
                                let key = &value_storage[value_idx];
                                let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                                let op = match count {
                                    Some(c) => maps::get_by_key_relative_index_range_count(bin_name, key.clone(), *index, *c, core_return_type),
                                    None => maps::get_by_key_relative_index_range(bin_name, key.clone(), *index, core_return_type),
                                };
                                value_idx += 1;
                                op
                            }
                            OperationType::MapGetByValueRelativeRankRange(bin_name, _, rank, count, return_type) => {
                                use aerospike_core::operations::maps;
                                let value = &value_storage[value_idx];
                                let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                                let op = match count {
                                    Some(c) => maps::get_by_value_relative_rank_range_count(bin_name, value.clone(), *rank, *c, core_return_type),
                                    None => maps::get_by_value_relative_rank_range(bin_name, value.clone(), *rank, core_return_type),
                                };
                                value_idx += 1;
                                op
                            }
                            OperationType::MapRemoveByKeyRelativeIndexRange(bin_name, _, index, count, return_type) => {
                                use aerospike_core::operations::maps;
                                let key = &value_storage[value_idx];
                                let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                                let op = match count {
                                    Some(c) => maps::remove_by_key_relative_index_range_count(bin_name, key.clone(), *index, *c, core_return_type),
                                    None => maps::remove_by_key_relative_index_range(bin_name, key.clone(), *index, core_return_type),
                                };
                                value_idx += 1;
                                op
                            }
                            OperationType::MapRemoveByValueRelativeRankRange(bin_name, _, rank, count, return_type) => {
                                use aerospike_core::operations::maps;
                                let value = &value_storage[value_idx];
                                let core_return_type: aerospike_core::operations::maps::MapReturnType = return_type.into();
                                let op = match count {
                                    Some(c) => maps::remove_by_value_relative_rank_range_count(bin_name, value.clone(), *rank, *c, core_return_type),
                                    None => maps::remove_by_value_relative_rank_range(bin_name, value.clone(), *rank, core_return_type),
                                };
                                value_idx += 1;
                                op
                            }
                            OperationType::MapCreate(bin_name, order) => {
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
                                let op = bitwise::insert(bin_name, *byte_offset, value.clone(), &policy._as);
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
                                let op = bitwise::set(bin_name, *bit_offset, *bit_size, value.clone(), &policy._as);
                                value_idx += 1;
                                op
                            }
                            OperationType::BitOr(bin_name, bit_offset, bit_size, _, policy) => {
                                use aerospike_core::operations::bitwise;
                                let value = &value_storage[value_idx];
                                let op = bitwise::or(bin_name, *bit_offset, *bit_size, value.clone(), &policy._as);
                                value_idx += 1;
                                op
                            }
                            OperationType::BitXor(bin_name, bit_offset, bit_size, _, policy) => {
                                use aerospike_core::operations::bitwise;
                                let value = &value_storage[value_idx];
                                let op = bitwise::xor(bin_name, *bit_offset, *bit_size, value.clone(), &policy._as);
                                value_idx += 1;
                                op
                            }
                            OperationType::BitAnd(bin_name, bit_offset, bit_size, _, policy) => {
                                use aerospike_core::operations::bitwise;
                                let value = &value_storage[value_idx];
                                let op = bitwise::and(bin_name, *bit_offset, *bit_size, value.clone(), &policy._as);
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
                            // HLL operations not yet supported in batch_operate - use operate() instead
                            OperationType::HllInit(_, _, _, _) |
                            OperationType::HllAdd(_, _, _, _, _) |
                            OperationType::HllGetCount(_) |
                            OperationType::HllDescribe(_) |
                            OperationType::HllRefreshCount(_) |
                            OperationType::HllFold(_, _) |
                            OperationType::HllGetUnion(_, _) |
                            OperationType::HllGetUnionCount(_, _) |
                            OperationType::HllGetIntersectCount(_, _) |
                            OperationType::HllGetSimilarity(_, _) |
                            OperationType::HllSetUnion(_, _, _) => {
                                return Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
                                    "HLL operations are not supported in batch_operate. Use operate() instead."
                                ));
                            }
                            OperationType::ExpRead(_, _, _) |
                            OperationType::ExpWrite(_, _, _) => {
                                return Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
                                    "Expression operations are not supported in batch_operate. Use operate() instead."
                                ));
                            }
                        };
                        core_ops.push(core_op);
                    }
                    // Use BatchOperation::read_ops() if all operations are read-only, otherwise use write()
                    let batch_op = if has_write_op {
                        BatchOperation::write(&write_policy, key, core_ops)
                    } else {
                        BatchOperation::read_ops(&read_policy, key, core_ops)
                    };
                    batch_ops.push(batch_op);
                }

                let results = client
                    .read()
                    .await
                    .batch(&batch_policy, &batch_ops)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|_py| {
                    let py_results: Vec<BatchRecord> = results
                        .into_iter()
                        .map(|br| BatchRecord { _as: br })
                        .collect();
                    Ok(py_results)
                })
            })
        }

        /// Delete multiple records for specified keys in one batch call.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.List[BatchRecord]]", imports=("typing")))]
        pub fn batch_delete<'a>(
            &self,
            batch_policy: Option<&BatchPolicy>,
            delete_policy: Option<&BatchDeletePolicy>,
            keys: Vec<Py<PyAny>>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let batch_policy = batch_policy.map(|p| p._as.clone()).unwrap_or_default();
            let delete_policy = delete_policy.map(|p| p._as.clone()).unwrap_or_default();
            let client = self._as.clone();

            // Extract Key objects from Python list
            let mut rust_keys = Vec::new();
            for key_obj in keys {
                Python::attach(|py| {
                    let key = key_obj.extract::<PyRef<Key>>(py)?;
                    rust_keys.push(key._as.clone());
                    Ok::<(), PyErr>(())
                })?;
            }
            let keys = rust_keys;

            pyo3_asyncio::future_into_py(py, async move {
                use aerospike_core::BatchOperation;

                let mut batch_ops = Vec::new();
                for key in keys {
                    let op = BatchOperation::delete(&delete_policy, key);
                    batch_ops.push(op);
                }

                let results = client
                    .read()
                    .await
                    .batch(&batch_policy, &batch_ops)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|_py| {
                    let py_results: Vec<BatchRecord> = results
                        .into_iter()
                        .map(|br| BatchRecord { _as: br })
                        .collect();
                    Ok(py_results)
                })
            })
        }

        /// Check if multiple record keys exist in one batch call.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.List[builtins.bool]]", imports=("typing", "builtins")))]
        pub fn batch_exists<'a>(
            &self,
            batch_policy: Option<&BatchPolicy>,
            read_policy: Option<&BatchReadPolicy>,
            keys: Vec<Py<PyAny>>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let batch_policy = batch_policy.map(|p| p._as.clone()).unwrap_or_default();
            let read_policy = read_policy.map(|p| p._as.clone()).unwrap_or_default();
            let client = self._as.clone();

            // Extract Key objects from Python list
            let mut rust_keys = Vec::new();
            for key_obj in keys {
                Python::attach(|py| {
                    let key = key_obj.extract::<PyRef<Key>>(py)?;
                    rust_keys.push(key._as.clone());
                    Ok::<(), PyErr>(())
                })?;
            }
            let keys = rust_keys;

            pyo3_asyncio::future_into_py(py, async move {
                use aerospike_core::BatchOperation;
                use aerospike_core::Bins;

                let mut batch_ops = Vec::new();
                for key in keys {
                    let op = BatchOperation::read(&read_policy, key, Bins::None);
                    batch_ops.push(op);
                }

                let results = client
                    .read()
                    .await
                    .batch(&batch_policy, &batch_ops)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|_py| {
                    let exists_list: Vec<bool> = results
                        .into_iter()
                        .map(|br| br.record.is_some())
                        .collect();
                    Ok(exists_list)
                })
            })
        }

        /// Read multiple record headers (metadata only, no bin data) for specified keys in one batch call.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.List[typing.Optional[Record]]]", imports=("typing")))]
        pub fn batch_get_header<'a>(
            &self,
            batch_policy: Option<&BatchPolicy>,
            read_policy: Option<&BatchReadPolicy>,
            keys: Vec<Py<PyAny>>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let batch_policy = batch_policy.map(|p| p._as.clone()).unwrap_or_default();
            let read_policy = read_policy.map(|p| p._as.clone()).unwrap_or_default();
            let client = self._as.clone();

            // Extract Key objects from Python list
            let mut rust_keys = Vec::new();
            for key_obj in keys {
                Python::attach(|py| {
                    let key = key_obj.extract::<PyRef<Key>>(py)?;
                    rust_keys.push(key._as.clone());
                    Ok::<(), PyErr>(())
                })?;
            }
            let keys = rust_keys;

            pyo3_asyncio::future_into_py(py, async move {
                use aerospike_core::BatchOperation;
                use aerospike_core::Bins;

                let mut batch_ops = Vec::new();
                for key in keys {
                    let op = BatchOperation::read(&read_policy, key, Bins::None);
                    batch_ops.push(op);
                }

                let results = client
                    .read()
                    .await
                    .batch(&batch_policy, &batch_ops)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|_py| {
                    let records: Vec<Option<Record>> = results
                        .into_iter()
                        .map(|br| br.record.map(|r| Record { _as: r }))
                        .collect();
                    Ok(records)
                })
            })
        }

        /// Apply UDF operations on multiple keys in one batch call.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.List[BatchRecord]]", imports=("typing")))]
        pub fn batch_apply<'a>(
            &self,
            batch_policy: Option<&BatchPolicy>,
            udf_policy: Option<&BatchUDFPolicy>,
            keys: Vec<Py<PyAny>>,
            udf_name: String,
            function_name: String,
            args: Option<Vec<PythonValue>>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let batch_policy = batch_policy.map(|p| p._as.clone()).unwrap_or_default();
            let udf_policy = udf_policy.map(|p| p._as.clone()).unwrap_or_default();
            let client = self._as.clone();

            // Extract Key objects from Python list
            let mut rust_keys = Vec::new();
            for key_obj in keys {
                Python::attach(|py| {
                    let key = key_obj.extract::<PyRef<Key>>(py)?;
                    rust_keys.push(key._as.clone());
                    Ok::<(), PyErr>(())
                })?;
            }
            let keys = rust_keys;

            // Convert args before moving into async block
            let rust_args = args.map(|args| {
                args.into_iter()
                    .map(|v| v.into())
                    .collect::<Vec<aerospike_core::Value>>()
            });

            pyo3_asyncio::future_into_py(py, async move {
                use aerospike_core::BatchOperation;

                let mut batch_ops = Vec::new();
                for key in keys {
                    let rust_args_owned = rust_args.as_ref().map(|a| a.to_vec());
                    let op = BatchOperation::udf(&udf_policy, key, &udf_name, &function_name, rust_args_owned);
                    batch_ops.push(op);
                }

                let results = client
                    .read()
                    .await
                    .batch(&batch_policy, &batch_ops)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|_py| {
                    let py_results: Vec<BatchRecord> = results
                        .into_iter()
                        .map(|br| BatchRecord { _as: br })
                        .collect();
                    Ok(py_results)
                })
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
                    .read()
                    .await
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
                    .read()
                    .await
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
                    .read()
                    .await
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
                    .read()
                    .await
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
                    .read()
                    .await
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
                    .read()
                    .await
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
                    .read()
                    .await
                    .create_pki_user(&admin_policy, &user, &roles)
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

        /// Return node given its name.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[Node]", imports=("typing")))]
        pub fn get_node<'a>(&self, name: String, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();

            pyo3_asyncio::future_into_py(py, async move {
                let node = client
                    .read()
                    .await
                    .get_node(&name)
                    .await
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
                    .read()
                    .await
                    .nodes()
                    .await;

                let py_nodes: Vec<Node> = nodes.into_iter().map(|n| Node { _as: n }).collect();
                Ok(py_nodes)
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
                    .read()
                    .await
                    .set_xdr_filter(
                        &admin_policy,
                        &datacenter,
                        &namespace,
                        expr.as_ref().map(|e| &e._as),
                    )
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;
                Python::attach(|py| Ok(py.None()))
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
                    let l: Result<Blob, _> = other.extract();
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
                    let l: Result<Blob, _> = other.extract();
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
                    let l: Result<Map, _> = other.extract();
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
                    let l: Result<Map, _> = other.extract();
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
                    let l: Result<List, _> = other.extract();
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
                    let l: Result<List, _> = other.extract();
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
            if let Ok(dict) = v.cast::<PyDict>() {
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
                    let l: Result<GeoJSON, _> = other.extract();
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
                    let l: Result<GeoJSON, _> = other.extract();
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
                    let l: Result<HLL, _> = other.extract();
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
                    let l: Result<HLL, _> = other.extract();
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

    impl<'a, 'py> FromPyObject<'a, 'py> for PythonValue {
        type Error = PyErr;

        fn extract(obj: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
            // Handle None first - check if the object is None
            if obj.is_none() {
                return Ok(PythonValue::Nil);
            }

            let b: PyResult<bool> = obj.extract();
            if let Ok(b) = b {
                return Ok(PythonValue::Bool(b));
            }

            // Try to extract as integer - handle both i64 and large u64 values
            // First try i64 (most common case)
            let i: PyResult<i64> = obj.extract();
            if let Ok(i) = i {
                return Ok(PythonValue::Int(i));
            }

            // For u64 values, convert to i64 (UInt has been removed from Rust core)
            // Values > i64::MAX will overflow, but this matches Rust core behavior
            let ui: PyResult<u64> = obj.extract();
            if let Ok(ui) = ui {
                // Convert u64 to i64 (may overflow for values > i64::MAX)
                return Ok(PythonValue::Int(ui as i64));
            }

            let f1: PyResult<f64> = obj.extract();
            if let Ok(f1) = f1 {
                return Ok(PythonValue::Float(ordered_float::OrderedFloat(f1)));
            }

            let s: PyResult<String> = obj.extract();
            if let Ok(s) = s {
                return Ok(PythonValue::String(s));
            }

            // Try to extract as bytearray
            if let Ok(ba) = obj.cast::<PyByteArray>() {
                return Ok(PythonValue::Blob(ba.to_vec()));
            }

            // Try to extract as bytes
            if let Ok(bytes) = obj.cast::<PyBytes>() {
                return Ok(PythonValue::Blob(bytes.as_bytes().to_vec()));
            }

            let b: Result<Blob, _> = obj.extract();
            if let Ok(b) = b {
                return Ok(PythonValue::Blob(b.v));
            }

            let l: PyResult<Vec<PythonValue>> = obj.extract();
            if let Ok(l) = l {
                return Ok(PythonValue::List(l));
            }

            let l: Result<List, _> = obj.extract();
            if let Ok(l) = l {
                return Ok(PythonValue::List(l.v));
            }

            let hm: PyResult<HashMap<PythonValue, PythonValue>> = obj.extract();
            if let Ok(hm) = hm {
                return Ok(PythonValue::HashMap(hm));
            }

            let geo: Result<GeoJSON, _> = obj.extract();
            if let Ok(geo) = geo {
                return Ok(PythonValue::GeoJSON(geo.v));
            }

            let hll: Result<HLL, _> = obj.extract();
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
                    // Convert to a list of PythonValues without flattening
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
                aerospike_core::Value::KeyValueList(kvl) => {
                    // KeyValueList is used for map operations returning key-value pairs
                    // Convert to a HashMap (dict) for Python
                    let mut map = HashMap::with_capacity(kvl.len());
                    for (k, v) in kvl {
                        map.insert(k.clone().into(), v.clone().into());
                    }
                    PythonValue::HashMap(map)
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
    m.add_class::<HllOperation>()?;
    m.add_class::<HLLWriteFlags>()?;
    m.add_class::<ExpOperation>()?;
    m.add_class::<ExpWriteFlags>()?;
    m.add_class::<ExpReadFlags>()?;

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
    m.add_class::<UDFLang>()?;
    m.add_class::<TaskStatus>()?;
    m.add_class::<RegisterTask>()?;
    m.add_class::<UdfRemoveTask>()?;
    m.add_class::<IndexTask>()?;
    m.add_class::<DropIndexTask>()?;
    m.add_class::<Version>()?;
    m.add_class::<Node>()?;
    #[cfg(feature = "tls")]
    m.add_class::<TlsConfig>()?;

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
