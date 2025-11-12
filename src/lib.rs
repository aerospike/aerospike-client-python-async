// #![deny(warnings)]
extern crate pyo3;

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

use pyo3::basic::CompareOp;
use pyo3::exceptions::{PyException, PyIndexError, PyValueError};
use pyo3::exceptions::{PyStopIteration, PyTypeError};
use pyo3::types::{PyBool, PyByteArray, PyBytes, PyDict, PyList};
use pyo3::{prelude::*, IntoPyObjectExt};
// use pyo3::conversion::IntoPy;

use pyo3_async_runtimes::tokio as pyo3_asyncio;
use pyo3_stub_gen::{
    define_stub_info_gatherer, derive::gen_stub_pyclass, derive::gen_stub_pyclass_enum, derive::gen_stub_pyfunction,
    derive::gen_stub_pymethods, PyStubType, TypeInfo,
};

use tokio::sync::RwLock;

use aerospike_core::as_geo;
use aerospike_core::as_val;
use aerospike_core::errors::Error;


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
create_exception!(aerospike_async.exceptions, ServerError, AerospikeError);
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


// Must define a wrapper type because of the orphan rule
struct RustClientError(Error);

impl From<RustClientError> for PyErr {
    fn from(value: RustClientError) -> Self {
        // RustClientError -> Error -> Custom Exception Classes
        match value.0 {
            Error::Base64(e) => Base64DecodeError::new_err(e.to_string()),
            Error::InvalidUtf8(e) => InvalidUTF8::new_err(e.to_string()),
            Error::Io(e) => IoError::new_err(e.to_string()),
            Error::MpscRecv(_) => RecvError::new_err("The sending half of a channel has been closed, so no messages can be received"),
            Error::ParseAddr(e) => ParseAddressError::new_err(e.to_string()),
            Error::ParseInt(e) => ParseIntError::new_err(e.to_string()),
            Error::PwHash(e) => PasswordHashError::new_err(e.to_string()),
            Error::BadResponse(string) => BadResponse::new_err(string),
            Error::Connection(string) => ConnectionError::new_err(string),
            Error::InvalidArgument(string) => ValueError::new_err(string),
            Error::InvalidNode(string) => InvalidNodeError::new_err(string),
            Error::NoMoreConnections => NoMoreConnections::new_err("Exceeded max. number of connections per node."),
            Error::ServerError(result_code, in_doubt, node) => {
                ServerError::new_err(format!("Code: {:?}, In Doubt: {}, Node: {}", 
                    result_code, in_doubt, node))
            },
            Error::UdfBadResponse(string) => UDFBadResponse::new_err(string),
            Error::Timeout(string, client_side) => TimeoutError::new_err(format!("{}, Client-Side: {}", string, client_side)),
            Error::Chain(first, second) => {
                // For Chain errors, look for the most specific error type
                // Check first error
                match first.as_ref() {
                    Error::ServerError(result_code, in_doubt, node) => {
                        ServerError::new_err(format!("Code: {:?}, In Doubt: {}, Node: {}", 
                            result_code, in_doubt, node))
                    },
                    Error::BadResponse(msg) => {
                        BadResponse::new_err(msg.clone())
                    },
                    Error::ClientError(msg) => {
                        // Check second error for more specific type
                        match second.as_ref() {
                            Error::ServerError(result_code, in_doubt, node) => {
                                ServerError::new_err(format!("Code: {:?}, In Doubt: {}, Node: {}", 
                                    result_code, in_doubt, node))
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
                                ServerError::new_err(format!("Code: {:?}, In Doubt: {}, Node: {}", 
                                    result_code, in_doubt, node))
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
            other => AerospikeError::new_err(format!("Unknown error: {:?}", other)),
        }
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
#[derive(Debug, Clone, Copy)]
pub enum Replica {
    Master,
    Sequence,
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
    
    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  ConsistencyLevel
    //
    ////////////////////////////////////////////////////////////////////////////////////////////
    #[gen_stub_pyclass_enum(module = "_aerospike_async_native")]
    #[pyclass(module = "_aerospike_async_native")]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum ConsistencyLevel {
        ConsistencyOne,
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
        Update,
        UpdateOnly,
        Replace,
        ReplaceOnly,
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
        #[pyo3(name = "None_")]
        None,
        ExpectGenEqual,
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
        CommitAll,
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
        Numeric,
        String,
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
        Default,
        List,
        MapKeys,
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
        Nil,
        Bool,
        Int,
        String,
        List,
        Map,
        Blob,
        Float,
        Geo,
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
        _as: aerospike_core::expressions::FilterExpression,
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
        pub fn device_size() -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::device_size(),
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

        // #[staticmethod]
        // /// Create List bin PHPValue
        // /// Not Supported in pre-alpha release
        // pub fn list_val(val: Vec<PythonValue>) -> Self {
        //     FilterExpression {
        //         _as: aerospike_core::expressions::list_val(val)
        //     }
        // }

        // #[staticmethod]
        // /// Create Map bin PHPValue
        // /// Not Supported in pre-alpha release
        // pub fn map_val(val: HashMap<PythonValue, PythonValue>) -> Self {
        //     FilterExpression {
        //         _as: aerospike_core::expressions::map_val(val)
        //     }
        // }

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



    /// Trait implemented by most policy types; policies that implement this trait typically encompass
    /// an instance of `PartitionFilter`.
    #[gen_stub_pymethods]
    #[pymethods]
    impl PartitionFilter {
        #[new]
        pub fn new() -> Self {
            PartitionFilter {
                _as: aerospike_core::query::PartitionFilter::default(),
            }
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
            self._as
                .total_timeout
                .map(|duration| duration.as_millis() as u64)
                .unwrap_or_default()
        }

        #[setter]
        pub fn set_timeout(&mut self, timeout_millis: u64) {
            let timeout = Duration::from_millis(timeout_millis);
            self._as.total_timeout = Some(timeout);
        }

        #[getter]
        pub fn get_max_retries(&self) -> Option<usize> {
            self._as.max_retries
        }

        #[setter]
        pub fn set_max_retries(&mut self, max_retries: Option<usize>) {
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
    }

    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(
        name = "ReadPolicy",
        freelist = 1000,
        module = "_aerospike_async_native",
        extends = BasePolicy
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
        subclass,
        freelist = 1000
    )]
    pub struct QueryPolicy {
        _as: aerospike_core::QueryPolicy,
    }

    /// `QueryPolicy` encapsulates parameters for query operations.
    #[gen_stub_pymethods]
    #[pymethods]
    impl QueryPolicy {
        #[new]
        pub fn __construct() -> Self {
            QueryPolicy {
                _as: aerospike_core::QueryPolicy::default(),
            }
        }

        // #[getter]
        // pub fn get_base_policy(&self) -> BasePolicy {
        //     BasePolicy {
        //         _as: self._as.base_policy.clone(),
        //     }
        // }

        // #[setter]
        // pub fn set_base_policy(&mut self, base_policy: BasePolicy) {
        //     self._as.base_policy = base_policy._as;
        // }

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
        pub fn get_fail_on_cluster_change(&self) -> bool {
            self._as.fail_on_cluster_change
        }

        #[setter]
        pub fn set_fail_on_cluster_change(&mut self, fail_on_cluster_change: bool) {
            self._as.fail_on_cluster_change = fail_on_cluster_change;
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
        subclass,
        freelist = 1000
    )]
    pub struct ScanPolicy {
        _as: aerospike_core::ScanPolicy,
    }

    /// `ScanPolicy` encapsulates optional parameters used in scan operations.
    #[gen_stub_pymethods]
    #[pymethods]
    impl ScanPolicy {
        #[new]
        pub fn __construct() -> Self {
            ScanPolicy {
                _as: aerospike_core::ScanPolicy::default(),
            }
        }

        // #[getter]
        // pub fn get_base_policy(&self) -> BasePolicy {
        //     BasePolicy {
        //         _as: self._as.base_policy.clone(),
        //     }
        // }

        // #[setter]
        // pub fn set_base_policy(&mut self, base_policy: BasePolicy) {
        //     self._as.base_policy = base_policy._as;
        // }

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
        pub fn get_socket_timeout(&self) -> u32 {
            self._as.socket_timeout
        }

        #[setter]
        pub fn set_socket_timeout(&mut self, socket_timeout: u32) {
            self._as.socket_timeout = socket_timeout;
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
            self._as.user_password.clone().map(|(user, _)| user)
        }

        #[setter]
        pub fn set_user(&mut self, user: Option<String>) {
            match (user, &self._as.user_password) {
                (Some(user), Some((_, password))) => {
                    self._as.user_password = Some((user, password.into()))
                }
                (Some(user), None) => self._as.user_password = Some((user, "".into())),
                (None, Some((_, password))) => {
                    self._as.user_password = Some(("".into(), password.into()))
                }
                (None, None) => {}
            }
        }

        #[getter]
        pub fn get_password(&self) -> Option<String> {
            self._as.user_password.clone().map(|(_, password)| password)
        }

        #[setter]
        pub fn set_password(&mut self, password: Option<String>) {
            match (password, &self._as.user_password) {
                (Some(password), Some((user, _))) => {
                    self._as.user_password = Some((user.into(), password))
                }
                (Some(password), None) => self._as.user_password = Some(("".into(), password)),
                (None, Some((user, _))) => self._as.user_password = Some((user.into(), "".into())),
                (None, None) => {}
            }
        }

        #[getter]
        pub fn get_timeout(&self) -> u64 {
            self._as
                .timeout
                .map(|duration| duration.as_millis() as u64)
                .unwrap_or_default()
        }

        #[setter]
        pub fn set_timeout(&mut self, timeout_millis: u64) {
            let timeout = Duration::from_millis(timeout_millis);
            self._as.timeout = Some(timeout);
        }

        /// Connection idle timeout. Every time a connection is used, its idle
        /// deadline will be extended by this duration. When this deadline is reached,
        /// the connection will be closed and discarded from the connection pool.
        #[getter]
        pub fn get_idle_timeout(&self) -> u64 {
            self._as
                .idle_timeout
                .map(|duration| duration.as_millis() as u64)
                .unwrap_or_default()
        }

        #[setter]
        pub fn set_idle_timeout(&mut self, timeout_millis: u64) {
            let timeout = Duration::from_millis(timeout_millis);
            self._as.idle_timeout = Some(timeout);
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
        #[getter]
        pub fn get_thread_pool_size(&self) -> usize {
            self._as.thread_pool_size
        }

        #[setter]
        pub fn set_thread_pool_size(&mut self, value: usize) {
            self._as.thread_pool_size = value;
        }

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
                None => Ok(py.None().into()),
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

        #[getter]
        pub fn get_value(&self) -> Option<PythonValue> {
            self._as.user_key.clone().map(|v| v.into())
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
        pub fn __construct(namespace: &str, set_name: &str, bins: Option<Vec<String>>) -> Self {
            Statement {
                _as: aerospike_core::Statement::new(namespace, set_name, bins_flag(bins)),
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
        // This matches Java's geoWithinRadius(name, lng, lat, radius) signature
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
            
            // Manually construct AeroCircle GeoJSON string to match Java client format
            // Java: String.format("{ \"type\": \"AeroCircle\", \"coordinates\": [[%.8f, %.8f], %f] }", lng, lat, radius)
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
    #[derive(Clone)]
    pub struct Recordset {
        _as: Arc<aerospike_core::Recordset>,
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

        fn __iter__(&self) -> Self {
            self.clone()
        }

        fn __next__<'a>(&mut self, py: Python<'a>) -> PyResult<Option<Py<PyAny>>> {
            let rcs = self._as.clone();
            match rcs.next_record() {
                None => Err(PyStopIteration::new_err("Recordset iteration complete")),
                Some(Err(e)) => Err(PyErr::from(RustClientError(e))),
                Some(Ok(rec)) => {
                    let res = Record { _as: rec };
                    Ok(Some(res.into_pyobject(py).unwrap().unbind().into()))
                }
            }
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
                    .put(&policy, &key, &bins)
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
                    Some(client
                        .read()
                        .await
                        .get(&policy, &key, aerospike_core::Bins::None)
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
        pub fn truncate<'a>(
            &self,
            namespace: String,
            set_name: String,
            before_nanos: Option<i64>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();

            let before_nanos = before_nanos.unwrap_or_default();

            pyo3_asyncio::future_into_py(py, async move {
                client
                    .read()
                    .await
                    .truncate(&namespace, &set_name, before_nanos)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|py| Ok(py.None()))
            })
        }

        /// Create a secondary index on a bin containing scalar values. This asynchronous server call
        /// returns before the command is complete.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        pub fn create_index<'a>(
            &self,
            namespace: String,
            set_name: String,
            bin_name: String,
            index_name: String,
            index_type: IndexType,
            cit: Option<CollectionIndexType>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();

            let cit = (&cit.unwrap_or(CollectionIndexType::Default)).into();
            let index_type = (&index_type).into();

            pyo3_asyncio::future_into_py(py, async move {
                client
                    .read()
                    .await
                    .create_complex_index(
                        &namespace,
                        &set_name,
                        &bin_name,
                        &index_name,
                        index_type,
                        cit,
                    )
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|py| Ok(py.None()))
            })
        }

        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        pub fn drop_index<'a>(
            &self,
            namespace: String,
            set_name: String,
            index_name: String,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            let client = self._as.clone();

            pyo3_asyncio::future_into_py(py, async move {
                client
                    .read()
                    .await
                    .drop_index(&namespace, &set_name, &index_name)
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

                Ok(Recordset { _as: res })
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
            let stmt = statement._as.clone();

            pyo3_asyncio::future_into_py(py, async move {
                let res = client
                    .read()
                    .await
                    .query(&policy, partition_filter._as, stmt)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Ok(Recordset { _as: res })
            })
        }

        /// Creates a new user with password and roles. Clear-text password will be hashed using bcrypt
        /// before sending to server.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        pub fn create_user<'a>(
            &self,
            user: String,
            password: String,
            roles: Vec<String>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            // let policy = policy._as.clone();
            let client = self._as.clone();

            pyo3_asyncio::future_into_py(py, async move {
                let roles: Vec<&str> = roles.iter().map(|r| &**r).collect();
                client
                    .read()
                    .await
                    .create_user(&user, &password, &roles)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|py| Ok(py.None()))
            })
        }

        /// Removes a user from the cluster.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        pub fn drop_user<'a>(&self, user: String, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
            // let policy = policy._as.clone();
            let client = self._as.clone();

            pyo3_asyncio::future_into_py(py, async move {
                client
                    .read()
                    .await
                    .drop_user(&user)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|py| Ok(py.None()))
            })
        }

        /// Changes a user's password. Clear-text password will be hashed using bcrypt before sending to server.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        pub fn change_password<'a>(
            &self,
            user: String,
            password: String,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            // let policy = policy._as.clone();
            let client = self._as.clone();

            pyo3_asyncio::future_into_py(py, async move {
                client
                    .read()
                    .await
                    .change_password(&user, &password)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|py| Ok(py.None()))
            })
        }

        /// Adds roles to user's list of roles.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        pub fn grant_roles<'a>(
            &self,
            user: String,
            roles: Vec<String>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            // let policy = policy._as.clone();
            let client = self._as.clone();

            pyo3_asyncio::future_into_py(py, async move {
                let roles: Vec<&str> = roles.iter().map(|r| &**r).collect();
                client
                    .read()
                    .await
                    .grant_roles(&user, &roles)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|py| Ok(py.None()))
            })
        }

        /// Removes roles from user's list of roles.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        pub fn revoke_roles<'a>(
            &self,
            user: String,
            roles: Vec<String>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            // let policy = policy._as.clone();
            let client = self._as.clone();

            pyo3_asyncio::future_into_py(py, async move {
                let roles: Vec<&str> = roles.iter().map(|r| &**r).collect();
                client
                    .read()
                    .await
                    .revoke_roles(&user, &roles)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|py| Ok(py.None()))
            })
        }

        /// Retrieves users and their roles.
        /// If None is passed for the user argument, all users will be returned.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        pub fn query_users<'a>(
            &self,
            user: Option<String>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            // let policy = policy._as.clone();
            let client = self._as.clone();

            pyo3_asyncio::future_into_py(py, async move {
                let user = user.as_ref().map(|u| u.as_str());
                let res = client
                    .read()
                    .await
                    .query_users(user)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                let res: Vec<User> = res.iter().map(|u| User { _as: u.clone() }).collect();
                Ok(res)
            })
        }

        /// Retrieves roles and their privileges.
        /// If None is passed for the role argument, all roles will be returned.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        pub fn query_roles<'a>(
            &self,
            role: Option<String>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            // let policy = policy._as.clone();
            let client = self._as.clone();

            pyo3_asyncio::future_into_py(py, async move {
                let role: Option<&str> = role.as_ref().map(|u| u.as_str());
                let res = client
                    .read()
                    .await
                    .query_roles(role)
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
        pub fn create_role<'a>(
            &self,
            role_name: String,
            privileges: Vec<Privilege>,
            allowlist: Vec<String>,
            read_quota: u32,
            write_quota: u32,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            // let policy = policy._as.clone();
            let client = self._as.clone();

            pyo3_asyncio::future_into_py(py, async move {
                let allowlist: Vec<&str> = allowlist.iter().map(|al| &**al).collect();
                let privileges: Vec<aerospike_core::Privilege> =
                    privileges.iter().map(|r| r._as.clone()).collect();
                client
                    .read()
                    .await
                    .create_role(&role_name, &privileges, &allowlist, read_quota, write_quota)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|py| Ok(py.None()))
            })
        }

        /// Removes a user-defined role.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        pub fn drop_role<'a>(
            &self,
            role_name: String,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            // let policy = policy._as.clone();
            let client = self._as.clone();

            pyo3_asyncio::future_into_py(py, async move {
                let role_name = role_name.clone();
                client
                    .read()
                    .await
                    .drop_role(&role_name)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|py| Ok(py.None()))
            })
        }

        /// Grants privileges to a user-defined role.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        pub fn grant_privileges<'a>(
            &self,
            role_name: String,
            privileges: Vec<Privilege>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            // let policy = policy._as.clone();
            let client = self._as.clone();

            pyo3_asyncio::future_into_py(py, async move {
                let privileges: Vec<aerospike_core::Privilege> =
                    privileges.iter().map(|p| p._as.clone()).collect();
                client
                    .read()
                    .await
                    .grant_privileges(&role_name, &privileges)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|py| Ok(py.None()))
            })
        }

        /// Revokes privileges from a user-defined role.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        pub fn revoke_privileges<'a>(
            &self,
            role_name: String,
            privileges: Vec<Privilege>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            // let policy = policy._as.clone();
            let client = self._as.clone();

            pyo3_asyncio::future_into_py(py, async move {
                let privileges: Vec<aerospike_core::Privilege> =
                    privileges.iter().map(|p| p._as.clone()).collect();
                client
                    .read()
                    .await
                    .revoke_privileges(&role_name, &privileges)
                    .await
                    .map_err(|e| PyErr::from(RustClientError(e)))?;

                Python::attach(|py| Ok(py.None()))
            })
        }

        /// Sets IP address allowlist for a role.
        /// If allowlist is nil or empty, it removes existing allowlist from role.
        #[gen_stub(override_return_type(type_repr="typing.Awaitable[typing.Any]", imports=("typing")))]
        pub fn set_allowlist<'a>(
            &self,
            role_name: String,
            allowlist: Vec<String>,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            // let policy = policy._as.clone();
            let client = self._as.clone();

            pyo3_asyncio::future_into_py(py, async move {
                let allowlist: Vec<&str> = allowlist.iter().map(|al| &**al).collect();
                client
                    .read()
                    .await
                    .set_allowlist(&role_name, &allowlist)
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
        pub fn set_quotas<'a>(
            &self,
            role_name: String,
            read_quota: u32,
            write_quota: u32,
            py: Python<'a>,
        ) -> PyResult<Bound<'a, PyAny>> {
            // let policy = policy._as.clone();
            let client = self._as.clone();

            pyo3_asyncio::future_into_py(py, async move {
                client
                    .read()
                    .await
                    .set_quotas(&role_name, read_quota, write_quota)
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
                aerospike_core::Value::HashMap(h) => {
                    let mut arr = HashMap::with_capacity(h.len());
                    h.iter().for_each(|(k, v)| {
                        arr.insert(k.clone().into(), v.clone().into());
                    });
                    PythonValue::HashMap(arr)
                }
                aerospike_core::Value::GeoJSON(gj) => PythonValue::GeoJSON(gj),
                aerospike_core::Value::HLL(b) => PythonValue::HLL(b),
                _ => unreachable!(),
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
        return GeoJSON::new(py, &geo_dict.into_bound_py_any(py)?.as_any());
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
    GeoJSON::new(py, &point_dict.as_any())
}

#[pymodule]
fn _aerospike_async_native(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Add all main classes to the top level for easy importing
    m.add_class::<Client>()?;
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

    m.add_class::<BasePolicy>()?;
    m.add_class::<ReadPolicy>()?;

    // Add helper functions
    m.add_function(wrap_pyfunction!(null, m)?)?;
    m.add_function(wrap_pyfunction!(geojson, m)?)?;
    m.add_class::<ClientPolicy>()?;
    m.add_class::<WritePolicy>()?;
    m.add_class::<ScanPolicy>()?;
    m.add_class::<QueryPolicy>()?;
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
    m.add_submodule(&exceptions_module)?;
    
    Ok(())
}
