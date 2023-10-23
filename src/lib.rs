#![deny(warnings)]
extern crate pyo3;

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;
use std::fmt;
use std::collections::hash_map::DefaultHasher;

use pyo3::exceptions::{PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyByteArray, PyBytes};
use pyo3::exceptions::{PyException,PyIndexError};
use pyo3::iter::IterNextOutput;
use pyo3::basic::CompareOp;

use tokio::sync::RwLock;

use aerospike_core::as_val;

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

#[pymodule]
fn aerospike_async(_py: Python, m: &PyModule) -> PyResult<()> {


    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  ConsistencyLevel
    //
    ////////////////////////////////////////////////////////////////////////////////////////////
    #[pyclass]
    #[derive(Debug, Clone, Copy)]
    pub enum ConsistencyLevel {
        ConsistencyOne,
        ConsistencyAll,
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  Priority
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Priority of operations on database server.
    #[pyclass]
    #[derive(Debug, Clone, Copy)]
    pub enum Priority {
        Default,
        Low,
        Medium,
        High,
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  RecordExistsAction
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// `RecordExistsAction` determines how to handle record writes based on record generation.
    #[pyclass]
    #[derive(Debug, PartialEq, Clone)]
    pub enum RecordExistsAction {
        Update,
        UpdateOnly,
        Replace,
        ReplaceOnly,
        CreateOnly,
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  GenerationPolicy
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[pyclass]
    #[derive(Debug, PartialEq, Clone)]
    pub enum GenerationPolicy {
        ExpectGenEqual,
        ExpectGenGreater,
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  CommitLevel
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[pyclass]
    #[derive(Debug, Clone)]
    pub enum CommitLevel {
        CommitAll,
        CommitMaster,
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  Expiration
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[pyclass(subclass, freelist = 1000, module = "aerospike")]
    #[derive(Debug, Clone)]
    pub struct Expiration {
        v: _Expiration,
    }

    #[pymethods]
    impl Expiration {
        #[classattr]
        const NAMESPACE_DEFAULT: Expiration = Expiration {v: _Expiration::NamespaceDefault};

        #[classattr]
        const NEVER_EXPIRE: Expiration = Expiration {v: _Expiration::Never};

        #[classattr]
        const DONT_UPDATE: Expiration = Expiration {v: _Expiration::DontUpdate};

        #[staticmethod]
        pub fn seconds(s: u32) -> Expiration {
            Expiration {v: _Expiration::Seconds(s)}
        }

        fn __richcmp__(&self, other: Expiration, op: CompareOp) -> bool {
            match op {
                CompareOp::Eq => {
                    self.v == other.v
                },
                CompareOp::Ne => {
                    self.v != other.v
                },
                _ => false,
            }
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

    #[derive(Debug, Clone, Copy, PartialEq)]
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
    #[pyclass]
    #[derive(Debug, Clone)]
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
    #[pyclass]
    #[derive(Debug, Clone)]
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
                CollectionIndexType::MapValues => aerospike_core::query::CollectionIndexType::MapValues,
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  ExpressionType (ExpType)
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Expression Data Types for usage in some `FilterExpressions`
    #[pyclass]
    #[derive(Debug, Clone, Copy)]
    #[allow(clippy::upper_case_acronyms)]
    pub enum ExpType {
        NIL,
        BOOL,
        INT,
        STRING,
        LIST,
        MAP,
        BLOB,
        FLOAT,
        GEO,
        HLL,
    }

    impl From<&ExpType> for aerospike_core::expressions::ExpType {
        fn from(input: &ExpType) -> Self {
            match &input {
                ExpType::NIL => aerospike_core::expressions::ExpType::NIL,
                ExpType::BOOL => aerospike_core::expressions::ExpType::BOOL,
                ExpType::INT => aerospike_core::expressions::ExpType::INT,
                ExpType::STRING => aerospike_core::expressions::ExpType::STRING,
                ExpType::LIST => aerospike_core::expressions::ExpType::LIST,
                ExpType::MAP => aerospike_core::expressions::ExpType::MAP,
                ExpType::BLOB => aerospike_core::expressions::ExpType::BLOB,
                ExpType::FLOAT => aerospike_core::expressions::ExpType::FLOAT,
                ExpType::GEO => aerospike_core::expressions::ExpType::GEO,
                ExpType::HLL => aerospike_core::expressions::ExpType::HLL,
            }
        }
    }


    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  RegexFlag
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Regex Bit Flags
    /// Used to change the Regex Mode in Filters
    #[pyclass]
    #[derive(Clone)]
    #[allow(clippy::upper_case_acronyms)]
    pub enum RegexFlag {
        /// Use regex defaults.
        // NONE = 0,
        /// Use POSIX Extended Regular Expression syntax when interpreting regex.
        EXTENDED = 1,
        /// Do not differentiate case.
        ICASE = 2,
        /// Do not report position of matches.
        NOSUB = 3,
        /// Match-any-character operators don't match a newline.
        NEWLINE = 8,
    }


    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  Filter Expression
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    /// Filter expression, which can be applied to most commands, to control which records are
    /// affected by the command.
    #[pyclass(subclass, freelist = 1000, module = "aerospike")]
    #[derive(Clone)]
    pub struct FilterExpression {
        _as: aerospike_core::expressions::FilterExpression,
    }

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
                _as: aerospike_core::expressions::bin_exists(name),
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
        pub fn regex_compare(regex: String, flags: Vec<RegexFlag>, bin: FilterExpression) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::regex_compare(regex, flags.into_iter().map(|a| a as i64).reduce(|a, b| a | b).unwrap_or(0), bin._as),
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
        /// Create List bin value
        pub fn list_val(val: Vec<PythonValue>) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::list_val(val.into_iter().map(|v| v.into()).collect())
            }
        }

        #[staticmethod]
        /// Create Map bin value
        pub fn map_val(val: HashMap<PythonValue, PythonValue>) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::map_val(val.into_iter().map(|(k, v)| (k.into(), v.into())).collect())
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
        /// Create a Nil value
        pub fn nil() -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::nil(),
            }
        }

        #[staticmethod]
        /// Create "not" operator expression.
        pub fn not(exp: FilterExpression) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::not(exp._as),
            }
        }

        #[staticmethod]
        /// Create "and" (&&) operator that applies to a variable number of expressions.
        /// // (a > 5 || a == 0) && b < 3
        pub fn and(exps: Vec<FilterExpression>) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::and(exps.into_iter().map(|exp| exp._as).collect()),
            }
        }

        #[staticmethod]
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
                _as: aerospike_core::expressions::xor(exps.into_iter().map(|exp| exp._as).collect()),
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
                _as: aerospike_core::expressions::int_or(exps.into_iter().map(|exp| exp._as).collect()),
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
                _as: aerospike_core::expressions::min(exps.into_iter().map(|exp| exp._as).collect()),
            }
        }

        #[staticmethod]
        /// Create expression that returns the maximum value in a variable number of expressions.
        /// All arguments must be the same type (integer or float).
        /// Requires server version 5.6.0+.
        pub fn max(exps: Vec<FilterExpression>) -> Self {
            FilterExpression {
                _as: aerospike_core::expressions::max(exps.into_iter().map(|exp| exp._as).collect()),
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
                _as: aerospike_core::expressions::cond(exps.into_iter().map(|exp| exp._as).collect()),
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
    //  ReadPolicy
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[pyclass(subclass, freelist = 1000, module = "aerospike")]
    #[derive(Debug, Clone)]
    pub struct ReadPolicy {
        _as: aerospike_core::policy::ReadPolicy,
    }

    /// Trait implemented by most policy types; policies that implement this trait typically encompass
    /// an instance of `ReadPolicy`.
    #[pymethods]
    impl ReadPolicy {
        #[new]
        pub fn __construct() -> Self {
            ReadPolicy {
                _as: aerospike_core::ReadPolicy::default(),
            }
        }

        #[getter]
        pub fn get_priority(&self) -> Priority {
            match &self._as.priority {
                aerospike_core::Priority::Default => Priority::Default,
                aerospike_core::Priority::Low => Priority::Low,
                aerospike_core::Priority::Medium => Priority::Medium,
                aerospike_core::Priority::High => Priority::High,
            }
        }

        #[setter]
        pub fn set_priority(&mut self, priority: Priority) {
            self._as.priority = match priority {
                Priority::Default => aerospike_core::Priority::Default,
                Priority::Low => aerospike_core::Priority::Low,
                Priority::Medium => aerospike_core::Priority::Medium,
                Priority::High => aerospike_core::Priority::High,
            }
        }

        #[getter]
        pub fn get_consistency_level(&self) -> ConsistencyLevel {
            match &self._as.consistency_level {
                aerospike_core::ConsistencyLevel::ConsistencyOne => ConsistencyLevel::ConsistencyOne,
                aerospike_core::ConsistencyLevel::ConsistencyAll => ConsistencyLevel::ConsistencyAll,
            }
        }

        #[setter]
        pub fn set_consistency_level(&mut self, consistency_level: ConsistencyLevel) {
            self._as.consistency_level = match consistency_level {
                ConsistencyLevel::ConsistencyOne => aerospike_core::ConsistencyLevel::ConsistencyOne,
                ConsistencyLevel::ConsistencyAll => aerospike_core::ConsistencyLevel::ConsistencyAll,
            };
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

    #[pyclass(subclass, freelist = 1000, module = "aerospike")]
    #[derive(Debug, Clone)]
    pub struct WritePolicy {
        _as: aerospike_core::WritePolicy,
    }

    /// `WritePolicy` encapsulates parameters for all write operations.
    #[pymethods]
    impl WritePolicy {
        #[new]
        pub fn __construct() -> Self {
            WritePolicy {
                _as: aerospike_core::WritePolicy::default(),
            }
        }

        #[getter]
        pub fn get_record_exists_action(&self) -> RecordExistsAction {
            match &self._as.record_exists_action {
                aerospike_core::RecordExistsAction::Update => RecordExistsAction::Update,
                aerospike_core::RecordExistsAction::UpdateOnly => RecordExistsAction::UpdateOnly,
                aerospike_core::RecordExistsAction::Replace => RecordExistsAction::Replace,
                aerospike_core::RecordExistsAction::ReplaceOnly => RecordExistsAction::ReplaceOnly,
                aerospike_core::RecordExistsAction::CreateOnly => RecordExistsAction::CreateOnly,
            }
        }

        #[setter]
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
        pub fn get_generation_policy(&self) -> Option<GenerationPolicy> {
            match &self._as.generation_policy {
                aerospike_core::GenerationPolicy::None => None,
                aerospike_core::GenerationPolicy::ExpectGenEqual => Some(GenerationPolicy::ExpectGenEqual),
                aerospike_core::GenerationPolicy::ExpectGenGreater => Some(GenerationPolicy::ExpectGenGreater),
            }
        }

        #[setter]
        pub fn set_generation_policy(&mut self, generation_policy: Option<GenerationPolicy>) {
            self._as.generation_policy = match generation_policy {
                None => aerospike_core::GenerationPolicy::None,
                Some(GenerationPolicy::ExpectGenEqual) => aerospike_core::GenerationPolicy::ExpectGenEqual,
                Some(GenerationPolicy::ExpectGenGreater) => aerospike_core::GenerationPolicy::ExpectGenGreater,
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
                    aerospike_core::Expiration::Seconds(s) => Expiration {v: _Expiration::Seconds(*s) },
                    aerospike_core::Expiration::NamespaceDefault => Expiration {v: _Expiration::NamespaceDefault },
                    aerospike_core::Expiration::Never => Expiration {v: _Expiration::Never },
                    aerospike_core::Expiration::DontUpdate => Expiration {v: _Expiration::DontUpdate },
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
            self._as.respond_per_each_op
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

    #[pyclass(subclass, freelist = 1000, module = "aerospike")]
    pub struct QueryPolicy {
        _as: aerospike_core::QueryPolicy,
    }

    /// `QueryPolicy` encapsulates parameters for query operations.
    #[pymethods]
    impl QueryPolicy {
        #[new]
        pub fn __construct() -> Self {
            QueryPolicy {
                _as: aerospike_core::QueryPolicy::default(),
            }
        }

        // #[getter]
        // pub fn get_base_policy(&self) -> ReadPolicy {
        //     ReadPolicy {
        //         _as: self._as.base_policy.clone(),
        //     }
        // }

        // #[setter]
        // pub fn set_base_policy(&mut self, base_policy: ReadPolicy) {
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


////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  ScanPolicy
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[pyclass(subclass, freelist = 1000, module = "aerospike")]
    pub struct ScanPolicy {
        _as: aerospike_core::ScanPolicy,
    }

    /// `ScanPolicy` encapsulates optional parameters used in scan operations.
    #[pymethods]
    impl ScanPolicy {
        #[new]
        pub fn __construct() -> Self {
            ScanPolicy {
                _as: aerospike_core::ScanPolicy::default(),
            }
        }

        // #[getter]
        // pub fn get_base_policy(&self) -> ReadPolicy {
        //     ReadPolicy {
        //         _as: self._as.base_policy.clone(),
        //     }
        // }

        // #[setter]
        // pub fn set_base_policy(&mut self, base_policy: ReadPolicy) {
        //     self._as.base_policy = base_policy._as;
        // }

        #[getter]
        pub fn get_scan_percent(&self) -> u8 {
            self._as.scan_percent
        }

        #[setter]
        pub fn set_scan_percent(&mut self, scan_percent: u8) {
            self._as.scan_percent = scan_percent;
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
        pub fn get_fail_on_cluster_change(&self) -> bool {
            self._as.fail_on_cluster_change
        }

        #[setter]
        pub fn set_fail_on_cluster_change(&mut self, fail_on_cluster_change: bool) {
            self._as.fail_on_cluster_change = fail_on_cluster_change;
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


    #[pyclass(subclass, freelist = 1000, module = "aerospike_async")]
    #[derive(Clone)]
    pub struct ClientPolicy {
        _as: aerospike_core::ClientPolicy,
    }

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

        fn __str__(&self) -> PyResult<String> {
            Ok("".into())
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

        pub fn __deepcopy__(&self, _memo: &PyDict) -> Self {
            // fast bitwise copy instead of python's pickling process
            self.clone()
        }
    }

    /**********************************************************************************
     *
     * Record
     *
     **********************************************************************************/

    #[pyclass(subclass, freelist = 1, module = "aerospike")]
    #[derive(Clone)]
    struct Record {
        _as: aerospike_core::Record,
    }

    #[pymethods]
    impl Record {
        pub fn bin(&self, name: &str) -> Option<PyObject> {
            let b = self._as.bins.get(name);
            b.map(|v| {
                let v: PythonValue = v.to_owned().into();
                Python::with_gil(|py| v.into_py(py) )
            })
        }

        #[getter]
        pub fn get_bins(&self) -> PyObject {
            let b = self._as.bins.clone();
            let v: PythonValue = b.into();
            Python::with_gil(|py| v.into_py(py) )
        }

        #[getter]
        pub fn get_generation(&self) -> Option<u32> {
            Some(self._as.generation)
        }

        #[getter]
        pub fn get_ttl(&self) -> Option<u32> {
            self._as.time_to_live().map(|v| v.as_secs() as u32).or(Some(0))
        }

        #[getter]
        pub fn get_key(&self) -> Option<Key> {
            self._as.key.as_ref().map(|k| Key { _as : k.clone() } )
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

    #[pyclass(subclass, freelist = 1, module = "aerospike")]
    #[derive(Clone)]
    struct Key {
        _as: aerospike_core::Key,
    }

    #[pymethods]
    impl Key {
        #[new]
        fn new(namespace: &str, set: &str, key: PythonValue) -> Self {
            let _as = aerospike_core::Key::new(namespace, set, key.into()).unwrap();
            Key { _as }
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
                CompareOp::Eq => {
                    self._as.digest == other._as.digest
                },
                CompareOp::Ne => {
                    self._as.digest != other._as.digest
                },
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

        pub fn __deepcopy__(&self, _memo: &PyDict) -> Self {
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
    #[pyclass(subclass, freelist = 1000, module = "aerospike")]
    #[derive(Clone)]
    pub struct Statement {
        _as: aerospike_core::Statement,
    }

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
    #[pyclass(subclass, freelist = 1000, module = "aerospike")]
    #[derive(Clone)]
    pub struct Filter {
        _as: aerospike_core::query::Filter,
    }

    #[pymethods]
    impl Filter {
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
        pub fn contains(bin_name: &str, value: PythonValue, cit: Option<&CollectionIndexType>) -> Self {
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
        pub fn within_region(bin_name: &str, region: &str, cit: Option<&CollectionIndexType>) -> Self {
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
        // $lat = 43.0004;
        // $lng = -89.0005;
        // $radius = 1000;
        // $filter = Filter::regionsContainingPoint("binName", $lat, $lng, $radius);
        pub fn within_radius(
            bin_name: &str,
            lat: f64,
            lng: f64,
            radius: f64,
            cit: Option<&CollectionIndexType>,
        ) -> Self {
            let default = CollectionIndexType::Default;
            let cit = cit.unwrap_or(&default);
            Filter {
                _as: aerospike_core::as_within_radius!(
                    bin_name,
                    lat,
                    lng,
                    radius,
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
    #[pyclass(subclass, freelist = 1000, module = "aerospike")]
    #[derive(Clone)]
    pub struct Recordset {
        _as: Arc<aerospike_core::Recordset>,
    }

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

        fn __next__(&mut self, py: Python<'_>) -> IterNextOutput<PyObject, String> {
            let rcs = self._as.clone();
            match rcs.next_record() {
                None => IterNextOutput::Return("ended".into()),
                Some(Err(e)) => panic!("{}", e),
                Some(Ok(rec)) => {
                    let res = Record{_as: rec};
                    IterNextOutput::Yield(res.into_py(py))
                },
            }
        }
    }

    /**********************************************************************************
     *
     * Client
     *
     **********************************************************************************/

    #[pyfunction]
    pub fn new_client(py: Python, hosts: String, policy: Option<ClientPolicy>) -> PyResult<PyObject> {
        let as_policy = policy.map(|p| p._as.clone()).unwrap_or(aerospike_core::ClientPolicy::default());
        let as_hosts = hosts.clone();

        Ok(pyo3_asyncio::tokio::future_into_py(py, async move {
            let c = aerospike_core::Client::new(&as_policy, &as_hosts)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?;

            let res = Client {
                _as: Arc::new(RwLock::new(c)),
                seeds: hosts.clone(),
            };

            // Python::with_gil(|_py| Ok(res))
            Ok(res)
        })?.into())
    }

    #[pyclass(subclass, freelist = 1, module = "aerospike")]
    #[derive(Clone)]
    struct Client {
        _as: Arc<RwLock<aerospike_core::Client>>,
        seeds: String,
    }

    #[pymethods]
    impl Client {
        pub fn seeds(&self) -> &str {
            &self.seeds
        }

        pub fn close<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
            let client = self._as.clone();

            pyo3_asyncio::tokio::future_into_py(py, async move {
                client
                    .read()
                    .await
                    .close()
                    .await
                    .map_err(|e| PyException::new_err(e.to_string()))?;

                Python::with_gil(|py| Ok(py.None()))
            })
        }

        /// Write record bin(s). The policy specifies the transaction timeout, record expiration and
        /// how the transaction is handled when the record already exists.
        pub fn put<'a>(&self, key: &Key, bins: HashMap<String, PythonValue>, policy: Option<&WritePolicy>, py: Python<'a>) -> PyResult<&'a PyAny>{
            let policy = policy.map(|policy| policy._as.clone()).unwrap_or(aerospike_core::policy::WritePolicy::default());
            let key = key._as.clone();
            let client = self._as.clone();

            let bins: Vec<aerospike_core::Bin> =
                bins.into_iter().map(|(name, val)| aerospike_core::Bin::new(name, val.into())).collect();

            pyo3_asyncio::tokio::future_into_py(py, async move {
                client
                    .read()
                    .await
                    .put(&policy, &key, &bins)
                    .await
                    .map_err(|e| PyException::new_err(e.to_string()))?;

                Python::with_gil(|py| Ok(py.None()))
            })
        }

        /// Read record for the specified key. Depending on the bins value provided, all record bins,
        /// only selected record bins or only the record headers will be returned. The policy can be
        /// used to specify timeouts.
        pub fn get<'a>(
            &self,
            key: &Key,
            bins: Option<Vec<String>>,
            policy: Option<&ReadPolicy>,
            py: Python<'a>,
        ) -> PyResult<&'a PyAny> {
            let policy = policy.map(|policy| policy._as.clone()).unwrap_or(aerospike_core::policy::ReadPolicy::default());
            let key = key._as.clone();
            let client = self._as.clone();

            pyo3_asyncio::tokio::future_into_py(py, async move {
                let res = client
                    .read()
                    .await
                    .get(&policy, &key, bins_flag(bins))
                    .await
                    .map_err(|e| PyException::new_err(e.to_string()))?;

                Ok(Record{_as:res})
            })
        }

            /// Add integer bin values to existing record bin values. The policy specifies the transaction
    /// timeout, record expiration and how the transaction is handled when the record already
    /// exists. This call only works for integer values.
    pub fn add<'a>(&self, key: &Key, bins: HashMap<String, PythonValue>, policy: Option<&WritePolicy>, py: Python<'a>) -> PyResult<&'a PyAny> {
        let policy = policy.map(|policy| policy._as.clone()).unwrap_or(aerospike_core::policy::WritePolicy::default());
        let key = key._as.clone();
        let client = self._as.clone();

        let bins: Vec<aerospike_core::Bin> =
            bins.into_iter().map(|(name, val)| aerospike_core::Bin::new(name, val.into())).collect();

        pyo3_asyncio::tokio::future_into_py(py, async move {
            client
                .read()
                .await
                .add(&policy, &key, &bins)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?;

            Python::with_gil(|py| Ok(py.None()))
        })
    }

    /// Append bin string values to existing record bin values. The policy specifies the
    /// transaction timeout, record expiration and how the transaction is handled when the record
    /// already exists. This call only works for string values.
    pub fn append<'a>(&self, key: &Key, bins: HashMap<String, PythonValue>, policy: Option<&WritePolicy>, py: Python<'a>) -> PyResult<&'a PyAny> {
        let policy = policy.map(|policy| policy._as.clone()).unwrap_or(aerospike_core::policy::WritePolicy::default());
        let key = key._as.clone();
        let client = self._as.clone();

        let bins: Vec<aerospike_core::Bin> =
            bins.into_iter().map(|(name, val)| aerospike_core::Bin::new(name, val.into())).collect();

        pyo3_asyncio::tokio::future_into_py(py, async move {
            client
                .read()
                .await
                .append(&policy, &key, &bins)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?;

                Python::with_gil(|py| Ok(py.None()))
        })
    }

    /// Prepend bin string values to existing record bin values. The policy specifies the
    /// transaction timeout, record expiration and how the transaction is handled when the record
    /// already exists. This call only works for string values.
    pub fn prepend<'a>(&self, key: &Key, bins: HashMap<String, PythonValue>, policy: Option<&WritePolicy>, py: Python<'a>) -> PyResult<&'a PyAny> {
        let policy = policy.map(|policy| policy._as.clone()).unwrap_or(aerospike_core::policy::WritePolicy::default());
        let key = key._as.clone();
        let client = self._as.clone();

        let bins: Vec<aerospike_core::Bin> =
            bins.into_iter().map(|(name, val)| aerospike_core::Bin::new(name, val.into())).collect();

        pyo3_asyncio::tokio::future_into_py(py, async move {
            client
                .read()
                .await
                .prepend(&policy, &key, &bins)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?;

                Python::with_gil(|py| Ok(py.None()))
        })
    }

    /// Delete record for specified key. The policy specifies the transaction timeout.
    /// The call returns `true` if the record existed on the server before deletion.
    pub fn delete<'a>(&self, key: &Key, policy: Option<&WritePolicy>, py: Python<'a>) -> PyResult<&'a PyAny> {
        let policy = policy.map(|policy| policy._as.clone()).unwrap_or(aerospike_core::policy::WritePolicy::default());
        let key = key._as.clone();
        let client = self._as.clone();

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let res = client
                .read()
                .await
                .delete(&policy, &key)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?;

                Ok(res)
        })
    }

    /// Reset record's time to expiration using the policy's expiration. Fail if the record does
    /// not exist.
    pub fn touch<'a>(&self, key: &Key, policy: Option<&WritePolicy>, py: Python<'a>) -> PyResult<&'a PyAny> {
        let policy = policy.map(|policy| policy._as.clone()).unwrap_or(aerospike_core::policy::WritePolicy::default());
        let key = key._as.clone();
        let client = self._as.clone();

        pyo3_asyncio::tokio::future_into_py(py, async move {
            client
                .read()
                .await
                .touch(&policy, &key)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?;

                Python::with_gil(|py| Ok(py.None()))
        })
    }

    /// Determine if a record key exists. The policy can be used to specify timeouts.
    pub fn exists<'a>(&self, key: &Key, policy: Option<&ReadPolicy>, py: Python<'a>) -> PyResult<&'a PyAny> {
        let policy = policy.map(|policy| policy._as.clone()).unwrap_or(aerospike_core::policy::ReadPolicy::default());
        let key = key._as.clone();
        let client = self._as.clone();

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let res = client
                .read()
                .await
                .exists(&policy, &key)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?;

                Ok(res)
        })
    }

    /// Removes all records in the specified namespace/set efficiently.
    pub fn truncate<'a>(
        &self,
        namespace: String,
        set_name: String,
        before_nanos: Option<i64>,
        py: Python<'a>,
    ) -> PyResult<&'a PyAny> {
        let client = self._as.clone();

        let before_nanos = before_nanos.unwrap_or_default();

        pyo3_asyncio::tokio::future_into_py(py, async move {
            client
                .read()
                .await
                .truncate(&namespace, &set_name, before_nanos)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?;

                Python::with_gil(|py| Ok(py.None()))
        })
    }

    /// Create a secondary index on a bin containing scalar values. This asynchronous server call
    /// returns before the command is complete.
    #[allow(clippy::too_many_arguments)]
    pub fn create_index<'a>(
        &self,
        namespace: String,
        set_name: String,
        bin_name: String,
        index_name: String,
        index_type: IndexType,
        cit: Option<CollectionIndexType>,
        py: Python<'a>,
    ) -> PyResult<&'a PyAny> {
        let client = self._as.clone();

        let cit = (&cit.unwrap_or(CollectionIndexType::Default)).into();
        let index_type = (&index_type).into();

        pyo3_asyncio::tokio::future_into_py(py, async move {
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
                .map_err(|e| PyException::new_err(e.to_string()))?;

                Python::with_gil(|py| Ok(py.None()))
        })
    }

    pub fn drop_index<'a>(&self, namespace: String, set_name: String, index_name: String, py: Python<'a>) -> PyResult<&'a PyAny> {
        let client = self._as.clone();

        pyo3_asyncio::tokio::future_into_py(py, async move {
            client
                .read()
                .await
                .drop_index(&namespace, &set_name, &index_name)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?;

                Python::with_gil(|py| Ok(py.None()))
        })
    }

    /// Read all records in the specified namespace and set and return a record iterator. The scan
    /// executor puts records on a queue in separate threads. The calling thread concurrently pops
    /// records off the queue through the record iterator. Up to `policy.max_concurrent_nodes`
    /// nodes are scanned in parallel. If concurrent nodes is set to zero, the server nodes are
    /// read in series.
    pub fn scan<'a>(
        &self,
        namespace: String,
        set_name: String,
        bins: Option<Vec<String>>,
        policy: Option<&ScanPolicy>,
        py: Python<'a>,
    ) -> PyResult<&'a PyAny> {
        let policy = policy.map(|policy| policy._as.clone()).unwrap_or(aerospike_core::policy::ScanPolicy::default());
        let client = self._as.clone();

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let res = client
                .read()
                .await
                .scan(&policy, &namespace, &set_name, bins_flag(bins))
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?;

                Ok(Recordset{_as: res})
        })

    }

    /// Execute a query on all server nodes and return a record iterator. The query executor puts
    /// records on a queue in separate threads. The calling thread concurrently pops records off
    /// the queue through the record iterator.
    pub fn query<'a>(&self, statement: &Statement, policy: Option<&QueryPolicy>, py: Python<'a>) -> PyResult<&'a PyAny> {
        let policy = policy.map(|policy| policy._as.clone()).unwrap_or(aerospike_core::policy::QueryPolicy::default());
        let client = self._as.clone();
        let stmt = statement._as.clone();

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let res = client
                .read()
                .await
                .query(&policy, stmt)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?;

                Ok(Recordset{_as: res})
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

        pub fn __deepcopy__(&self, _memo: &PyDict) -> Self {
            // fast bitwise copy instead of python's pickling process
            self.clone()
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  Blob
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[pyclass(subclass, freelist = 1, module = "aerospike", sequence)]
    #[derive(Debug, Clone)]
    pub struct Blob {
        v: Vec<u8>,
        index: usize,
    }

    #[pymethods]
    impl Blob {
        #[new]
        pub fn new(value: Vec<u8>) -> Self {
            Blob { v: value, index: 0 }
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
            format!("{}", PythonValue::Blob(self.v.clone()))
        }

        fn __str__(&self) -> PyResult<String> {
            Ok(self.as_string())
        }

        fn __repr__(&self) -> PyResult<String> {
            let s = self.__str__()?;
            Ok(format!("Blob('{}')", s))
        }

        fn __getitem__(&mut self, idx: usize) -> PyResult<u8> {
            if idx >= self.v.len() {
                return Err(PyIndexError::new_err("index out of bound"));
            }
            Ok(self.v[idx])
        }

        fn __setitem__(&mut self, idx: usize, v: u8) -> PyResult<()> {
            if idx >= self.v.len() {
                return Err(PyIndexError::new_err("index out of bound"))
            }
            self.v[idx] = v;
            Ok(())
        }

        fn __delitem__(&mut self, idx: usize) -> PyResult<()> {
            if idx >= self.v.len() {
                return Err(PyIndexError::new_err("index out of bound"))
            }
            self.v.remove(idx);
            Ok(())
        }

        fn __concat__(&self, mut other: Blob) -> PyResult<Blob> {
            let mut new_blob = self.v.clone();
            new_blob.append(&mut other.v);
            Ok(Blob { v: new_blob, index: 0 })
        }

        fn __inplace_concat__(&mut self, mut other: Blob) -> PyResult<Blob> {
            self.v.append(&mut other.v);
            Ok(self.clone())
        }

        fn __repeat__(&self, times: usize) -> PyResult<Blob> {
            Ok(Blob { v: self.v.repeat(times), index: 0 })
        }

        fn __inplace_repeat__(&mut self, times: usize) -> PyResult<Blob> {
            self.__repeat__(times)
        }


        fn __hash__(&self) -> u64 {
            let mut s = DefaultHasher::new();
            self.v.hash(&mut s);
            s.finish()
        }

        fn __len__(&self) -> usize {
            return self.v.len()
        }

        fn __richcmp__(&self, other: &PyAny, op: CompareOp) -> bool {
            match op {
                CompareOp::Eq => {
                    let l: PyResult<Blob> = PyAny::extract(other);
                    if let Ok(l) = l {
                        return self.v == l.v;
                    }

                    let l: PyResult<Vec<u8>> = PyAny::extract(other);
                    if let Ok(l) = l {
                        return self.v == l;
                    }

                    false
                },
                CompareOp::Ne => {
                    let l: PyResult<Blob> = PyAny::extract(other);
                    if let Ok(l) = l {
                        return self.v != l.v;
                    }

                    let l: PyResult<Vec<u8>> = PyAny::extract(other);
                    if let Ok(l) = l {
                        return self.v != l;
                    }

                    true
                },
                _ => false,
            }
        }

        fn __iter__(&self) -> Self {
            self.clone()
        }

        fn __next__(&mut self, py: Python<'_>) -> IterNextOutput<PyObject, String> {
            let res = self.v.get(self.index);
            self.index += 1;
            match res {
                None => IterNextOutput::Return("ended".into()),
                Some(v) => {
                    IterNextOutput::Yield(v.clone().into_py(py))
                },
            }
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

    #[pyclass(subclass, freelist = 1, module = "aerospike", sequence)]
    #[derive(Debug, Clone)]
    pub struct Map {
        v: HashMap<PythonValue, PythonValue>,
    }

    #[pymethods]
    impl Map {
        #[new]
        pub fn new(v: HashMap<PythonValue, PythonValue>) -> Self {
            Map { v }
        }

        #[getter]
        pub fn get_value(&self) -> HashMap<PythonValue, PythonValue>{
            self.v.clone()
        }

        #[setter]
        pub fn set_value(&mut self, b: HashMap<PythonValue, PythonValue>) {
            self.v = b
        }

        /// Returns a string representation of the value.
        pub fn as_string(&self) -> String {
            format!("{}", PythonValue::HashMap(self.v.clone()))
        }

        // TODO: Change HashMap into BTreeMap and use that
        // This requires Rust Client implementation first
        // fn __hash__(&self) -> u64 {
        //     let mut s = DefaultHasher::new();
        //     self.v.hash(&mut s);
        //     s.finish()
        // }

        fn __richcmp__(&self, other: &PyAny, op: CompareOp) -> bool {
            match op {
                CompareOp::Eq => {
                    let l: PyResult<Map> = PyAny::extract(other);
                    if let Ok(l) = l {
                        return self.v == l.v;
                    }

                    let l: PyResult<HashMap<PythonValue, PythonValue>> = PyAny::extract(other);
                    if let Ok(l) = l {
                        return self.v == l;
                    }

                    false
                },
                CompareOp::Ne => {
                    let l: PyResult<Map> = PyAny::extract(other);
                    if let Ok(l) = l {
                        return self.v != l.v;
                    }

                    let l: PyResult<HashMap<PythonValue, PythonValue>> = PyAny::extract(other);
                    if let Ok(l) = l {
                        return self.v != l;
                    }

                    true
                },
                _ => false,
            }
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

    #[pyclass(subclass, freelist = 1, module = "aerospike", sequence)]
    #[derive(Debug, Clone)]
    pub struct List {
        v: Vec<PythonValue>,
        index: usize,
    }

    #[pymethods]
    impl List {
        #[new]
        pub fn new(value: Vec<PythonValue>) -> Self {
            List { v: value, index: 0 }
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
            format!("{}", PythonValue::List(self.v.clone()))
        }

        fn __str__(&self) -> PyResult<String> {
            Ok(format!("{}", PythonValue::List(self.v.clone())))
        }

        fn __repr__(&self) -> PyResult<String> {
            let s = self.__str__()?;
            Ok(format!("List('{}')", s))
        }

        fn __getitem__(&mut self, idx: usize) -> PyResult<PythonValue> {
            if idx > self.v.len() {
                return Err(PyIndexError::new_err("index out of bound"))
            }
            Ok(self.v[idx].clone())
        }

        fn __setitem__(&mut self, idx: usize, v: PythonValue) -> PyResult<()> {
            if idx >= self.v.len() {
                return Err(PyIndexError::new_err("index out of bound"))
            }
            self.v[idx] = v;
            Ok(())
        }

        fn __delitem__(&mut self, idx: usize) -> PyResult<()> {
            if idx >= self.v.len() {
                return Err(PyIndexError::new_err("index out of bound"))
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

        fn __richcmp__(&self, other: &PyAny, op: CompareOp) -> bool {
            match op {
                CompareOp::Eq => {
                    let l: PyResult<List> = PyAny::extract(other);
                    if let Ok(l) = l {
                        return self.v == l.v;
                    }

                    let l: PyResult<Vec<PythonValue>> = PyAny::extract(other);
                    if let Ok(l) = l {
                        return self.v == l;
                    }

                    false
                },
                CompareOp::Ne => {
                    let l: PyResult<List> = PyAny::extract(other);
                    if let Ok(l) = l {
                        return self.v != l.v;
                    }

                    let l: PyResult<Vec<PythonValue>> = PyAny::extract(other);
                    if let Ok(l) = l {
                        return self.v != l;
                    }

                    true
                },
                _ => false,
            }
        }

        fn __iter__(&self) -> Self {
            self.clone()
        }

        fn __next__(&mut self, py: Python<'_>) -> IterNextOutput<PyObject, String> {
            let res = self.v.get(self.index);
            self.index += 1;
            match res {
                None => IterNextOutput::Return("ended".into()),
                Some(v) => {
                    IterNextOutput::Yield(v.clone().into_py(py))
                },
            }
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

    #[pyclass(subclass, freelist = 1, module = "aerospike")]
    #[derive(Debug, Clone)]
    pub struct GeoJSON {
        v: String,
    }

    #[pymethods]
    impl GeoJSON {
        #[new]
        pub fn new(value: String) -> Self {
            GeoJSON { v: value}
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
            format!("{}", PythonValue::GeoJSON(self.v.clone()))
        }

        fn __richcmp__(&self, other: &PyAny, op: CompareOp) -> bool {
            match op {
                CompareOp::Eq => {
                    let l: PyResult<GeoJSON> = PyAny::extract(other);
                    if let Ok(l) = l {
                        return self.v == l.v;
                    }

                    let l: PyResult<String> = PyAny::extract(other);
                    if let Ok(l) = l {
                        return self.v == l;
                    }

                    false
                },
                CompareOp::Ne => {
                    let l: PyResult<GeoJSON> = PyAny::extract(other);
                    if let Ok(l) = l {
                        return self.v != l.v;
                    }

                    let l: PyResult<String> = PyAny::extract(other);
                    if let Ok(l) = l {
                        return self.v != l;
                    }

                    true
                },
                _ => false,
            }
        }

        fn __str__(&self) -> String {
            self.get_value()
        }

        fn __repr__(&self) -> String {
            self.as_string()
        }
    }

    // TODO: not sure why we need this
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

    #[pyclass(subclass, freelist = 1, module = "aerospike", sequence)]
    #[derive(Debug, Clone)]
    #[allow(clippy::upper_case_acronyms)]
    pub struct HLL {
        v: Vec<u8>,
    }

    #[pymethods]
    impl HLL {
        #[new]
        pub fn new(value: Vec<u8>) -> Self {
            HLL { v: value }
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
            format!("{}", PythonValue::HLL(self.v.clone()))
        }

        fn __richcmp__(&self, other: &PyAny, op: CompareOp) -> bool {
            match op {
                CompareOp::Eq => {
                    let l: PyResult<HLL> = PyAny::extract(other);
                    if let Ok(l) = l {
                        return self.v == l.v;
                    }

                    let l: PyResult<Vec<u8>> = PyAny::extract(other);
                    if let Ok(l) = l {
                        return self.v == l;
                    }

                    false
                },
                CompareOp::Ne => {
                    let l: PyResult<HLL> = PyAny::extract(other);
                    if let Ok(l) = l {
                        return self.v != l.v;
                    }

                    let l: PyResult<Vec<u8>> = PyAny::extract(other);
                    if let Ok(l) = l {
                        return self.v != l;
                    }

                    true
                },
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
}

impl fmt::Display for PythonValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> std::result::Result<(), fmt::Error> {
        match *self {
            PythonValue::Nil => write!(f, "{}", "None".to_string()),
            PythonValue::Int(ref val) => write!(f, "{}", val.to_string()),
            PythonValue::UInt(ref val) => write!(f, "{}", val.to_string()),
            PythonValue::Bool(ref val) => match val {
                true => write!(f, "{}", "True".to_string()),
                false => write!(f, "{}", "False".to_string()),
            },
            PythonValue::Float(ref val) => write!(f, "{}", val.to_string()),
            PythonValue::String(ref val) => write!(f, "\"{}\"", val.to_string()),
            PythonValue::GeoJSON(ref val) => write!(f, "{}", format!("GeoJSON('{}')", val)),
            PythonValue::Blob(ref val) => write!(f, "{}", format!("{:?}", val)),
            PythonValue::HLL(ref val) => write!(f, "{}", format!("HLL('{:?}')", val)),
            PythonValue::List(ref val) => {
                write!(f, "[")?;
                for (i, v) in val.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", v)?;
                }
                write!(f, "]")?;

                Ok(())
            },
            PythonValue::HashMap(ref val) => {
                write!(f, "{{")?;
                for (i, (k, v)) in val.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", k, v)?;
                }
                write!(f, "}}")?;

                Ok(())

            },
            // PythonValue::OrderedMap(ref val) => format!("{:?}", val),
        }
    }
}

impl IntoPy<PyObject> for PythonValue {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self {
            PythonValue::Nil => py.None(),
            PythonValue::Bool(b) => b.into_py(py),
            PythonValue::Int(i) => i.into_py(py),
            PythonValue::UInt(ui) => ui.into_py(py),
            PythonValue::Float(f) => f.into_py(py),
            PythonValue::String(s) => s.into_py(py),
            PythonValue::Blob(b) => Blob::new(b).into_py(py),
            PythonValue::List(l) => List::new(l).into_py(py),
            PythonValue::HashMap(h) => h.into_py(py),
            PythonValue::GeoJSON(s) => GeoJSON::new(s).into_py(py),
            PythonValue::HLL(b) => HLL::new(b).into_py(py),
        }
    }
}

impl FromPyObject<'_> for PythonValue {
    fn extract(arg: &pyo3::PyAny) -> PyResult<Self> {
        let b: PyResult<bool> = PyAny::extract(arg);
        if let Ok(b) = b {
            return Ok(PythonValue::Bool(b));
        }

        let i: PyResult<i64> = PyAny::extract(arg);
        if let Ok(i) = i {
            return Ok(PythonValue::Int(i));
        }

        let f1: PyResult<f64> = PyAny::extract(arg);
        if let Ok(f1) = f1 {
            return Ok(PythonValue::Float(ordered_float::OrderedFloat(f1)));
        }

        let s: PyResult<String> = PyAny::extract(arg);
        if let Ok(s) = s {
            return Ok(PythonValue::String(s));
        }

        let b: PyResult<&PyByteArray> = PyAny::extract(arg);
        if let Ok(b) = b {
            return Ok(PythonValue::Blob(b.to_vec()));
        }

        let b: PyResult<&PyBytes> = PyAny::extract(arg);
        if let Ok(b) = b {
            return Ok(PythonValue::Blob(b.as_bytes().to_vec()));
        }

        let b: PyResult<Blob> = PyAny::extract(arg);
        if let Ok(b) = b {
            return Ok(PythonValue::Blob(b.v));
        }

        let l: PyResult<Vec<PythonValue>> = PyAny::extract(arg);
        if let Ok(l) = l {
            return Ok(PythonValue::List(l));
        }

        let l: PyResult<List> = PyAny::extract(arg);
        if let Ok(l) = l {
            return Ok(PythonValue::List(l.v));
        }

        let hm: PyResult<HashMap<PythonValue, PythonValue>> = PyAny::extract(arg);
        if let Ok(hm) = hm {
            return Ok(PythonValue::HashMap(hm));
        }

        let hm: PyResult<HashMap<PythonValue, PythonValue>> = PyAny::extract(arg);
        if let Ok(hm) = hm {
            return Ok(PythonValue::HashMap(hm));
        }

        let geo: PyResult<GeoJSON> = PyAny::extract(arg);
        if let Ok(geo) = geo {
            return Ok(PythonValue::GeoJSON(geo.v));
        }

        let hll: PyResult<HLL> = PyAny::extract(arg);
        if let Ok(hll) = hll {
            return Ok(PythonValue::HLL(hll.v));
        }

        Err(PyTypeError::new_err(format!("invalid value {}", arg)))
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

    m.add_class::<Priority>()?;
    m.add_class::<Expiration>()?;
    m.add_class::<CommitLevel>()?;
    m.add_class::<ConsistencyLevel>()?;
    m.add_class::<RecordExistsAction>()?;
    m.add_class::<GenerationPolicy>()?;
    m.add_class::<IndexType>()?;
    m.add_class::<CollectionIndexType>()?;
    m.add_class::<RegexFlag>()?;

    m.add_class::<List>()?;
    // TODO: Implement map and make it an ordered map
    // Needs Rust Client implementation
    // m.add_class::<Map>()?;
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
    m.add_class::<Client>()?;

    m.add_class::<ReadPolicy>()?;
    m.add_class::<ClientPolicy>()?;
    m.add_class::<WritePolicy>()?;
    m.add_class::<ScanPolicy>()?;
    m.add_class::<QueryPolicy>()?;

    m.add_function(wrap_pyfunction!(new_client, m)?)?;

    Ok(())
}
