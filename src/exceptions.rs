use pyo3::create_exception;
use pyo3::prelude::*;

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

/// Register all exceptions in the exceptions module
pub fn register_exceptions(py: Python, m: &pyo3::Bound<pyo3::types::PyModule>) -> PyResult<()> {
    println!("Registering exceptions in submodule...");

    // Register all exceptions
    m.add("AerospikeError", py.get_type::<AerospikeError>())?;
    m.add("ServerError", py.get_type::<ServerError>())?;
    m.add("UDFBadResponse", py.get_type::<UDFBadResponse>())?;
    m.add("TimeoutError", py.get_type::<TimeoutError>())?;
    m.add("BadResponse", py.get_type::<BadResponse>())?;
    m.add("ConnectionError", py.get_type::<ConnectionError>())?;
    m.add("InvalidNodeError", py.get_type::<InvalidNodeError>())?;
    m.add("NoMoreConnections", py.get_type::<NoMoreConnections>())?;
    m.add("RecvError", py.get_type::<RecvError>())?;
    m.add("Base64DecodeError", py.get_type::<Base64DecodeError>())?;
    m.add("InvalidUTF8", py.get_type::<InvalidUTF8>())?;
    m.add("ParseAddressError", py.get_type::<ParseAddressError>())?;
    m.add("ParseIntError", py.get_type::<ParseIntError>())?;
    m.add("ValueError", py.get_type::<ValueError>())?;
    m.add("IoError", py.get_type::<IoError>())?;
    m.add("PasswordHashError", py.get_type::<PasswordHashError>())?;
    m.add("InvalidRustClientArgs", py.get_type::<InvalidRustClientArgs>())?;

    println!("All exceptions registered successfully!");
    Ok(())
}