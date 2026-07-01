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

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use aerospike_core::errors::Error;
use aerospike_core::ResultCode as CoreResultCode;

use crate::enums::ResultCode;

create_exception!(aerospike_async.exceptions, AerospikeError, pyo3::exceptions::PyException);

// Server-related exceptions
// ServerError is a custom exception with a result_code property
// Note: It extends PyException directly, but Python-side it should be treated as an AerospikeError subclass
#[gen_stub_pyclass(module = "_aerospike_async_native")]
#[pyclass(extends = PyException, subclass)]
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

// Resolve the Python ServerError subclass for the given result code (for dispatch).
fn resolve_server_error_class(py: Python<'_>, result_code: CoreResultCode) -> PyResult<pyo3::Bound<'_, pyo3::types::PyAny>> {
    let module = py.import("aerospike_async.exceptions")?;
    let func = module.getattr("_get_server_error_class")?;
    let rc_wrapper = ResultCode(result_code);
    let py_rc = Py::new(py, rc_wrapper)?;
    func.call1((py_rc,))
}

// Helper function to create ServerError (or subclass) as a PyErr
fn create_server_error(message: String, result_code: CoreResultCode, in_doubt: bool) -> PyErr {
    Python::attach(|py| -> PyErr {
        let exc_cls = resolve_server_error_class(py, result_code)
            .unwrap_or_else(|_| py.get_type::<ServerError>().into_any());
        let rc = ResultCode(result_code);
        match exc_cls.call1((message.clone(), rc, in_doubt)) {
            Ok(obj) => PyErr::from_value(obj),
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

// Per-node circuit breaker tripped (client-side, not sent to server). Carries
// the offending node identifier in the exception message. Raised when a node
// exceeds the policy's `max_error_rate` over `error_rate_window` ticks, so the
// client backs off rather than forwarding more commands to that node.
create_exception!(aerospike_async.exceptions, MaxErrorRate, AerospikeError);


// Must define a wrapper type because of the orphan rule
pub struct RustClientError(pub(crate) Error);

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
            Error::MaxErrorRate(node) => MaxErrorRate::new_err(format!(
                "Max error rate exceeded for node {node}; backing off"
            )),
            #[allow(unreachable_patterns)]
            other => AerospikeError::new_err(format!("Unknown error: {:?}", other)),
        }
    }
}
