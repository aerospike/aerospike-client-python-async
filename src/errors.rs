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
create_exception!(aerospike_async.exceptions, InvalidNamespaceError, AerospikeError);
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
create_exception!(aerospike_async.exceptions, CommitFailedError, AerospikeError);


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
            Error::InvalidNamespace(string) => InvalidNamespaceError::new_err(string),
            Error::NoMoreConnections => NoMoreConnections::new_err("Exceeded max. number of connections per node."),
            Error::ServerError(result_code, in_doubt, node) => {
                let message = format!("Code: {:?}, In Doubt: {}, Node: {}", result_code, in_doubt, node);
                create_server_error(message, result_code, in_doubt)
            },
            Error::UdfBadResponse(string) => UDFBadResponse::new_err(string),
            Error::Timeout(string) => TimeoutError::new_err(string),
            Error::Chain(first, second) => {
                // v3 wraps errors as Chain(outer, cause). Promote the most
                // specific error: if either side is a ServerError, use that;
                // otherwise convert the outer and append the cause message.
                fn find_server_error(e: &Error) -> Option<(CoreResultCode, bool, &str)> {
                    match e {
                        Error::ServerError(rc, id, node) => Some((*rc, *id, node.as_str())),
                        _ => None,
                    }
                }

                if let Some((rc, id, node)) = find_server_error(&first).or_else(|| find_server_error(&second)) {
                    let message = format!("Code: {:?}, In Doubt: {}, Node: {}", rc, id, node);
                    return create_server_error(message, rc, id);
                }

                let cause_msg = format!("{}", second);
                match *first {
                    Error::Timeout(msg) => {
                        TimeoutError::new_err(format!("{msg}: {cause_msg}"))
                    },
                    Error::Connection(msg) => {
                        ConnectionError::new_err(format!("{msg}: {cause_msg}"))
                    },
                    Error::InvalidNode(msg) => {
                        InvalidNodeError::new_err(format!("{msg}: {cause_msg}"))
                    },
                    Error::InvalidNamespace(msg) => {
                        InvalidNamespaceError::new_err(format!("{msg}: {cause_msg}"))
                    },
                    other => {
                        let outer_err: PyErr = RustClientError(other).into();
                        let msg = format!("{}: {}", outer_err, cause_msg);
                        AerospikeError::new_err(msg)
                    }
                }
            },
            Error::ClientError(msg) => ClientError::new_err(msg),
            Error::CommitFailed { error_type, in_doubt, .. } => {
                CommitFailedError::new_err(format!("{error_type} (in_doubt={in_doubt})"))
            },
            #[allow(unreachable_patterns)]
            other => AerospikeError::new_err(format!("Unknown error: {:?}", other)),
        }
    }
}
