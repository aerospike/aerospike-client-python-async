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
use crate::server_error::ExpressionTrace;

create_exception!(aerospike_async.exceptions, AerospikeError, pyo3::exceptions::PyException);

// Server-related exceptions
// ServerError is a custom exception with a result_code property
// Note: It extends PyException directly, but Python-side it should be treated as an AerospikeError subclass
#[gen_stub_pyclass(module = "_aerospike_async_native")]
#[pyclass(extends = PyException, subclass)]
pub struct ServerError {
    result_code: CoreResultCode,
    in_doubt: bool,
    sub_code: Option<u32>,
    server_message: Option<String>,
    exp_trace: Option<ExpressionTrace>,
}

#[gen_stub_pymethods]
#[pymethods]
impl ServerError {
    #[new]
    #[pyo3(signature = (_message, result_code, in_doubt=false, sub_code=None, server_message=None, exp_trace=None))]
    fn new(
        _message: String,
        result_code: ResultCode,
        in_doubt: bool,
        sub_code: Option<u32>,
        server_message: Option<String>,
        exp_trace: Option<ExpressionTrace>,
    ) -> PyResult<Self> {
        // Note: message is handled by the base PyException; the fields here
        // are the structured accessors.
        Ok(ServerError {
            result_code: result_code.0,
            in_doubt,
            sub_code,
            server_message,
            exp_trace,
        })
    }

    #[getter]
    fn result_code(&self) -> ResultCode {
        ResultCode(self.result_code)
    }

    #[getter]
    fn in_doubt(&self) -> bool {
        self.in_doubt
    }

    /// Server-supplied error subcode, present when the request asked for
    /// extended error detail (``error_detail_verbosity`` >= 1) and the
    /// server (>= 8.1.3) attached one. Subcode values are scoped to their
    /// parent result code — interpret the (result_code, sub_code) pair.
    #[getter]
    fn sub_code(&self) -> Option<u32> {
        self.sub_code
    }

    /// Server-supplied error detail message, present at
    /// ``error_detail_verbosity`` >= 2 when the server attached one.
    #[getter]
    fn server_message(&self) -> Option<String> {
        self.server_message.clone()
    }

    /// Structured server-supplied expression build trace, present only at
    /// ``error_detail_verbosity`` 3 on an expression build failure and when the
    /// server build emits one.
    #[getter]
    fn exp_trace(&self) -> Option<ExpressionTrace> {
        self.exp_trace.clone()
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
fn create_server_error(
    message: String,
    result_code: CoreResultCode,
    in_doubt: bool,
    detail: Option<&aerospike_core::ServerErrorDetail>,
) -> PyErr {
    Python::attach(|py| -> PyErr {
        let exc_cls = resolve_server_error_class(py, result_code)
            .unwrap_or_else(|_| py.get_type::<ServerError>().into_any());
        let rc = ResultCode(result_code);
        let sub_code = detail.map(|d| d.sub_code);
        let server_message = detail.map(|d| d.message.clone());
        let exp_trace = detail.and_then(|d| d.exp_trace.as_ref().map(ExpressionTrace::from_core));
        match exc_cls.call1((message.clone(), rc, in_doubt, sub_code, server_message, exp_trace)) {
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
            Error::InvalidNamespace(string) => InvalidNamespaceError::new_err(string),
            Error::NoMoreConnections => NoMoreConnections::new_err("Exceeded max. number of connections per node."),
            Error::ServerError(result_code, in_doubt, node, detail) => {
                let mut message = format!("Code: {:?}, In Doubt: {}, Node: {}", result_code, in_doubt, node);
                if let Some(detail) = &detail {
                    // Extended server error detail (subcode / message / exp
                    // trace), present when error_detail_verbosity > 0 and the
                    // server (>= 8.1.3) attached one.
                    message.push_str(&format!(", Detail: {detail}"));
                }
                create_server_error(message, result_code, in_doubt, detail.as_deref())
            },
            Error::UdfBadResponse(string) => UDFBadResponse::new_err(string),
            Error::Timeout(string) => TimeoutError::new_err(string),
            Error::Chain(first, second) => {
                // v3's `Error::with_retry_context` wraps retried errors as
                // `Chain(ClientError("iterations=…"), <inner>)`, where
                // `<inner>` may itself be another Chain holding the prior
                // sub-errors. The outer-first slot is therefore no longer
                // reliably the "interesting" variant — we have to walk the
                // whole chain and promote the most specific reachable
                // typed leaf so callers can still `except TimeoutError` /
                // `except ConnectionError` against retried failures.
                fn flatten<'a>(e: &'a Error, out: &mut Vec<&'a Error>) {
                    if let Error::Chain(a, b) = e {
                        flatten(a, out);
                        flatten(b, out);
                    } else {
                        out.push(e);
                    }
                }
                let mut leaves: Vec<&Error> = Vec::with_capacity(4);
                flatten(&first, &mut leaves);
                flatten(&second, &mut leaves);

                // Combined Display string preserves the iteration / last-node /
                // sub-error context that `with_retry_context` attaches at the
                // front of the chain — matches `Display for Error::Chain`.
                let combined_msg = format!("{}\n\t{}", first, second);

                // ServerError carries result code + in_doubt + node, so it
                // wins over transport-level promotions when both are present.
                if let Some((rc, id, node, detail)) = leaves.iter().find_map(|n| match n {
                    Error::ServerError(rc, id, node, detail) => {
                        Some((*rc, *id, node.as_str(), detail.as_deref()))
                    }
                    _ => None,
                }) {
                    let mut message = format!("Code: {:?}, In Doubt: {}, Node: {}", rc, id, node);
                    if let Some(detail) = detail {
                        message.push_str(&format!(", Detail: {detail}"));
                    }
                    return create_server_error(message, rc, id, detail);
                }

                // Transport-level promotions, in priority order. Timeout and
                // Connection are the common retried cases; InvalidNode /
                // InvalidNamespace can show up in handshake retries.
                if leaves.iter().any(|n| matches!(n, Error::Timeout(_))) {
                    return TimeoutError::new_err(combined_msg);
                }
                if leaves.iter().any(|n| matches!(n, Error::Connection(_))) {
                    return ConnectionError::new_err(combined_msg);
                }
                if leaves.iter().any(|n| matches!(n, Error::InvalidNode(_))) {
                    return InvalidNodeError::new_err(combined_msg);
                }
                if leaves.iter().any(|n| matches!(n, Error::InvalidNamespace(_))) {
                    return InvalidNamespaceError::new_err(combined_msg);
                }

                // Nothing typed reachable — surface the joined chain text on
                // the generic AerospikeError. Avoids the recursive-conversion
                // path of the previous implementation (which could swallow
                // the cause when the outer-first was itself a Chain).
                AerospikeError::new_err(combined_msg)
            },
            Error::ClientError(msg) => ClientError::new_err(msg),
            Error::CommitFailed { error_type, in_doubt, .. } => {
                CommitFailedError::new_err(format!("{error_type} (in_doubt={in_doubt})"))
            },
            Error::MaxErrorRate(node) => MaxErrorRate::new_err(format!(
                "Max error rate exceeded for node {node}; backing off"
            )),
            #[allow(unreachable_patterns)]
            other => AerospikeError::new_err(format!("Unknown error: {:?}", other)),
        }
    }
}
