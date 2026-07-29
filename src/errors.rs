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

use std::marker::PhantomData;

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::PyErrArguments;
use pyo3::PyTypeInfo;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use aerospike_core::errors::{Error, ErrorKind};
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

// Deferred arguments for a ServerError (or subclass) PyErr.  Holds only plain
// data (Send + Sync + 'static) so it can be carried across threads without
// touching Python.  `detail` carries the extended server error detail
// (subcode / message / expression trace) when the server attached one.
struct ServerErrorArgs {
    message: String,
    result_code: CoreResultCode,
    in_doubt: bool,
    detail: Option<aerospike_core::ServerErrorDetail>,
}

impl PyErrArguments for ServerErrorArgs {
    // Runs when the PyErr is first materialized — for async completions that is
    // on the event-loop/drainer thread inside `set_exception`, never on a Tokio
    // worker.  Builds the concrete subclass *instance* (with any extended error
    // detail) and hands it back as the exception value.
    fn arguments(self, py: Python<'_>) -> Py<PyAny> {
        let exc_cls = resolve_server_error_class(py, self.result_code)
            .unwrap_or_else(|_| py.get_type::<ServerError>().into_any());
        let rc = ResultCode(self.result_code);
        let detail = self.detail.as_ref();
        let sub_code = detail.map(|d| d.sub_code);
        let server_message = detail.map(|d| d.message.clone());
        let exp_trace = detail.and_then(|d| d.exp_trace.as_ref().map(ExpressionTrace::from_core));
        match exc_cls.call1((self.message, rc, self.in_doubt, sub_code, server_message, exp_trace)) {
            Ok(obj) => obj.unbind(),
            // Construction failed — surface that failure's own exception value.
            Err(e) => e.into_value(py).into_any(),
        }
    }
}

// Helper function to create ServerError (or subclass) as a PyErr.
//
// The conversion is *lazy*: `PyErr::new` stores `ServerErrorArgs` and only
// materializes the exception when it is first inspected.  For async completions
// that happens on the event-loop/drainer thread (`set_exception`), NOT on the
// Tokio worker that produced the error.  This keeps `Python::attach` off Tokio
// workers (see the invariant in waker.rs): a worker that attaches registers a
// PyThreadState whose free-threaded finalization teardown
// (`_Py_brc_remove_thread`) segfaults, and attaching mid-finalization panics.
//
// The subclass instance is built in `arguments()` and passed as the exception
// *value* under the base `ServerError` type; CPython's exception normalization
// narrows the reported type to that subclass, so `except RecordNotFound` (etc.)
// still matches.
fn create_server_error(
    message: String,
    result_code: CoreResultCode,
    in_doubt: bool,
    detail: Option<&aerospike_core::ServerErrorDetail>,
) -> PyErr {
    PyErr::new::<ServerError, _>(ServerErrorArgs {
        message,
        result_code,
        in_doubt,
        detail: detail.cloned(),
    })
}

// Deferred arguments for a client-side error whose core cause chain is marked
// in-doubt.  Lazy for the same reason as `ServerErrorArgs` above: `arguments()`
// runs when the PyErr is first materialized, on the event-loop/drainer thread,
// never on a Tokio worker.  Builds the exception instance and sets the
// `in_doubt` instance attribute, overriding the `False` class default that
// `AerospikeError` declares on the Python side.
struct InDoubtArgs<T> {
    message: String,
    // `fn() -> T` keeps this Send + Sync regardless of T: the generated
    // exception types wrap `PyAny`, which is not Sync.
    _cls: PhantomData<fn() -> T>,
}

impl<T: PyTypeInfo + 'static> PyErrArguments for InDoubtArgs<T> {
    fn arguments(self, py: Python<'_>) -> Py<PyAny> {
        match py.get_type::<T>().call1((self.message,)) {
            Ok(obj) => {
                // Best effort: a failed setattr falls back to the class
                // default False rather than masking the original error.
                let _ = obj.setattr("in_doubt", true);
                obj.unbind()
            }
            // Construction failed — surface that failure's own exception value.
            Err(e) => e.into_value(py).into_any(),
        }
    }
}

// Not-in-doubt is the overwhelmingly common case and stays on the existing
// fast path: no setattr, no extra allocation, identical to `T::new_err(msg)`.
fn client_err<T>(message: String, in_doubt: bool) -> PyErr
where
    T: PyTypeInfo + 'static,
{
    if in_doubt {
        PyErr::new::<T, _>(InDoubtArgs::<T> { message, _cls: PhantomData })
    } else {
        PyErr::new::<T, _>(message)
    }
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
        // RustClientError -> Error -> Custom Exception Classes.
        //
        // The core `Error` is now an opaque struct carrying an `ErrorKind`
        // plus metadata that drills through the retry cause chain (server
        // result code, in-doubt, node, extended detail). Retry wrapping is
        // handled entirely by core, so we no longer walk a `Chain` variant
        // by hand: the accessors already surface the most specific reachable
        // server code, and `Display` renders the full context (iteration,
        // node, sub-errors, cause) for the message.
        let err = value.0;

        // A server result code reachable anywhere in the cause chain wins: it
        // carries the typed ResultCode + extended detail + in-doubt + node,
        // and maps to the concrete ServerError subclass (RecordNotFound, etc.).
        if let Some(result_code) = err.server_result_code() {
            let in_doubt = err.in_doubt();
            let detail = err.server_error_detail();
            let node = err.node().unwrap_or("");
            let mut message = format!("Code: {:?}, In Doubt: {}, Node: {}", result_code, in_doubt, node);
            if let Some(detail) = detail {
                // Extended server error detail (subcode / message / exp trace),
                // present when error_detail_verbosity > 0 and the server
                // (>= 8.1.3) attached one.
                message.push_str(&format!(", Detail: {detail}"));
            }
            return create_server_error(message, result_code, in_doubt, detail);
        }

        // Otherwise dispatch on the specific client-side failure. `Display`
        // carries the full retry context, so `except TimeoutError` /
        // `except ConnectionError` still match retried failures.
        let msg = err.to_string();
        // Typed in-doubt from core; `in_doubt()` walks the cause chain, so a
        // wrapper (retry decoration, BatchFailed, NoMoreConnections over an
        // in-doubt failure) inherits it.  Every kind that can carry a cause
        // chain routes through `client_err`; the pure conversion failures
        // (Base64 .. PwHash) never do and stay on `new_err`.
        let in_doubt = err.in_doubt();
        match err.kind() {
            ErrorKind::Timeout => client_err::<TimeoutError>(msg, in_doubt),
            ErrorKind::Connection | ErrorKind::ConnectionPoolEmpty => {
                client_err::<ConnectionError>(msg, in_doubt)
            }
            ErrorKind::NoMoreConnections => client_err::<NoMoreConnections>(msg, in_doubt),
            ErrorKind::MaxErrorRate => client_err::<MaxErrorRate>(msg, in_doubt),
            ErrorKind::InvalidNode => client_err::<InvalidNodeError>(msg, in_doubt),
            ErrorKind::InvalidNamespace => client_err::<InvalidNamespaceError>(msg, in_doubt),
            ErrorKind::InvalidArgument => client_err::<ValueError>(msg, in_doubt),
            ErrorKind::BadResponse | ErrorKind::ParsePeers => {
                client_err::<BadResponse>(msg, in_doubt)
            }
            ErrorKind::UdfBadResponse => client_err::<UDFBadResponse>(msg, in_doubt),
            ErrorKind::Commit { error_type, .. } => {
                client_err::<CommitFailedError>(format!("{error_type} (in_doubt={in_doubt})"), in_doubt)
            }
            ErrorKind::Base64(e) => Base64DecodeError::new_err(e.to_string()),
            ErrorKind::InvalidUtf8(e) => InvalidUTF8::new_err(e.to_string()),
            ErrorKind::Io(e) => IoError::new_err(e.to_string()),
            ErrorKind::ParseAddr(e) => ParseAddressError::new_err(e.to_string()),
            ErrorKind::ParseInt(e) => ParseIntError::new_err(e.to_string()),
            ErrorKind::PwHash(e) => PasswordHashError::new_err(e.to_string()),
            // Client / StreamTerminated / BatchFailed / BatchRow / Async and
            // any future kinds fall back to the generic client error, keeping
            // the full context from `Display`. (Server / Timeout / Connection
            // etc. are handled above.)
            _ => client_err::<ClientError>(msg, in_doubt),
        }
    }
}
