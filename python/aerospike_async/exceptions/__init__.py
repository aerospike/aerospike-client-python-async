# Exceptions are created by PyO3 in this submodule
# via create_exception!(aerospike_async.exceptions, ...) and add_submodule
# Users can import: from aerospike_async.exceptions import AerospikeError

from .. import _aerospike_async_native

# Access the exceptions submodule created by PyO3
_exceptions = getattr(_aerospike_async_native, "exceptions", None)
if _exceptions is None:
    raise ImportError("Exceptions submodule not found in native module")

# Re-export all exception classes
AerospikeError = _exceptions.AerospikeError
ServerError = _exceptions.ServerError
UDFBadResponse = _exceptions.UDFBadResponse
TimeoutError = _exceptions.TimeoutError
BadResponse = _exceptions.BadResponse
ConnectionError = _exceptions.ConnectionError
InvalidNodeError = _exceptions.InvalidNodeError
InvalidNamespaceError = _exceptions.InvalidNamespaceError
NoMoreConnections = _exceptions.NoMoreConnections
CommitFailedError = _exceptions.CommitFailedError
BatchFailedError = _exceptions.BatchFailedError
RecvError = _exceptions.RecvError
Base64DecodeError = _exceptions.Base64DecodeError
InvalidUTF8 = _exceptions.InvalidUTF8
ParseAddressError = _exceptions.ParseAddressError
ParseIntError = _exceptions.ParseIntError
ValueError = _exceptions.ValueError
IoError = _exceptions.IoError
PasswordHashError = _exceptions.PasswordHashError
InvalidRustClientArgs = _exceptions.InvalidRustClientArgs
ClientError = _exceptions.ClientError
MaxErrorRate = _exceptions.MaxErrorRate
# ResultCode is in the main native module, not in exceptions submodule
ResultCode = _aerospike_async_native.ResultCode

# Typed in-doubt lives on the base so every error answers it; the native
# layer sets the instance attribute only when core reports the write may
# have landed.
AerospikeError.in_doubt = False
# Retry/diagnostic context defaults, same mechanism: the native layer
# sets the instance attribute only when the retry loop recorded a value.
AerospikeError.node = None
AerospikeError.iteration = None
AerospikeError.base_message = None
AerospikeError.sub_exceptions = None
# Per-key outcomes; the native layer attaches the list on batch failures.
BatchFailedError.records = None

# ServerError subclasses for specific result codes (grouping bases first)
class RecordError(ServerError):
    """Record-level server errors."""


class IndexError(ServerError):
    """Index-related server errors."""


class SecurityError(ServerError):
    """Security and authentication server errors."""


# Tier 1 — core server errors
class RecordNotFound(RecordError):
    """Record not found (KEY_NOT_FOUND_ERROR)."""

class GenerationError(RecordError):
    """Generation check failed (GENERATION_ERROR)."""

class InvalidRequest(ServerError):
    """Invalid request / parameter error (PARAMETER_ERROR)."""

class RecordExistsError(RecordError):
    """Record already exists (KEY_EXISTS_ERROR)."""

class BinTypeError(RecordError):
    """Bin type incompatible (BIN_TYPE_ERROR)."""

class RecordTooBig(RecordError):
    """Record too big (RECORD_TOO_BIG)."""

class BinNotFound(RecordError):
    """Bin not found (BIN_NOT_FOUND)."""

class FilteredOut(ServerError):
    """Record filtered out (FILTERED_OUT)."""

class OpNotApplicable(ServerError):
    """Operation not applicable (OP_NOT_APPLICABLE)."""

# Tier 2 — index and security
class IndexNotFound(IndexError):
    """Index not found (INDEX_NOT_FOUND)."""

class IndexFoundError(IndexError):
    """Index already exists (INDEX_FOUND)."""

class NotAuthenticated(SecurityError):
    """Not authenticated (NOT_AUTHENTICATED)."""

class SecurityNotEnabled(SecurityError):
    """Security not enabled (SECURITY_NOT_ENABLED)."""

# Tier 3 — subsystem families (query/scan, batch, quota, UDF)
class QueryError(ServerError):
    """Query and scan server errors."""

class BatchError(ServerError):
    """Batch subsystem server errors."""

class QuotaError(ServerError):
    """Quota server errors."""

class UdfError(ServerError):
    """Server-side UDF execution errors (UDF_BAD_RESPONSE)."""

# ResultCode -> exception class for Rust create_server_error() dispatch
_RC_TO_CLS = {
    ResultCode.KEY_NOT_FOUND_ERROR: RecordNotFound,
    ResultCode.GENERATION_ERROR: GenerationError,
    ResultCode.PARAMETER_ERROR: InvalidRequest,
    ResultCode.KEY_EXISTS_ERROR: RecordExistsError,
    ResultCode.BIN_TYPE_ERROR: BinTypeError,
    ResultCode.RECORD_TOO_BIG: RecordTooBig,
    ResultCode.BIN_NOT_FOUND: BinNotFound,
    ResultCode.FILTERED_OUT: FilteredOut,
    ResultCode.OP_NOT_APPLICABLE: OpNotApplicable,
    ResultCode.INDEX_NOT_FOUND: IndexNotFound,
    ResultCode.INDEX_FOUND: IndexFoundError,
    ResultCode.NOT_AUTHENTICATED: NotAuthenticated,
    ResultCode.SECURITY_NOT_ENABLED: SecurityNotEnabled,
    # Security family (flat: the finer authentication-vs-
    # authorization split is an SDK-level concern)
    ResultCode.ILLEGAL_STATE: SecurityError,
    ResultCode.INVALID_USER: SecurityError,
    ResultCode.USER_ALREADY_EXISTS: SecurityError,
    ResultCode.INVALID_PASSWORD: SecurityError,
    ResultCode.EXPIRED_PASSWORD: SecurityError,
    ResultCode.FORBIDDEN_PASSWORD: SecurityError,
    ResultCode.INVALID_CREDENTIAL: SecurityError,
    ResultCode.EXPIRED_SESSION: SecurityError,
    ResultCode.INVALID_ROLE: SecurityError,
    ResultCode.ROLE_ALREADY_EXISTS: SecurityError,
    ResultCode.INVALID_PRIVILEGE: SecurityError,
    ResultCode.INVALID_ALLOWLIST: SecurityError,
    ResultCode.ROLE_VIOLATION: SecurityError,
    ResultCode.NOT_ALLOWLISTED: SecurityError,
    ResultCode.SECURITY_NOT_SUPPORTED: SecurityError,
    ResultCode.SECURITY_SCHEME_NOT_SUPPORTED: SecurityError,
    # Query/scan family
    ResultCode.QUERY_GENERIC: QueryError,
    ResultCode.QUERY_ABORTED: QueryError,
    ResultCode.QUERY_QUEUE_FULL: QueryError,
    ResultCode.QUERY_NETIO_ERR: QueryError,
    ResultCode.QUERY_DUPLICATE: QueryError,
    ResultCode.SCAN_ABORT: QueryError,
    # Batch family
    ResultCode.BATCH_DISABLED: BatchError,
    ResultCode.BATCH_MAX_REQUESTS_EXCEEDED: BatchError,
    ResultCode.BATCH_QUEUES_FULL: BatchError,
    # Quota family
    ResultCode.QUOTA_EXCEEDED: QuotaError,
    ResultCode.QUOTAS_NOT_ENABLED: QuotaError,
    ResultCode.INVALID_QUOTA: QuotaError,
    # UDF
    ResultCode.UDF_BAD_RESPONSE: UdfError,
}

def _get_server_error_class(result_code):
    """Return the ServerError subclass for the given result code, or ServerError."""
    return _RC_TO_CLS.get(result_code, ServerError)
