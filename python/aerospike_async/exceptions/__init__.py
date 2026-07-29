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
}

def _get_server_error_class(result_code):
    """Return the ServerError subclass for the given result code, or ServerError."""
    return _RC_TO_CLS.get(result_code, ServerError)
