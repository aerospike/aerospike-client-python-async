# Exceptions are created by PyO3 in this submodule
# via create_exception!(aerospike_async.exceptions, ...) and add_submodule
# Users can import: from aerospike_async.exceptions import AerospikeError

import sys
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
NoMoreConnections = _exceptions.NoMoreConnections
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
# ResultCode is in the main native module, not in exceptions submodule
ResultCode = _aerospike_async_native.ResultCode
