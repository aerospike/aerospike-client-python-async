# Import exceptions from the compiled native module
# Exceptions are registered in Rust and available from the parent module
from .._aerospike_async_native import (
    AerospikeError,
    ServerError,
    UDFBadResponse,
    TimeoutError,
    BadResponse,
    ConnectionError,
    InvalidNodeError,
    NoMoreConnections,
    RecvError,
    Base64DecodeError,
    InvalidUTF8,
    ParseAddressError,
    ParseIntError,
    ValueError,
    IoError,
    PasswordHashError,
    InvalidRustClientArgs,
)
