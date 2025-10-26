"""
Aerospike Async Exceptions

This module contains all Aerospike-specific exceptions organized in a submodule.
All exceptions are also available at the top level of aerospike_async for backward compatibility.
"""

# Import all exceptions from the parent module
from .. import (
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

__all__ = [
    "AerospikeError",
    "ServerError", 
    "UDFBadResponse",
    "TimeoutError",
    "BadResponse",
    "ConnectionError",
    "InvalidNodeError",
    "NoMoreConnections",
    "RecvError",
    "Base64DecodeError",
    "InvalidUTF8",
    "ParseAddressError",
    "ParseIntError",
    "ValueError",
    "IoError",
    "PasswordHashError",
    "InvalidRustClientArgs",
]
