#!/usr/bin/env python3
"""
Post-process the generated .pyi file to add exception class stubs.
This is needed because create_exception! macros don't generate stubs automatically.
"""

import os
import sys

def add_exception_stubs(pyi_file_path):
    """Add exception class stubs to the generated .pyi file and create exceptions submodule stub."""
    
    exception_stubs = '''
# Exception classes (added by post-processing script)
class AerospikeError(builtins.Exception):
    """Base exception class for all Aerospike-specific errors."""
    def __init__(self, message: builtins.str) -> None: ...

class ServerError(AerospikeError):
    """Exception raised when the Aerospike server returns an error."""
    def __init__(self, message: builtins.str) -> None: ...

class UDFBadResponse(AerospikeError):
    """Exception raised when a UDF (User Defined Function) returns a bad response."""
    def __init__(self, message: builtins.str) -> None: ...

class TimeoutError(AerospikeError):
    """Exception raised when an operation times out."""
    def __init__(self, message: builtins.str) -> None: ...

class Base64DecodeError(AerospikeError):
    """Exception raised when Base64 decoding fails."""
    def __init__(self, message: builtins.str) -> None: ...

class InvalidUTF8(AerospikeError):
    """Exception raised when invalid UTF-8 is encountered."""
    def __init__(self, message: builtins.str) -> None: ...

class IoError(AerospikeError):
    """Exception raised for I/O related errors."""
    def __init__(self, message: builtins.str) -> None: ...

class ParseAddressError(AerospikeError):
    """Exception raised when parsing an address fails."""
    def __init__(self, message: builtins.str) -> None: ...

class ParseIntError(AerospikeError):
    """Exception raised when parsing an integer fails."""
    def __init__(self, message: builtins.str) -> None: ...

class ConnectionError(AerospikeError):
    """Exception raised when a connection error occurs."""
    def __init__(self, message: builtins.str) -> None: ...

class ValueError(AerospikeError):
    """Exception raised when an invalid value is provided."""
    def __init__(self, message: builtins.str) -> None: ...

class RecvError(AerospikeError):
    """Exception raised when receiving data fails."""
    def __init__(self, message: builtins.str) -> None: ...

class PasswordHashError(AerospikeError):
    """Exception raised when password hashing fails."""
    def __init__(self, message: builtins.str) -> None: ...

class BadResponse(AerospikeError):
    """Exception raised when a bad response is received."""
    def __init__(self, message: builtins.str) -> None: ...

class InvalidRustClientArgs(AerospikeError):
    """Exception raised when invalid arguments are provided to the Rust client."""
    def __init__(self, message: builtins.str) -> None: ...

class InvalidNodeError(AerospikeError):
    """Exception raised when an invalid node is encountered."""
    def __init__(self, message: builtins.str) -> None: ...

class NoMoreConnections(AerospikeError):
    """Exception raised when no more connections are available."""
    def __init__(self, message: builtins.str) -> None: ...
'''
    
    # Read the current file
    with open(pyi_file_path, 'r') as f:
        content = f.read()
    
    # Fix import statements - replace absolute imports from aerospike_async._aerospike_async_native 
    # with simpler imports
    import re
    content = re.sub(
        r'from aerospike_async\._aerospike_async_native import ([\w, ]+)',
        r'from ._aerospike_async_native import \1',
        content
    )
    # Fix any existing circular imports
    content = re.sub(
        r'^from aerospike_async import (Key|Record|Blob|GeoJSON|HLL|List|Map)\b',
        r'from ._aerospike_async_native import \1',
        content,
        flags=re.MULTILINE
    )
    content = re.sub(
        r'from \. import _aerospike_async_native',
        r'from . import _aerospike_async_native',
        content
    )
    
    # Write the fixed content back
    with open(pyi_file_path, 'w') as f:
        f.write(content)
    
    # Check if exceptions are already added
    if 'class AerospikeError(' in content:
        print(f"Exception stubs already present in {pyi_file_path}")
    else:
        # Add the exception stubs at the end
        with open(pyi_file_path, 'a') as f:
            f.write(exception_stubs)
        print(f"Added exception stubs to {pyi_file_path}")
    
    # Create exceptions submodule directory and stub files
    # Go up one level from the main module directory to find the package directory
    package_dir = os.path.dirname(pyi_file_path)
    exceptions_dir = os.path.join(package_dir, 'exceptions')
    os.makedirs(exceptions_dir, exist_ok=True)
    
    # Create exceptions.pyi for the submodule with the actual exception definitions
    init_stub_path = os.path.join(exceptions_dir, 'exceptions.pyi')
    init_stub_content = '''# This file contains type stubs for the aerospike_async.exceptions submodule
# Generated by add_exception_stubs.py

# Exception classes
class AerospikeError(builtins.Exception):
    """Base exception class for all Aerospike-specific errors."""
    def __init__(self, message: builtins.str) -> None: ...

class ServerError(AerospikeError):
    """Exception raised when the Aerospike server returns an error."""
    def __init__(self, message: builtins.str) -> None: ...

class UDFBadResponse(AerospikeError):
    """Exception raised when a UDF (User Defined Function) returns a bad response."""
    def __init__(self, message: builtins.str) -> None: ...

class TimeoutError(AerospikeError):
    """Exception raised when an operation times out."""
    def __init__(self, message: builtins.str) -> None: ...

class Base64DecodeError(AerospikeError):
    """Exception raised when Base64 decoding fails."""
    def __init__(self, message: builtins.str) -> None: ...

class InvalidUTF8(AerospikeError):
    """Exception raised when invalid UTF-8 is encountered."""
    def __init__(self, message: builtins.str) -> None: ...

class IoError(AerospikeError):
    """Exception raised for I/O related errors."""
    def __init__(self, message: builtins.str) -> None: ...

class ParseAddressError(AerospikeError):
    """Exception raised when parsing an address fails."""
    def __init__(self, message: builtins.str) -> None: ...

class ParseIntError(AerospikeError):
    """Exception raised when parsing an integer fails."""
    def __init__(self, message: builtins.str) -> None: ...

class ConnectionError(AerospikeError):
    """Exception raised when a connection error occurs."""
    def __init__(self, message: builtins.str) -> None: ...

class ValueError(AerospikeError):
    """Exception raised when an invalid value is provided."""
    def __init__(self, message: builtins.str) -> None: ...

class RecvError(AerospikeError):
    """Exception raised when receiving data fails."""
    def __init__(self, message: builtins.str) -> None: ...

class PasswordHashError(AerospikeError):
    """Exception raised when password hashing fails."""
    def __init__(self, message: builtins.str) -> None: ...

class BadResponse(AerospikeError):
    """Exception raised when a bad response is received."""
    def __init__(self, message: builtins.str) -> None: ...

class InvalidRustClientArgs(AerospikeError):
    """Exception raised when invalid arguments are provided to the Rust client."""
    def __init__(self, message: builtins.str) -> None: ...

class InvalidNodeError(AerospikeError):
    """Exception raised when an invalid node is encountered."""
    def __init__(self, message: builtins.str) -> None: ...

class NoMoreConnections(AerospikeError):
    """Exception raised when no more connections are available."""
    def __init__(self, message: builtins.str) -> None: ...

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
'''
    
    with open(init_stub_path, 'w') as f:
        f.write(init_stub_content)
    
    print(f"Created exceptions submodule stub: {init_stub_path}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python add_exception_stubs.py <path_to_pyi_file>")
        sys.exit(1)

    pyi_file = sys.argv[1]
    if not os.path.exists(pyi_file):
        print(f"Error: File {pyi_file} does not exist")
        sys.exit(1)

    add_exception_stubs(pyi_file)
