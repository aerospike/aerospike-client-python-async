#!/usr/bin/env python3
"""
Post-process the generated .pyi stub files to fix issues that pyo3_stub_gen cannot handle automatically.

This consolidated script replaces multiple separate stub processing scripts and handles:
1. Exception classes: create_exception! macro doesn't generate stubs, so we manually add them
2. Policy classes: PyClassInitializer return types don't generate complete method stubs for 
   ReadPolicy and WritePolicy, so we replace the placeholder with full method definitions
3. Import/export fixes: Ensures Key, Client, and Record are properly exported in __init__.pyi
4. Import cleanup: Fixes circular imports and uses relative imports where appropriate

All fixes are applied idempotently - safe to run multiple times.
"""

import os
import sys
import re


def fix_imports(content: str, pyi_file_path: str = "") -> str:
    """Fix import statements to use relative imports and avoid circular dependencies."""
    # Fix absolute imports from aerospike_async._aerospike_async_native to relative imports
    content = re.sub(
        r'from aerospike_async\._aerospike_async_native import ([\w, ]+)',
        r'from ._aerospike_async_native import \1',
        content
    )
    # Fix any existing circular imports
    content = re.sub(
        r'^from aerospike_async import (Key|Record|Blob|GeoJSON|HLL|List|Map|Client)\b',
        r'from ._aerospike_async_native import \1',
        content,
        flags=re.MULTILINE
    )
    
    # Remove self-imports in _aerospike_async_native.pyi - types are now all in the same module
    if '_aerospike_async_native.pyi' in pyi_file_path:
        # Remove self-imports since all types are now in the same module file
        content = re.sub(
            r'^from _aerospike_async_native import [^\n]+\n',
            '',
            content,
            flags=re.MULTILINE
        )
    
    return content


def ensure_key_client_record_exports(content: str) -> str:
    """Ensure Key, Client, and Record are properly imported in __init__.pyi."""
    # Check if we need to add or update the import
    if 'from ._aerospike_async_native import Key' in content:
        # Check if Client and Record are also imported
        import_line_match = re.search(
            r'from \._aerospike_async_native import ([^\n]+)',
            content
        )
        if import_line_match:
            imports = import_line_match.group(1)
            if 'Client' not in imports or 'Record' not in imports:
                # Update to include all three
                content = re.sub(
                    r'from \._aerospike_async_native import [^\n]+',
                    'from ._aerospike_async_native import Key, Client, Record',
                    content
                )
    else:
        # Add import for Key, Client, Record
        if 'from . import _aerospike_async_native' in content:
            content = re.sub(
                r'(from \. import _aerospike_async_native)',
                r'\1\nfrom ._aerospike_async_native import Key, Client, Record',
                content
            )
        else:
            # Add after other imports
            content = re.sub(
                r'(from enum import Enum)',
                r'\1\nfrom ._aerospike_async_native import Key, Client, Record',
                content
            )
    
    # Ensure Key, Client, and Record are explicitly re-exported at module level
    # This is needed for type checkers to recognize them when imported from aerospike_async
    # Check if they're already re-exported (after the imports section, before class definitions)
    if not re.search(r'^Key\s*:\s*type\s*=\s*_aerospike_async_native\.Key\b', content, re.MULTILINE):
        # Find the position after imports but before first class definition
        # Insert after the import statements and before the first class
        first_class_match = re.search(r'^(class\s+\w+)', content, re.MULTILINE)
        if first_class_match:
            insert_pos = first_class_match.start()
            re_exports = '\n\n# Re-export Key, Client, and Record for type checking\nKey: type = _aerospike_async_native.Key\nClient: type = _aerospike_async_native.Client\nRecord: type = _aerospike_async_native.Record\n'
            content = content[:insert_pos] + re_exports + content[insert_pos:]
        else:
            # If no classes found, add at end of imports section
            content = re.sub(
                r'(from enum import Enum\n)',
                r'\1\n# Re-export Key, Client, and Record for type checking\nKey: type = _aerospike_async_native.Key\nClient: type = _aerospike_async_native.Client\nRecord: type = _aerospike_async_native.Record\n',
                content
            )
    
    return content


def add_policy_stubs(content: str) -> str:
    """Add full method stubs for WritePolicy and ReadPolicy classes."""
    
    # ReadPolicy stub definition
    read_policy_stub = '''class ReadPolicy(BasePolicy):
    def __new__(cls) -> ReadPolicy: ...
    @property
    def replica(self) -> Replica: ...
    @replica.setter
    def replica(self, value: Replica) -> None: ...
    @property
    def filter_expression(self) -> typing.Optional[FilterExpression]: ...
    @filter_expression.setter
    def filter_expression(self, value: typing.Optional[FilterExpression]) -> None: ...
'''
    
    # WritePolicy stub definition
    write_policy_stub = '''class WritePolicy(BasePolicy):
    def __new__(cls) -> WritePolicy: ...
    @property
    def record_exists_action(self) -> RecordExistsAction: ...
    @record_exists_action.setter
    def record_exists_action(self, value: RecordExistsAction) -> None: ...
    @property
    def generation_policy(self) -> GenerationPolicy: ...
    @generation_policy.setter
    def generation_policy(self, value: GenerationPolicy) -> None: ...
    @property
    def commit_level(self) -> CommitLevel: ...
    @commit_level.setter
    def commit_level(self, value: CommitLevel) -> None: ...
    @property
    def generation(self) -> builtins.int: ...
    @generation.setter
    def generation(self, value: builtins.int) -> None: ...
    @property
    def expiration(self) -> Expiration: ...
    @expiration.setter
    def expiration(self, value: Expiration) -> None: ...
    @property
    def send_key(self) -> builtins.bool: ...
    @send_key.setter
    def send_key(self, value: builtins.bool) -> None: ...
    @property
    def respond_per_each_op(self) -> builtins.bool: ...
    @respond_per_each_op.setter
    def respond_per_each_op(self, value: builtins.bool) -> None: ...
    @property
    def durable_delete(self) -> builtins.bool: ...
    @durable_delete.setter
    def durable_delete(self, value: builtins.bool) -> None: ...
'''
    
    # Replace ReadPolicy if it's just "class ReadPolicy(BasePolicy): ..."
    read_policy_pattern = r'class ReadPolicy\(BasePolicy\):\s*\.\.\.'
    if re.search(read_policy_pattern, content):
        content = re.sub(read_policy_pattern, read_policy_stub.rstrip(), content)
        print("  ✓ Updated ReadPolicy class stubs")
    
    # Replace WritePolicy if it's just "class WritePolicy(BasePolicy): ..."
    write_policy_pattern = r'class WritePolicy\(BasePolicy\):\s*\.\.\.'
    if re.search(write_policy_pattern, content):
        content = re.sub(write_policy_pattern, write_policy_stub.rstrip(), content)
        print("  ✓ Updated WritePolicy class stubs")
    
    return content


def add_exception_stubs(content: str, pyi_file_path: str) -> tuple[str, bool]:
    """Add exception class stubs and create exceptions submodule stub."""
    
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
    
    # Check if exceptions are already added
    exceptions_added = 'class AerospikeError(' in content
    
    if not exceptions_added:
        # Add the exception stubs at the end
        content = content + exception_stubs
        print("  ✓ Added exception class stubs")
    
    # Create exceptions submodule directory and stub files
    package_dir = os.path.dirname(pyi_file_path)
    exceptions_dir = os.path.join(package_dir, 'exceptions')
    os.makedirs(exceptions_dir, exist_ok=True)
    
    # Create exceptions.pyi for the submodule
    init_stub_path = os.path.join(exceptions_dir, 'exceptions.pyi')
    init_stub_content = '''# This file contains type stubs for the aerospike_async.exceptions submodule
# Generated by postprocess_stubs.py

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
    
    if not exceptions_added:
        print(f"  ✓ Created exceptions submodule stub: {init_stub_path}")
    
    return content, exceptions_added


def postprocess_stubs(pyi_file_path: str):
    """Post-process the generated .pyi file to fix all stub issues."""
    
    print(f"Post-processing stubs: {pyi_file_path}")
    
    # Read the current file
    with open(pyi_file_path, 'r') as f:
        content = f.read()
    
    # Apply all fixes in order
    content = fix_imports(content, pyi_file_path)
    
    # Only apply these fixes to __init__.pyi, not _aerospike_async_native.pyi
    if '__init__.pyi' in pyi_file_path:
        content = ensure_key_client_record_exports(content)
        content = add_policy_stubs(content)
        content, _ = add_exception_stubs(content, pyi_file_path)
    
    # Write the updated content back
    with open(pyi_file_path, 'w') as f:
        f.write(content)
    
    print(f"✓ Completed post-processing: {pyi_file_path}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python postprocess_stubs.py <path_to_pyi_file>")
        sys.exit(1)
    
    pyi_file = sys.argv[1]
    if not os.path.exists(pyi_file):
        print(f"Error: File {pyi_file} does not exist")
        sys.exit(1)
    
    postprocess_stubs(pyi_file)

