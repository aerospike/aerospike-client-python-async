import pytest
from aerospike_async.exceptions import (
    AerospikeError,
    ServerError,
    RecvError,
    BadResponse,
    InvalidRustClientArgs,
    InvalidNodeError,
    NoMoreConnections,
    UDFBadResponse,
    TimeoutError,
    Base64DecodeError,
    InvalidUTF8,
    IoError,
    ParseAddressError,
    ParseIntError,
    ConnectionError,
    ValueError
)


class TestException:
    """Test exception types and inheritance."""

    def test_aerospike_error(self):
        """Test that AerospikeError is a proper exception."""
        assert issubclass(AerospikeError, Exception)

    def test_supertype(self):
        """Test that all specific exceptions inherit from AerospikeError."""
        exceptions = [
            ServerError,
            RecvError,
            BadResponse,
            InvalidRustClientArgs,
            InvalidNodeError,
            NoMoreConnections,
            UDFBadResponse,
            TimeoutError,
            Base64DecodeError,
            InvalidUTF8,
            IoError,
            ParseAddressError,
            ParseIntError,
            ConnectionError,
            ValueError
        ]
        
        for exception in exceptions:
            assert issubclass(exception, AerospikeError)
