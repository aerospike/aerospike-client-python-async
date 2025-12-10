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
    ValueError,
    ClientError
)


class TestException:
    """Test exception types and inheritance."""

    def test_aerospike_error(self):
        """Test that AerospikeError is a proper exception."""
        assert issubclass(AerospikeError, Exception)

    def test_supertype(self):
        """Test that all specific exceptions inherit from AerospikeError."""
        exceptions = [
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
            ValueError,
            ClientError
        ]
        
        for exception in exceptions:
            assert issubclass(exception, AerospikeError)
        
        # ServerError is a special case - it extends PyException directly in Rust
        # but is still an Aerospike-related exception
        assert issubclass(ServerError, Exception)
        # Note: ServerError does not extend AerospikeError due to Rust implementation constraints
