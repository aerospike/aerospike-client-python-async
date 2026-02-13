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
    ClientError,
    ResultCode,
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


class TestServerError:
    """Test ServerError construction and properties."""

    def test_result_code(self):
        """Test that result_code is accessible."""
        err = ServerError("fail", ResultCode.GENERATION_ERROR)
        assert err.result_code == ResultCode.GENERATION_ERROR

    def test_in_doubt_default(self):
        """Test that in_doubt defaults to False."""
        err = ServerError("fail", ResultCode.GENERATION_ERROR)
        assert err.in_doubt is False

    def test_in_doubt_true(self):
        """Test that in_doubt can be set to True."""
        err = ServerError("fail", ResultCode.SERVER_ERROR, True)
        assert err.in_doubt is True

    def test_in_doubt_false_explicit(self):
        """Test explicit False for in_doubt."""
        err = ServerError("fail", ResultCode.SERVER_ERROR, False)
        assert err.in_doubt is False

    def test_message(self):
        """Test that the message is preserved."""
        err = ServerError("something broke", ResultCode.SERVER_ERROR)
        assert "something broke" in str(err)


class TestResultCode:
    """Test ResultCode equality and hashability."""

    def test_equality(self):
        """Test that identical result codes are equal."""
        assert ResultCode.OK == ResultCode.OK
        assert ResultCode.GENERATION_ERROR == ResultCode.GENERATION_ERROR

    def test_inequality(self):
        """Test that different result codes are not equal."""
        assert ResultCode.OK != ResultCode.GENERATION_ERROR
        assert ResultCode.TIMEOUT != ResultCode.KEY_NOT_FOUND_ERROR

    def test_hash(self):
        """Test that ResultCode values are hashable."""
        h = hash(ResultCode.OK)
        assert isinstance(h, int)

    def test_hash_consistency(self):
        """Test that equal values produce equal hashes."""
        assert hash(ResultCode.OK) == hash(ResultCode.OK)
        assert hash(ResultCode.GENERATION_ERROR) == hash(ResultCode.GENERATION_ERROR)

    def test_usable_in_set(self):
        """Test that ResultCode values can be used in sets."""
        s = {ResultCode.OK, ResultCode.GENERATION_ERROR, ResultCode.TIMEOUT}
        assert ResultCode.OK in s
        assert ResultCode.KEY_NOT_FOUND_ERROR not in s
        assert len(s) == 3

    def test_usable_as_dict_key(self):
        """Test that ResultCode values can be used as dict keys."""
        d = {ResultCode.OK: "success", ResultCode.TIMEOUT: "timed out"}
        assert d[ResultCode.OK] == "success"
        assert d[ResultCode.TIMEOUT] == "timed out"
