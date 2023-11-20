import unittest

from aerospike_async import (
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

class TestException(unittest.TestCase):
    def test_aerospike_error(self):
        self.assertTrue(issubclass(AerospikeError, Exception))

    def test_supertype(self):
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
            self.assertTrue(issubclass(exception, AerospikeError))
