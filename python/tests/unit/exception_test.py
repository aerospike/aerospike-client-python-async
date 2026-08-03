# Copyright 2023-2026 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import subprocess
import sys

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
    RecordError,
    IndexError as AsyncIndexError,
    SecurityError,
    RecordNotFound,
    GenerationError,
    InvalidRequest,
    RecordExistsError,
    BinTypeError,
    RecordTooBig,
    BinNotFound,
    FilteredOut,
    OpNotApplicable,
    IndexNotFound,
    IndexFoundError,
    NotAuthenticated,
    SecurityNotEnabled,
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


class TestClientSideInDoubt:
    """Test the in_doubt attribute on AerospikeError and its subclasses."""

    def test_default_false(self):
        """in_doubt defaults to False on the base and inherited subclasses."""
        for exception in (AerospikeError, TimeoutError, ConnectionError, ClientError):
            assert exception("boom").in_doubt is False

    def test_instance_override(self):
        """A per-instance in_doubt=True reads back without touching the class default."""
        err = TimeoutError("timed out")
        err.in_doubt = True
        assert err.in_doubt is True
        assert TimeoutError("timed out").in_doubt is False

    def test_str_unaffected(self):
        """Setting in_doubt leaves str() as the plain message, not an args tuple."""
        err = ConnectionError("connection reset")
        err.in_doubt = True
        assert str(err) == "connection reset"
        assert err.args == ("connection reset",)

    def test_package_attribute_applies_class_default(self):
        """A bare package import loads the wrapper module and applies class defaults.

        Runs in a fresh interpreter: any in-process import of
        aerospike_async.exceptions rebinds the package attribute to the wrapper
        as an import side effect, which would mask a missing package-level
        import.
        """
        code = (
            "import aerospike_async; "
            "assert aerospike_async.exceptions.TimeoutError('x').in_doubt is False; "
            "assert hasattr(aerospike_async.exceptions, 'RecordNotFound')"
        )
        subprocess.run([sys.executable, "-c", code], check=True)


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


class TestServerErrorSubclasses:
    """Test ServerError subclasses (construction, isinstance, except hierarchy)."""

    def test_subclass_constructable(self):
        """Each subclass is constructable with (message, result_code, in_doubt)."""
        pairs = [
            (RecordNotFound, ResultCode.KEY_NOT_FOUND_ERROR),
            (GenerationError, ResultCode.GENERATION_ERROR),
            (InvalidRequest, ResultCode.PARAMETER_ERROR),
            (RecordExistsError, ResultCode.KEY_EXISTS_ERROR),
            (BinTypeError, ResultCode.BIN_TYPE_ERROR),
            (RecordTooBig, ResultCode.RECORD_TOO_BIG),
            (BinNotFound, ResultCode.BIN_NOT_FOUND),
            (FilteredOut, ResultCode.FILTERED_OUT),
            (OpNotApplicable, ResultCode.OP_NOT_APPLICABLE),
            (IndexNotFound, ResultCode.INDEX_NOT_FOUND),
            (IndexFoundError, ResultCode.INDEX_FOUND),
            (NotAuthenticated, ResultCode.NOT_AUTHENTICATED),
            (SecurityNotEnabled, ResultCode.SECURITY_NOT_ENABLED),
        ]
        for exc_cls, rc in pairs:
            err = exc_cls("msg", rc, False)
            assert err.result_code == rc
            assert err.in_doubt is False
            assert "msg" in str(err)

    def test_subclass_isinstance_server_error(self):
        """Subclass instances are isinstance(exc, ServerError)."""
        err = RecordNotFound("x", ResultCode.KEY_NOT_FOUND_ERROR)
        assert isinstance(err, ServerError)
        assert isinstance(InvalidRequest("x", ResultCode.PARAMETER_ERROR), ServerError)

    def test_subclass_isinstance_grouping_base(self):
        """Grouped subclasses are isinstance of their grouping base."""
        assert isinstance(RecordNotFound("x", ResultCode.KEY_NOT_FOUND_ERROR), RecordError)
        assert isinstance(BinNotFound("x", ResultCode.BIN_NOT_FOUND), RecordError)
        assert isinstance(IndexNotFound("x", ResultCode.INDEX_NOT_FOUND), AsyncIndexError)
        assert isinstance(NotAuthenticated("x", ResultCode.NOT_AUTHENTICATED), SecurityError)

    def test_subclass_result_code_in_doubt(self):
        """Subclass instances expose result_code and in_doubt."""
        err = GenerationError("g", ResultCode.GENERATION_ERROR, True)
        assert err.result_code == ResultCode.GENERATION_ERROR
        assert err.in_doubt is True

    def test_except_server_error_catches_subclasses(self):
        """except ServerError catches subclass instances."""
        try:
            raise RecordNotFound("not found", ResultCode.KEY_NOT_FOUND_ERROR)
        except ServerError:
            pass
        else:
            raise AssertionError("RecordNotFound should be caught by except ServerError")

    def test_except_record_error_catches_record_not_found_not_invalid_request(self):
        """except RecordError catches RecordNotFound but not InvalidRequest."""
        try:
            raise RecordNotFound("x", ResultCode.KEY_NOT_FOUND_ERROR)
        except RecordError:
            pass
        except InvalidRequest:
            raise AssertionError("RecordNotFound should not be InvalidRequest")

        try:
            raise InvalidRequest("x", ResultCode.PARAMETER_ERROR)
        except RecordError:
            raise AssertionError("InvalidRequest should not be caught by except RecordError")
        except InvalidRequest:
            pass


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


class TestRetryContextSurface:
    """Retry/diagnostic context on both exception surfaces.

    The class defaults live on ``AerospikeError`` (applied by the generated
    exceptions wrapper); the native layer sets instance attributes only when
    core recorded a value. ``ServerError`` carries them as real fields.
    """

    def test_client_side_class_defaults(self):
        err = TimeoutError("timed out")
        assert err.node is None
        assert err.iteration is None
        assert err.base_message is None
        assert err.sub_exceptions is None

    def test_client_side_instance_overrides(self):
        err = TimeoutError("timed out")
        err.node = "BB9020011AC4202"
        err.iteration = 3
        err.base_message = "Client Timeout: deadline exceeded"
        err.sub_exceptions = [TimeoutError("attempt 1"), TimeoutError("attempt 2")]
        assert err.node == "BB9020011AC4202"
        assert err.iteration == 3
        assert err.base_message == "Client Timeout: deadline exceeded"
        assert len(err.sub_exceptions) == 2
        assert isinstance(err.sub_exceptions[0], TimeoutError)
        assert TimeoutError.node is None

    def test_server_error_retry_context_defaults(self):
        err = ServerError("fail", ResultCode.GENERATION_ERROR)
        assert err.node is None
        assert err.iteration is None
        assert err.base_message is None
        assert err.sub_exceptions is None

    def test_server_error_full_positional_construction(self):
        subs = [TimeoutError("attempt 1")]
        err = ServerError(
            "fail", ResultCode.GENERATION_ERROR, True, 2, "generation conflict",
            None, "BB9020011AC4202", 3, "Server error: GenerationError", subs,
        )
        assert err.in_doubt is True
        assert err.sub_code == 2
        assert err.server_message == "generation conflict"
        assert err.node == "BB9020011AC4202"
        assert err.iteration == 3
        assert err.base_message == "Server error: GenerationError"
        assert len(err.sub_exceptions) == 1
        assert isinstance(err.sub_exceptions[0], TimeoutError)

    def test_server_error_trailing_defaults(self):
        # Construction is positional-only: the PyException base rejects
        # keyword arguments, so the retry-context parameters are reachable
        # as a positional suffix with defaults.
        err = ServerError(
            "fail", ResultCode.SERVER_ERROR, False, None, None, None,
            "BB9020011AC4202", 1, "Server error: ServerError",
        )
        assert err.node == "BB9020011AC4202"
        assert err.iteration == 1
        assert err.base_message == "Server error: ServerError"
        assert err.sub_exceptions is None

    def test_server_error_subclass_inherits_retry_context(self):
        err = RecordNotFound(
            "not found", ResultCode.KEY_NOT_FOUND_ERROR, False, None, None,
            None, "BB9020011AC4202", 2,
        )
        assert err.node == "BB9020011AC4202"
        assert err.iteration == 2


class TestSubsystemFamilyDispatch:
    """Result-code dispatch for the subsystem family classes."""

    def test_family_classes_subclass_server_error(self):
        from aerospike_async.exceptions import (
            BatchError, QueryError, QuotaError, UdfError,
        )
        for cls in (QueryError, BatchError, QuotaError, UdfError):
            assert issubclass(cls, ServerError)

    def test_dispatch_by_result_code(self):
        from aerospike_async.exceptions import (
            BatchError, QueryError, QuotaError, UdfError,
            _get_server_error_class,
        )
        expected = {
            ResultCode.QUERY_GENERIC: QueryError,
            ResultCode.SCAN_ABORT: QueryError,
            ResultCode.QUERY_ABORTED: QueryError,
            ResultCode.BATCH_DISABLED: BatchError,
            ResultCode.BATCH_QUEUES_FULL: BatchError,
            ResultCode.QUOTA_EXCEEDED: QuotaError,
            ResultCode.UDF_BAD_RESPONSE: UdfError,
            ResultCode.ROLE_VIOLATION: SecurityError,
            ResultCode.INVALID_PASSWORD: SecurityError,
            # Unmapped codes keep the base
            ResultCode.SERVER_ERROR: ServerError,
        }
        for code, cls in expected.items():
            assert _get_server_error_class(code) is cls, code


class TestServerErrorStr:
    """str(exc) is the message alone, not the constructor args tuple."""

    def test_str_is_message_only(self):
        err = ServerError(
            "something broke", ResultCode.SERVER_ERROR, True, 2, "detail",
            None, "BB9020011AC4202", 3, "Server error: ServerError", None,
        )
        assert str(err) == "something broke"

    def test_subclass_str_is_message_only(self):
        err = RecordNotFound("not found", ResultCode.KEY_NOT_FOUND_ERROR)
        assert str(err) == "not found"
