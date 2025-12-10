"""
Test ClientError exception mapping from Rust client.

ClientError is raised for various client-side validation errors in the Rust client,
such as:
- Batch operation validation errors (when batch operations are available)
- Partition tracker errors
- Invalid operation combinations
- Client-side validation failures

Note: Currently, batch operations are not available in the Python async client,
so we can't easily trigger ClientError through normal operations. This test
verifies that the exception exists and can be caught properly.
"""

import pytest
from aerospike_async.exceptions import ClientError, AerospikeError


class TestClientError:
    """Test ClientError exception type and behavior."""

    def test_client_error_exists(self):
        """Test that ClientError exception class exists."""
        assert ClientError is not None
        assert issubclass(ClientError, AerospikeError)
        assert issubclass(ClientError, Exception)

    def test_client_error_instantiation(self):
        """Test that ClientError can be instantiated with a message."""
        error_msg = "Test client error message"
        error = ClientError(error_msg)
        assert str(error) == error_msg
        assert isinstance(error, ClientError)
        assert isinstance(error, AerospikeError)
        assert isinstance(error, Exception)

    def test_client_error_can_be_caught(self):
        """Test that ClientError can be caught specifically."""
        error_msg = "Client-side validation error"
        error = ClientError(error_msg)
        
        # Can catch as ClientError
        try:
            raise error
        except ClientError as e:
            assert str(e) == error_msg
            assert isinstance(e, ClientError)
        
        # Can catch as AerospikeError
        try:
            raise error
        except AerospikeError as e:
            assert str(e) == error_msg
            assert isinstance(e, ClientError)
        
        # Can catch as Exception
        try:
            raise error
        except Exception as e:
            assert str(e) == error_msg
            assert isinstance(e, ClientError)

    def test_client_error_inheritance_chain(self):
        """Test that ClientError has correct inheritance chain."""
        error = ClientError("test")
        
        # Verify inheritance chain
        assert isinstance(error, ClientError)
        assert isinstance(error, AerospikeError)
        assert isinstance(error, Exception)
        assert isinstance(error, BaseException)
        
        # Verify MRO (Method Resolution Order)
        mro = ClientError.__mro__
        assert ClientError in mro
        assert AerospikeError in mro
        assert Exception in mro
        assert BaseException in mro

    def test_client_error_message_preservation(self):
        """Test that ClientError preserves the error message."""
        test_messages = [
            "Simple error message",
            "Error with details: operation failed",
            "Write operations not allowed in batch read",
            "Can't pass both bin names and operations to BatchReads",
            "No nodes were assigned",
        ]
        
        for msg in test_messages:
            error = ClientError(msg)
            assert str(error) == msg
            assert error.args == (msg,)

    @pytest.mark.asyncio
    async def test_client_error_in_exception_handling(self):
        """Test ClientError in async exception handling context."""
        # Simulate a scenario where ClientError might be raised
        # (In real scenarios, this would come from Rust client operations)
        
        async def operation_that_might_raise_client_error():
            # This is a placeholder - in real scenarios, ClientError would come from:
            # - Invalid batch operation combinations (when batch ops are available)
            # - Partition tracker errors in query/scan operations
            # - Invalid operation parameters
            raise ClientError("Simulated client-side validation error")
        
        # Test that we can catch it in async context
        with pytest.raises(ClientError) as exc_info:
            await operation_that_might_raise_client_error()
        
        assert "Simulated client-side validation error" in str(exc_info.value)
        assert isinstance(exc_info.value, ClientError)
        assert isinstance(exc_info.value, AerospikeError)

    def test_client_error_vs_other_errors(self):
        """Test that ClientError is distinct from other error types."""
        from aerospike_async.exceptions import (
            ServerError, ConnectionError, ValueError, TimeoutError
        )
        
        client_error = ClientError("client error")
        server_error = ServerError("server error", 4)  # ParameterError
        connection_error = ConnectionError("connection error")
        value_error = ValueError("value error")
        timeout_error = TimeoutError("timeout error")
        
        # All errors are exceptions
        assert isinstance(client_error, Exception)
        assert isinstance(server_error, Exception)
        assert isinstance(connection_error, Exception)
        assert isinstance(value_error, Exception)
        assert isinstance(timeout_error, Exception)
        
        # Most errors are AerospikeError subclasses (ServerError is a special case)
        assert isinstance(client_error, AerospikeError)
        # ServerError extends PyException directly, not AerospikeError
        assert not isinstance(server_error, AerospikeError)
        assert isinstance(connection_error, AerospikeError)
        assert isinstance(value_error, AerospikeError)
        assert isinstance(timeout_error, AerospikeError)
        
        # But they are different types
        assert type(client_error) == ClientError
        assert type(server_error) == ServerError
        assert type(connection_error) == ConnectionError
        assert type(value_error) == ValueError
        assert type(timeout_error) == TimeoutError
        
        # ClientError should not be caught by other exception handlers
        try:
            raise client_error
        except ServerError:
            pytest.fail("ClientError should not be caught as ServerError")
        except ConnectionError:
            pytest.fail("ClientError should not be caught as ConnectionError")
        except ClientError:
            pass  # Expected
        except Exception:
            pytest.fail("ClientError should be caught as ClientError, not generic Exception")

