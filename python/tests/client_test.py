import os

import pytest

from aerospike_async import new_client, ClientPolicy
from aerospike_async.exceptions import ConnectionError

async def test_connect():
    """Test basic client connection."""

    cp = ClientPolicy()
    client = await new_client(cp, os.environ.get("AEROSPIKE_HOST", "localhost:3000"))
    assert client is not None

async def test_failed_connect():
    """Test basic client connection failure."""
    client = None
    cp = ClientPolicy()
    # Use a non-existent host and port to ensure connection failure
    with pytest.raises(ConnectionError) as exc_info:
        client = await new_client(cp, "nonexistent-host:9999")
    assert client is None
    assert exc_info.value.args[0] == "Failed to connect to host(s). The network connection(s) to cluster nodes may have timed out, or the cluster may be in a state of flux."

async def test_close():
    """Test client connection and proper closing."""

    cp = ClientPolicy()
    client = await new_client(cp, os.environ.get("AEROSPIKE_HOST", "localhost:3000"))
    assert client is not None
    await client.close()


async def test_is_connected():
    """Test is_connected() method returns True when connected and False after closing."""
    cp = ClientPolicy()
    client = await new_client(cp, os.environ.get("AEROSPIKE_HOST", "localhost:3000"))
    assert client is not None

    # After successful connection, should be connected
    connected = await client.is_connected()
    assert connected is True, "Client should be connected after successful new_client()"

    # Close the client (now async)
    await client.close()

    # After closing, should not be connected
    connected = await client.is_connected()
    assert connected is False, "Client should not be connected after close()"
