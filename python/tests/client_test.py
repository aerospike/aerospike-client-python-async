import os

import pytest

from aerospike_async import new_client, ClientPolicy
from aerospike_async.exceptions import ConnectionError


async def test_connect():
    """Test basic client connection."""

    cp = ClientPolicy()
    client = await new_client(cp, os.environ["AEROSPIKE_HOST"])
    assert client is not None

async def test_failed_connect():
    """Test basic client connection."""
    client = None
    cp = ClientPolicy()
    with pytest.raises(ConnectionError) as exc_info:
        client = await new_client(cp, "0.0.0.0:3000")
    assert client is None
    assert exc_info.value.args[0] == "Failed to connect to host(s). The network connection(s) to cluster nodes may have timed out, or the cluster may be in a state of flux."


async def test_close():
    """Test client connection and proper closing."""

    cp = ClientPolicy()
    client = await new_client(cp, os.environ["AEROSPIKE_HOST"])
    assert client is not None
    client.close()
