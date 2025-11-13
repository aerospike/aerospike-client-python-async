import os

import pytest

from aerospike_async import new_client, ClientPolicy
from aerospike_async.exceptions import ConnectionError

async def test_connect():
    """Test basic client connection."""

    cp = ClientPolicy()
    cp.use_services_alternate = True
    client = await new_client(cp, os.environ.get("AEROSPIKE_HOST", "localhost:3000"))
    assert client is not None

async def test_failed_connect():
    """Test basic client connection failure."""
    client = None
    cp = ClientPolicy()
    cp.use_services_alternate = True
    # Set a shorter timeout for faster test execution
    cp.timeout = 2000  # 2 seconds instead of default 30 seconds
    # Use a non-existent host and port to ensure connection failure
    with pytest.raises(ConnectionError) as exc_info:
        client = await new_client(cp, "nonexistent-host:9999")
    assert client is None
    assert exc_info.value.args[0] == "Failed to connect to host(s). The network connection(s) to cluster nodes may have timed out, or the cluster may be in a state of flux."

async def test_close():
    """Test client connection and proper closing."""

    cp = ClientPolicy()
    cp.use_services_alternate = True
    client = await new_client(cp, os.environ.get("AEROSPIKE_HOST", "localhost:3000"))
    assert client is not None
    await client.close()


async def test_is_connected():
    """Test is_connected() method returns True when connected and False after closing."""
    cp = ClientPolicy()
    cp.use_services_alternate = True
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


def test_client_policy_properties():
    """Test all ClientPolicy properties can be set and retrieved."""
    cp = ClientPolicy()

    # Test setting all properties
    cp.user = "testuser"
    cp.password = "testpass"
    cp.timeout = 5000
    cp.idle_timeout = 3000
    cp.max_conns_per_node = 128
    cp.conn_pools_per_node = 2
    cp.use_services_alternate = True
    cp.rack_ids = [1, 2, 3]
    # thread_pool_size doesn't exist in TLS branch
    # cp.thread_pool_size = 64
    cp.fail_if_not_connected = False
    cp.buffer_reclaim_threshold = 32768
    cp.tend_interval = 2000
    cp.cluster_name = "test-cluster"
    ip_map = {"10.0.0.1": "192.168.1.1", "10.0.0.2": "192.168.1.2"}
    cp.ip_map = ip_map

    # Test retrieving all properties
    assert cp.user == "testuser"
    assert cp.password == "testpass"
    assert cp.timeout == 5000
    assert cp.idle_timeout == 3000
    assert cp.max_conns_per_node == 128
    assert cp.conn_pools_per_node == 2
    assert cp.use_services_alternate is True
    assert set(cp.rack_ids) == {1, 2, 3}  # HashSet doesn't preserve order
    # thread_pool_size doesn't exist in TLS branch
    # assert cp.thread_pool_size == 64
    assert cp.fail_if_not_connected is False
    assert cp.buffer_reclaim_threshold == 32768
    assert cp.tend_interval == 2000
    assert cp.cluster_name == "test-cluster"
    assert cp.ip_map == ip_map

    # Test None values for optional properties
    cp.rack_ids = None
    cp.cluster_name = None
    cp.ip_map = None

    assert cp.rack_ids is None
    assert cp.cluster_name is None
    assert cp.ip_map is None

    # Note: user/password setters have special logic - setting to None
    # when the other exists sets it to empty string, not None
    # Both need to be None to clear the user_password completely
    cp.user = None
    cp.password = None
    # After both are None, user_password should be cleared
    assert cp.user is None or cp.user == ""
    assert cp.password is None or cp.password == ""

    # Test default values
    cp2 = ClientPolicy()
    assert cp2.use_services_alternate is False
    assert cp2.rack_ids is None
    # thread_pool_size doesn't exist in TLS branch
    # assert cp2.thread_pool_size == 128
    assert cp2.fail_if_not_connected is True
    assert cp2.buffer_reclaim_threshold == 65536
    assert cp2.tend_interval == 1000
    assert cp2.cluster_name is None
    assert cp2.ip_map is None
