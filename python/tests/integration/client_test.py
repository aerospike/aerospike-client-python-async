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
