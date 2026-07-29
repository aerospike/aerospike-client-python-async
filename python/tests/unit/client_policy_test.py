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

from aerospike_async import Client, ClientPolicy


def test_client_policy_properties():
    """Test all ClientPolicy properties can be set and retrieved."""
    cp = ClientPolicy()

    # Test setting all properties
    cp.user = "testuser"
    cp.password = "testpass"
    cp.timeout = 5000
    cp.connect_timeout = 1500
    cp.login_timeout = 8000
    cp.idle_timeout = 3000
    cp.max_conns_per_node = 128
    cp.conn_pools_per_node = 2
    cp.use_services_alternate = True
    cp.rack_ids = [1, 2, 3]
    cp.fail_if_not_connected = False
    cp.seed_only_cluster = True
    cp.buffer_reclaim_threshold = 32768
    cp.tend_interval = 2000
    cp.cluster_name = "test-cluster"
    ip_map = {"10.0.0.1": "192.168.1.1", "10.0.0.2": "192.168.1.2"}
    cp.ip_map = ip_map

    # Test retrieving all properties
    assert cp.user == "testuser"
    assert cp.password == "testpass"
    assert cp.timeout == 5000
    assert cp.connect_timeout == 1500
    assert cp.login_timeout == 8000
    assert cp.idle_timeout == 3000
    assert cp.max_conns_per_node == 128
    assert cp.conn_pools_per_node == 2
    assert cp.use_services_alternate is True
    assert cp.rack_ids == [1, 2, 3]  # ordered Vec preserves preference order
    assert cp.fail_if_not_connected is False
    assert cp.seed_only_cluster is True
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

    # user/password setters have special logic - setting to None
    # when the other exists sets it to empty string, not None
    cp.user = None
    cp.password = None
    assert cp.user is None or cp.user == ""
    assert cp.password is None or cp.password == ""

    # Test default values
    cp2 = ClientPolicy()
    assert cp2.use_services_alternate is False
    assert cp2.rack_ids is None
    assert cp2.fail_if_not_connected is True
    assert cp2.seed_only_cluster is False
    assert cp2.buffer_reclaim_threshold == 65536
    assert cp2.tend_interval == 1000
    assert cp2.cluster_name is None
    assert cp2.ip_map is None
    # connect_timeout defaults to 0 (falls back to timeout); login_timeout to 5s.
    assert cp2.connect_timeout == 0
    assert cp2.login_timeout == 5000


def test_version_static_methods_expose_client_and_core_versions():
    """``Client.client_version()`` reports this library's own version;
    ``Client.core_version()`` reports the embedded engine version. Both are
    static methods callable without a live cluster connection."""
    client_version = Client.client_version()
    core_version = Client.core_version()
    for version in (client_version, core_version):
        assert isinstance(version, str)
        assert version  # non-empty
        # Semantic-version-ish: at least a major.minor prefix.
        assert version[0].isdigit()
        assert "." in version


def test_default_custom_client_id_identifies_this_client():
    """The default policy stamps a client-specific user-agent id.

    This lets bare-client usage be distinguished on the wire from the bare
    Rust core; wrapper layers override it with their own id.
    """
    cp = ClientPolicy()
    assert cp.custom_client_id is not None
    assert cp.custom_client_id.startswith("python-async-")
