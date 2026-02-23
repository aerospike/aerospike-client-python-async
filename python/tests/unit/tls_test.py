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

"""Unit tests for TLS config and auth mode types."""

import pytest
from aerospike_async import ClientPolicy, TlsConfig, AuthMode


class TestTlsConfig:
    """Test TlsConfig creation and configuration."""

    def test_tls_config_new_with_nonexistent_file(self):
        """Test TlsConfig creation fails with nonexistent CA file."""
        with pytest.raises(Exception):
            TlsConfig("/nonexistent/ca.pem")

    def test_tls_config_with_client_auth_nonexistent_files(self):
        """Test TlsConfig.with_client_auth fails with nonexistent files."""
        with pytest.raises(Exception):
            TlsConfig.with_client_auth(
                "/nonexistent/ca.pem",
                "/nonexistent/cert.pem",
                "/nonexistent/key.pem"
            )


class TestAuthMode:
    """Test AuthMode enum functionality."""

    def test_auth_mode_values(self):
        """Test AuthMode enum values."""
        assert AuthMode.NONE is not None
        assert AuthMode.INTERNAL is not None
        assert AuthMode.EXTERNAL is not None
        assert AuthMode.PKI is not None

    def test_auth_mode_equality(self):
        """Test AuthMode enum equality."""
        assert AuthMode.NONE == AuthMode.NONE
        assert AuthMode.INTERNAL == AuthMode.INTERNAL
        assert AuthMode.EXTERNAL == AuthMode.EXTERNAL
        assert AuthMode.PKI == AuthMode.PKI
        assert AuthMode.NONE != AuthMode.INTERNAL

    def test_auth_mode_hashable(self):
        """Test AuthMode is hashable."""
        modes = {AuthMode.NONE, AuthMode.INTERNAL, AuthMode.EXTERNAL, AuthMode.PKI}
        assert len(modes) == 4


class TestClientPolicyAuth:
    """Test ClientPolicy authentication mode functionality."""

    def test_auth_mode_default(self):
        """Test default auth mode is NONE."""
        policy = ClientPolicy()
        assert policy.auth_mode == AuthMode.NONE

    def test_set_auth_mode_internal(self):
        """Test setting auth mode to INTERNAL."""
        policy = ClientPolicy()
        policy.set_auth_mode(AuthMode.INTERNAL, user="testuser", password="testpass")
        assert policy.auth_mode == AuthMode.INTERNAL
        assert policy.user == "testuser"
        assert policy.password == "testpass"

    def test_set_auth_mode_external(self):
        """Test setting auth mode to EXTERNAL."""
        policy = ClientPolicy()
        policy.set_auth_mode(AuthMode.EXTERNAL, user="testuser", password="testpass")
        assert policy.auth_mode == AuthMode.EXTERNAL
        assert policy.user == "testuser"
        assert policy.password == "testpass"

    def test_set_auth_mode_pki(self):
        """Test setting auth mode to PKI."""
        policy = ClientPolicy()
        policy.set_auth_mode(AuthMode.PKI)
        assert policy.auth_mode == AuthMode.PKI
        assert policy.user is None
        assert policy.password is None

    def test_set_pki_auth(self):
        """Test set_pki_auth convenience method."""
        policy = ClientPolicy()
        policy.set_pki_auth()
        assert policy.auth_mode == AuthMode.PKI

    def test_set_auth_mode_none(self):
        """Test setting auth mode to NONE."""
        policy = ClientPolicy()
        policy.set_auth_mode(AuthMode.INTERNAL, user="testuser", password="testpass")
        policy.set_auth_mode(AuthMode.NONE)
        assert policy.auth_mode == AuthMode.NONE
        assert policy.user is None
        assert policy.password is None

    def test_user_password_with_pki(self):
        """Test that user/password setters ignore changes in PKI mode."""
        policy = ClientPolicy()
        policy.set_pki_auth()

        policy.user = "testuser"
        policy.password = "testpass"

        assert policy.auth_mode == AuthMode.PKI
        assert policy.user is None
        assert policy.password is None
