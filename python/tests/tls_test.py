"""Tests for TLS and PKI authentication functionality."""

import os
import pytest
from aerospike_async import new_client, ClientPolicy, TlsConfig, AuthMode
from aerospike_async.exceptions import ConnectionError
from fixtures import TestFixtureConnection


class TestTlsConfig(TestFixtureConnection):
    """Test TlsConfig creation and configuration."""

    def test_tls_config_new_with_nonexistent_file(self):
        """Test TlsConfig creation fails with nonexistent CA file."""
        with pytest.raises(Exception):  # Should raise IOError or similar
            TlsConfig("/nonexistent/ca.pem")

    def test_tls_config_with_client_auth_nonexistent_files(self):
        """Test TlsConfig.with_client_auth fails with nonexistent files."""
        with pytest.raises(Exception):  # Should raise IOError or similar
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


class TestClientPolicyAuth(TestFixtureConnection):
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
        # PKI mode should not have user/password
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

        # Setting user/password should be ignored in PKI mode
        policy.user = "testuser"
        policy.password = "testpass"

        assert policy.auth_mode == AuthMode.PKI
        assert policy.user is None
        assert policy.password is None


@pytest.mark.skipif(
    not os.environ.get("AEROSPIKE_TLS_HOST"),
    reason="AEROSPIKE_TLS_HOST not set - TLS server not available"
)
class TestTlsConnection:
    """Test TLS connection functionality (requires TLS-enabled server)."""

    @pytest.fixture
    def tls_host(self):
        """Get TLS host from environment."""
        return os.environ.get("AEROSPIKE_TLS_HOST", "localhost:4333")

    @pytest.fixture
    def tls_ca_file(self):
        """Get TLS CA file path from environment."""
        return os.environ.get("AEROSPIKE_TLS_CA_FILE")

    async def test_tls_connection_basic(self, tls_host, tls_ca_file):
        """Test basic TLS connection with CA certificate."""
        if not tls_ca_file:
            pytest.skip("AEROSPIKE_TLS_CA_FILE not set")

        policy = ClientPolicy()
        policy.tls_config = TlsConfig(tls_ca_file)

        client = await new_client(policy, tls_host)
        assert client is not None

        connected = await client.is_connected()
        assert connected is True

        await client.close()

    async def test_tls_connection_with_tls_name(self, tls_host, tls_ca_file):
        """Test TLS connection with TLS name in host string."""
        if not tls_ca_file:
            pytest.skip("AEROSPIKE_TLS_CA_FILE not set")

        policy = ClientPolicy()
        policy.tls_config = TlsConfig(tls_ca_file)

        # Parse host to extract base hostname
        host_parts = tls_host.split(":")
        base_host = host_parts[0] if host_parts else "localhost"
        port = host_parts[1] if len(host_parts) > 1 else "4333"

        # Use TLS name format: hostname:tls_name:port
        tls_host_with_name = f"{base_host}:{base_host}-tls:{port}"

        client = await new_client(policy, tls_host_with_name)
        assert client is not None

        connected = await client.is_connected()
        assert connected is True

        await client.close()


@pytest.mark.skipif(
    not os.environ.get("AEROSPIKE_TLS_HOST") or not os.environ.get("AEROSPIKE_TLS_CLIENT_CERT_FILE"),
    reason="AEROSPIKE_TLS_HOST or AEROSPIKE_TLS_CLIENT_CERT_FILE not set - PKI server not available"
)
class TestPkiConnection:
    """Test PKI authentication (requires TLS server with client certificate support)."""

    @pytest.fixture
    def tls_host(self):
        """Get TLS host from environment."""
        return os.environ.get("AEROSPIKE_TLS_HOST", "localhost:4333")

    @pytest.fixture
    def tls_ca_file(self):
        """Get TLS CA file path from environment."""
        return os.environ.get("AEROSPIKE_TLS_CA_FILE")

    @pytest.fixture
    def tls_client_cert_file(self):
        """Get TLS client certificate file path from environment."""
        return os.environ.get("AEROSPIKE_TLS_CLIENT_CERT_FILE")

    @pytest.fixture
    def tls_client_key_file(self):
        """Get TLS client key file path from environment."""
        return os.environ.get("AEROSPIKE_TLS_CLIENT_KEY_FILE")

    async def test_pki_connection(self, tls_host, tls_ca_file, tls_client_cert_file, tls_client_key_file):
        """Test PKI authentication with client certificate."""
        if not all([tls_ca_file, tls_client_cert_file, tls_client_key_file]):
            pytest.skip("TLS certificate files not configured")

        policy = ClientPolicy()
        policy.tls_config = TlsConfig.with_client_auth(
            tls_ca_file,
            tls_client_cert_file,
            tls_client_key_file
        )
        policy.set_pki_auth()

        assert policy.auth_mode == AuthMode.PKI

        client = await new_client(policy, tls_host)
        assert client is not None

        connected = await client.is_connected()
        assert connected is True

        await client.close()

    async def test_pki_connection_with_set_auth_mode(self, tls_host, tls_ca_file, tls_client_cert_file, tls_client_key_file):
        """Test PKI authentication using set_auth_mode method."""
        if not all([tls_ca_file, tls_client_cert_file, tls_client_key_file]):
            pytest.skip("TLS certificate files not configured")

        policy = ClientPolicy()
        policy.tls_config = TlsConfig.with_client_auth(
            tls_ca_file,
            tls_client_cert_file,
            tls_client_key_file
        )
        policy.set_auth_mode(AuthMode.PKI)

        assert policy.auth_mode == AuthMode.PKI

        client = await new_client(policy, tls_host)
        assert client is not None

        connected = await client.is_connected()
        assert connected is True

        await client.close()
