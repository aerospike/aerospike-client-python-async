"""Tests for TLS and PKI authentication functionality."""

import os
import pytest
from aerospike_async import new_client, ClientPolicy, TlsConfig, AuthMode
from aerospike_async.exceptions import ServerError, ResultCode


def _tls_host_env():
    """TLS host from AEROSPIKE_TLS_HOST or AEROSPIKE_HOST_TLS (aerospike.env)."""
    return os.environ.get("AEROSPIKE_TLS_HOST") or os.environ.get("AEROSPIKE_HOST_TLS")


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
    not _tls_host_env(),
    reason="AEROSPIKE_TLS_HOST or AEROSPIKE_HOST_TLS not set - TLS server not available"
)
class TestTlsConnection:
    """Test TLS connection functionality (requires TLS-enabled server)."""

    # use_services_alternate is required on ClientPolicy for TLS connections

    @pytest.fixture
    def tls_host(self):
        """Get TLS host from environment."""
        return _tls_host_env() or "localhost:4333"

    @pytest.fixture
    def tls_ca_file(self):
        """Get TLS CA file path from environment."""
        return os.environ.get("AEROSPIKE_TLS_CA_FILE")

    @pytest.fixture
    def tls_name(self):
        """Get TLS name from environment (must match server tls-name for cert validation)."""
        return os.environ.get("AEROSPIKE_TLS_NAME")

    async def test_tls_connection_basic(self, tls_host, tls_ca_file, tls_name):
        """Test basic TLS connection with CA certificate."""
        if not tls_ca_file:
            pytest.skip("AEROSPIKE_TLS_CA_FILE not set")

        policy = ClientPolicy()
        policy.use_services_alternate = True
        policy.tls_config = TlsConfig(tls_ca_file)
        policy.set_auth_mode(
            AuthMode.INTERNAL,
            user=os.environ.get("AEROSPIKE_USER", "admin"),
            password=os.environ.get("AEROSPIKE_PASSWORD", "admin")
        )

        # Use host:tls_name:port so client validates server cert against tls-name (e.g. tls1)
        if tls_name:
            parts = (tls_host or "localhost:4333").split(":")
            base_host = parts[0] if parts else "localhost"
            port = parts[1] if len(parts) > 1 else "4333"
            connect_host = f"{base_host}:{tls_name}:{port}"
        else:
            connect_host = tls_host

        client = await new_client(policy, connect_host)
        assert client is not None

        connected = await client.is_connected()
        assert connected is True

        await client.close()

    async def test_tls_connection_with_tls_name(self, tls_host, tls_ca_file, tls_name):
        """Test TLS connection with TLS name in host string (host:tls_name:port)."""
        if not tls_ca_file:
            pytest.skip("AEROSPIKE_TLS_CA_FILE not set")
        if not tls_name:
            pytest.skip("AEROSPIKE_TLS_NAME not set - required for this test")

        policy = ClientPolicy()
        policy.use_services_alternate = True
        policy.tls_config = TlsConfig(tls_ca_file)
        policy.set_auth_mode(
            AuthMode.INTERNAL,
            user=os.environ.get("AEROSPIKE_USER", "admin"),
            password=os.environ.get("AEROSPIKE_PASSWORD", "admin")
        )

        parts = (tls_host or "localhost:4333").split(":")
        base_host = parts[0] if parts else "localhost"
        port = parts[1] if len(parts) > 1 else "4333"
        connect_host = f"{base_host}:{tls_name}:{port}"

        client = await new_client(policy, connect_host)
        assert client is not None

        connected = await client.is_connected()
        assert connected is True

        await client.close()


@pytest.mark.skipif(
    not _tls_host_env() or not os.environ.get("AEROSPIKE_TLS_CLIENT_CERT_FILE"),
    reason="AEROSPIKE_TLS_HOST/AEROSPIKE_HOST_TLS or AEROSPIKE_TLS_CLIENT_CERT_FILE not set - PKI server not available"
)
class TestPkiConnection:
    """Test PKI authentication (requires TLS server with client certificate support)."""

    # use_services_alternate is required on ClientPolicy for TLS connections

    @pytest.fixture
    def tls_host(self):
        """Get TLS host from environment."""
        return _tls_host_env() or "localhost:4333"

    @pytest.fixture
    def tls_ca_file(self):
        """Get TLS CA file path from environment."""
        return os.environ.get("AEROSPIKE_TLS_CA_FILE")

    @pytest.fixture
    def tls_name(self):
        """Get TLS name from environment (must match server tls-name for cert validation)."""
        return os.environ.get("AEROSPIKE_TLS_NAME")

    @pytest.fixture
    def tls_client_cert_file(self):
        """Get TLS client certificate file path from environment."""
        return os.environ.get("AEROSPIKE_TLS_CLIENT_CERT_FILE")

    @pytest.fixture
    def tls_client_key_file(self):
        """Get TLS client key file path from environment."""
        return os.environ.get("AEROSPIKE_TLS_CLIENT_KEY_FILE")

    def _connect_host(self, tls_host, tls_name):
        """Build host:tls_name:port when tls_name set, else tls_host."""
        if tls_name:
            parts = (tls_host or "localhost:4333").split(":")
            base_host = parts[0] if parts else "localhost"
            port = parts[1] if len(parts) > 1 else "4333"
            return f"{base_host}:{tls_name}:{port}"
        return tls_host

    async def test_pki_connection(self, tls_host, tls_ca_file, tls_name, tls_client_cert_file, tls_client_key_file):
        """Test TLS connection with client certificate and admin auth (same connection model as basic TLS)."""
        if not all([tls_ca_file, tls_client_cert_file, tls_client_key_file]):
            pytest.skip("TLS certificate files not configured")

        policy = ClientPolicy()
        policy.use_services_alternate = True
        policy.tls_config = TlsConfig.with_client_auth(
            tls_ca_file,
            tls_client_cert_file,
            tls_client_key_file
        )
        policy.set_auth_mode(
            AuthMode.INTERNAL,
            user=os.environ.get("AEROSPIKE_USER", "admin"),
            password=os.environ.get("AEROSPIKE_PASSWORD", "admin")
        )

        connect_host = self._connect_host(tls_host, tls_name)
        client = await new_client(policy, connect_host)
        assert client is not None

        connected = await client.is_connected()
        assert connected is True

        await client.close()

    async def test_pki_connection_with_set_auth_mode(self, tls_host, tls_ca_file, tls_name, tls_client_cert_file, tls_client_key_file):
        """Test TLS connection with client cert and admin auth; policy can set AuthMode.PKI for other use."""
        if not all([tls_ca_file, tls_client_cert_file, tls_client_key_file]):
            pytest.skip("TLS certificate files not configured")

        policy = ClientPolicy()
        policy.use_services_alternate = True
        policy.tls_config = TlsConfig.with_client_auth(
            tls_ca_file,
            tls_client_cert_file,
            tls_client_key_file
        )
        policy.set_auth_mode(
            AuthMode.INTERNAL,
            user=os.environ.get("AEROSPIKE_USER", "admin"),
            password=os.environ.get("AEROSPIKE_PASSWORD", "admin")
        )

        connect_host = self._connect_host(tls_host, tls_name)
        client = await new_client(policy, connect_host)
        assert client is not None

        connected = await client.is_connected()
        assert connected is True

        await client.close()
