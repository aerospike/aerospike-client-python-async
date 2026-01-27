"""TLS and PKI authentication examples for Aerospike async client."""

import asyncio
import os
from aerospike_async import new_client, ClientPolicy, TlsConfig, AuthMode, WritePolicy, Key


async def example_basic_tls():
    """Example: Basic TLS connection with CA certificate."""
    print("\n=== Basic TLS Connection ===")

    # Configure TLS with CA certificate
    policy = ClientPolicy()
    policy.use_services_alternate = True  # Required for connection
    policy.tls_config = TlsConfig("path/to/ca-certificate.pem")

    # Connect to TLS-enabled server
    try:
        client = await new_client(policy, "tls-host:4333")
        print("✓ Connected with TLS")
        await client.close()
    except Exception as e:
        print(f"✗ Connection failed: {e}")


async def example_tls_with_client_auth():
    """Example: TLS with client certificate authentication."""
    print("\n=== TLS with Client Certificate ===")

    # Configure TLS with client certificate
    policy = ClientPolicy()
    policy.use_services_alternate = True  # Required for connection
    policy.tls_config = TlsConfig.with_client_auth(
        "ca.pem",      # CA certificate
        "client.pem",  # Client certificate
        "client.key"   # Client private key
    )

    try:
        client = await new_client(policy, "tls-host:4333")
        print("✓ Connected with TLS and client certificate")
        await client.close()
    except Exception as e:
        print(f"✗ Connection failed: {e}")


async def example_pki_authentication():
    """Example: PKI (certificate-based) authentication."""
    print("\n=== PKI Authentication ===")

    # Configure TLS with client certificate
    policy = ClientPolicy()
    policy.use_services_alternate = True  # Required for connection
    policy.tls_config = TlsConfig.with_client_auth(
        "ca.pem",
        "client.pem",
        "client.key"
    )

    # Set PKI authentication mode (no username/password needed)
    policy.set_pki_auth()

    print(f"Auth mode: {policy.auth_mode}")

    try:
        client = await new_client(policy, "tls-host:4333")
        print("✓ Connected with PKI authentication")

        # Perform operations
        wp = WritePolicy()
        key = Key("test", "test", "pki-test")
        await client.put(wp, key, {"message": "PKI authenticated"})
        print("✓ Write operation successful")

        await client.close()
    except Exception as e:
        print(f"✗ Connection failed: {e}")


async def example_tls_name_in_host():
    """Example: Using TLS name in host string for certificate validation."""
    print("\n=== TLS Name in Host String ===")

    policy = ClientPolicy()
    policy.use_services_alternate = True  # Required for connection
    policy.tls_config = TlsConfig("ca.pem")

    # When certificate name differs from connection hostname
    # Format: hostname:tls_name:port
    # Example: Connect to IP 192.168.1.100, but validate certificate against "server.example.com"
    tls_host = "192.168.1.100:server.example.com:4333"

    try:
        client = await new_client(policy, tls_host)
        print(f"✓ Connected to {tls_host} with TLS name override")
        await client.close()
    except Exception as e:
        print(f"✗ Connection failed: {e}")


async def example_auth_modes():
    """Example: Different authentication modes."""
    print("\n=== Authentication Modes ===")

    # Internal authentication
    policy_internal = ClientPolicy()
    policy_internal.use_services_alternate = True  # Required for connection
    policy_internal.set_auth_mode(AuthMode.INTERNAL, user="admin", password="secret")
    print(f"Internal auth - Mode: {policy_internal.auth_mode}, User: {policy_internal.user}")

    # External authentication
    policy_external = ClientPolicy()
    policy_external.use_services_alternate = True  # Required for connection
    policy_external.set_auth_mode(AuthMode.EXTERNAL, user="user", password="pass")
    print(f"External auth - Mode: {policy_external.auth_mode}, User: {policy_external.user}")

    # PKI authentication
    policy_pki = ClientPolicy()
    policy_pki.use_services_alternate = True  # Required for connection
    policy_pki.set_pki_auth()
    print(f"PKI auth - Mode: {policy_pki.auth_mode}, User: {policy_pki.user}")

    # No authentication
    policy_none = ClientPolicy()
    policy_none.use_services_alternate = True  # Required for connection
    policy_none.set_auth_mode(AuthMode.NONE)
    print(f"No auth - Mode: {policy_none.auth_mode}")


async def main():
    """Run all TLS examples."""
    print("TLS and PKI Authentication Examples")
    print("=" * 50)

    # Note: These examples require a TLS-enabled Aerospike server
    # Set environment variables:
    #   AEROSPIKE_TLS_HOST=tls-host:4333
    #   AEROSPIKE_TLS_CA_FILE=path/to/ca.pem
    #   AEROSPIKE_TLS_CLIENT_CERT_FILE=path/to/client.pem
    #   AEROSPIKE_TLS_CLIENT_KEY_FILE=path/to/client.key

    tls_host = os.environ.get("AEROSPIKE_TLS_HOST")
    tls_ca_file = os.environ.get("AEROSPIKE_TLS_CA_FILE")

    if not tls_host or not tls_ca_file:
        print("\n⚠️  TLS examples require environment variables:")
        print("   AEROSPIKE_TLS_HOST=tls-host:4333")
        print("   AEROSPIKE_TLS_CA_FILE=path/to/ca.pem")
        print("\nRunning authentication mode examples only...\n")
        await example_auth_modes()
        return

    # Run examples that require TLS server
    await example_basic_tls()
    await example_tls_with_client_auth()
    await example_pki_authentication()
    await example_tls_name_in_host()
    await example_auth_modes()


if __name__ == "__main__":
    asyncio.run(main())
