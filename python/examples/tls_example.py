"""TLS and PKI authentication examples for Aerospike async client."""

import asyncio
import os
from aerospike_async import new_client, ClientPolicy, TlsConfig, AuthMode, WritePolicy, Key


async def example_basic_tls(host: str, tls_name: str, ca_file: str):
    """Example: Basic TLS connection with CA certificate."""
    print("\n=== Basic TLS Connection ===")

    policy = ClientPolicy()
    policy.use_services_alternate = True
    policy.tls_config = TlsConfig(ca_file)
    policy.set_auth_mode(
        AuthMode.INTERNAL,
        user=os.environ.get("AEROSPIKE_USER", "admin"),
        password=os.environ.get("AEROSPIKE_PASSWORD", "admin")
    )

    # Format: host:tls_name:port
    parts = host.split(":")
    tls_host = f"{parts[0]}:{tls_name}:{parts[1]}"

    try:
        client = await new_client(policy, tls_host)
        print(f"✓ Connected with TLS to {tls_host}")
        await client.close()
    except Exception as e:
        print(f"✗ Connection failed: {e}")


async def example_tls_with_client_auth(host: str, tls_name: str, ca_file: str, cert_file: str, key_file: str):
    """Example: TLS with client certificate authentication."""
    print("\n=== TLS with Client Certificate ===")

    policy = ClientPolicy()
    policy.use_services_alternate = True
    policy.tls_config = TlsConfig.with_client_auth(ca_file, cert_file, key_file)
    policy.set_auth_mode(
        AuthMode.INTERNAL,
        user=os.environ.get("AEROSPIKE_USER", "admin"),
        password=os.environ.get("AEROSPIKE_PASSWORD", "admin")
    )

    parts = host.split(":")
    tls_host = f"{parts[0]}:{tls_name}:{parts[1]}"

    try:
        client = await new_client(policy, tls_host)
        print(f"✓ Connected with TLS and client certificate to {tls_host}")
        await client.close()
    except Exception as e:
        print(f"✗ Connection failed: {e}")


async def example_pki_authentication(host: str, tls_name: str, ca_file: str, cert_file: str, key_file: str):
    """Example: PKI (certificate-based) authentication."""
    print("\n=== PKI Authentication ===")

    policy = ClientPolicy()
    policy.use_services_alternate = True
    policy.tls_config = TlsConfig.with_client_auth(ca_file, cert_file, key_file)
    policy.set_pki_auth()

    print(f"Auth mode: {policy.auth_mode}")

    parts = host.split(":")
    tls_host = f"{parts[0]}:{tls_name}:{parts[1]}"

    try:
        client = await new_client(policy, tls_host)
        print(f"✓ Connected with PKI authentication to {tls_host}")

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
    print("Format: hostname:tls_name:port")
    print("Example: 192.168.1.100:server.example.com:4333")
    print("  - Connects to IP 192.168.1.100:4333")
    print("  - Validates certificate against 'server.example.com'")


async def example_auth_modes():
    """Example: Different authentication modes."""
    print("\n=== Authentication Modes ===")

    # Internal authentication
    policy_internal = ClientPolicy()
    policy_internal.use_services_alternate = True
    policy_internal.set_auth_mode(AuthMode.INTERNAL, user="admin", password="secret")
    print(f"Internal auth - Mode: {policy_internal.auth_mode}, User: {policy_internal.user}")

    # External authentication
    policy_external = ClientPolicy()
    policy_external.use_services_alternate = True
    policy_external.set_auth_mode(AuthMode.EXTERNAL, user="user", password="pass")
    print(f"External auth - Mode: {policy_external.auth_mode}, User: {policy_external.user}")

    # PKI authentication
    policy_pki = ClientPolicy()
    policy_pki.use_services_alternate = True
    policy_pki.set_pki_auth()
    print(f"PKI auth - Mode: {policy_pki.auth_mode}, User: {policy_pki.user}")

    # No authentication
    policy_none = ClientPolicy()
    policy_none.use_services_alternate = True
    policy_none.set_auth_mode(AuthMode.NONE)
    print(f"No auth - Mode: {policy_none.auth_mode}")


async def main():
    """Run all TLS examples."""
    print("TLS and PKI Authentication Examples")
    print("=" * 50)

    # Environment variables from aerospike.env
    tls_host = os.environ.get("AEROSPIKE_HOST_TLS")
    tls_name = os.environ.get("AEROSPIKE_TLS_NAME")
    ca_file = os.environ.get("AEROSPIKE_TLS_CA_FILE")
    cert_file = os.environ.get("AEROSPIKE_TLS_CLIENT_CERT_FILE")
    key_file = os.environ.get("AEROSPIKE_TLS_CLIENT_KEY_FILE")

    if not tls_host or not tls_name or not ca_file:
        print("\nTLS examples require environment variables (see aerospike.env):")
        print("  AEROSPIKE_HOST_TLS - TLS server host:port")
        print("  AEROSPIKE_TLS_NAME - TLS name for certificate validation")
        print("  AEROSPIKE_TLS_CA_FILE - CA certificate path")
        print("  AEROSPIKE_TLS_CLIENT_CERT_FILE - Client certificate path (optional)")
        print("  AEROSPIKE_TLS_CLIENT_KEY_FILE - Client key path (optional)")
        print("\nRunning authentication mode examples only...\n")
        await example_auth_modes()
        return

    print(f"\nUsing TLS host: {tls_host}")
    print(f"Using TLS name: {tls_name}")
    print(f"Using CA file: {ca_file}")

    await example_basic_tls(tls_host, tls_name, ca_file)

    if cert_file and key_file:
        await example_tls_with_client_auth(tls_host, tls_name, ca_file, cert_file, key_file)
        await example_pki_authentication(tls_host, tls_name, ca_file, cert_file, key_file)
    else:
        print("\nSkipping client cert examples (AEROSPIKE_TLS_CLIENT_CERT_FILE/KEY_FILE not set)")

    await example_tls_name_in_host()
    await example_auth_modes()


if __name__ == "__main__":
    asyncio.run(main())
