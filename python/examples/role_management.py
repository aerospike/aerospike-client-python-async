#!/usr/bin/env python3
"""
Role Management Examples - Demonstrates role creation, privilege assignment, and role configuration.
"""

import asyncio
import os
from aerospike_async import new_client, ClientPolicy, PrivilegeCode, Privilege


async def role_management_examples():
    """Demonstrate role management operations."""

    print("🔐 Role Management Examples")
    print("="*50)

    # Connect to Aerospike server (security-enabled)
    host = os.environ.get("AEROSPIKE_HOST_SEC", os.environ.get("AEROSPIKE_HOST", "localhost:3101"))
    client_policy = ClientPolicy()
    client_policy.use_services_alternate = True  # Required for connection
    # Set credentials for security-enabled server
    client_policy.user = os.environ.get("AEROSPIKE_USER", "admin")
    client_policy.password = os.environ.get("AEROSPIKE_PASSWORD", "admin")

    try:
        # Create client
        client = await new_client(client_policy, host)
        print(f"✅ Connected to Aerospike server at {host}")

        print(f"\n--- Role Creation Examples ---")

        # Create custom roles with specific privilege sets
        roles_to_create = [
            {
                "role_name": "database_administrator",
                "privileges": [
                    Privilege(PrivilegeCode.UserAdmin, None, None),
                    Privilege(PrivilegeCode.SysAdmin, None, None),
                    Privilege(PrivilegeCode.DataAdmin, None, None),
                ],
                "allowlist": ["192.168.1.0/24", "10.0.0.0/8"],
                "read_quota": 0,  # No limit
                "write_quota": 0,  # No limit
                "description": "Full database administration access"
            },
            {
                "role_name": "application_user",
                "privileges": [
                    Privilege(PrivilegeCode.Read, "app_data", None),
                    Privilege(PrivilegeCode.Write, "app_data", None),
                    Privilege(PrivilegeCode.Read, "user_sessions", None),
                ],
                "allowlist": ["192.168.1.100", "192.168.1.101"],
                "read_quota": 1000,  # 1000 reads per second
                "write_quota": 500,   # 500 writes per second
                "description": "Application user with limited access"
            },
            {
                "role_name": "analytics_reader",
                "privileges": [
                    Privilege(PrivilegeCode.Read, "analytics", None),
                    Privilege(PrivilegeCode.Read, "logs", None),
                    Privilege(PrivilegeCode.Read, "metrics", None),
                ],
                "allowlist": ["192.168.1.200"],
                "read_quota": 2000,  # 2000 reads per second
                "write_quota": 0,    # No write access
                "description": "Analytics user with read-only access"
            },
            {
                "role_name": "udf_developer",
                "privileges": [
                    Privilege(PrivilegeCode.UDFAdmin, None, None),
                    Privilege(PrivilegeCode.Read, "test", None),
                    Privilege(PrivilegeCode.Write, "test", None),
                ],
                "allowlist": ["192.168.1.150"],
                "read_quota": 100,
                "write_quota": 100,
                "description": "UDF developer with test environment access"
            }
        ]

        for role_info in roles_to_create:
            try:
                await client.create_role(
                    role_info["role_name"],
                    role_info["privileges"],
                    role_info["allowlist"],
                    role_info["read_quota"],
                    role_info["write_quota"]
                )
                print(f"✅ Created role: {role_info['role_name']} - {role_info['description']}")
            except Exception as e:
                print(f"❌ Failed to create role {role_info['role_name']}: {e}")

        print(f"\n--- Role Query Examples ---")

        # Query all roles
        try:
            all_roles = await client.query_roles(None)
            print(f"All roles in the system:")
            for role in all_roles:
                print(f"  - {role}")
        except Exception as e:
            print(f"❌ Failed to query roles: {e}")

        # Query specific roles
        for role_name in ["database_administrator", "application_user", "analytics_reader"]:
            try:
                role_details = await client.query_roles(role_name)
                print(f"Role details for {role_name}: {role_details}")
            except Exception as e:
                print(f"❌ Failed to query role {role_name}: {e}")

        print(f"\n--- Privilege Management Examples ---")

        # Grant additional privileges to existing role
        try:
            new_privileges = [
                Privilege(PrivilegeCode.Read, "backup", None),
                Privilege(PrivilegeCode.Write, "backup", None),
            ]
            await client.grant_privileges("application_user", new_privileges)
            print("✅ Granted backup access privileges to application_user role")
        except Exception as e:
            print(f"❌ Failed to grant privileges: {e}")

        # Revoke privileges from role
        try:
            privileges_to_revoke = [
                Privilege(PrivilegeCode.Write, "user_sessions", None),
            ]
            await client.revoke_privileges("application_user", privileges_to_revoke)
            print("✅ Revoked user_sessions write access from application_user role")
        except Exception as e:
            print(f"❌ Failed to revoke privileges: {e}")

        print(f"\n--- Role Configuration Examples ---")

        # Update IP allowlist for role
        try:
            new_allowlist = ["192.168.1.100", "192.168.1.101", "192.168.1.102"]
            await client.set_allowlist("application_user", new_allowlist)
            print("✅ Updated IP allowlist for application_user role")
        except Exception as e:
            print(f"❌ Failed to update allowlist: {e}")

        # Update quotas for role
        try:
            await client.set_quotas("analytics_reader", 3000, 0)  # 3000 reads/sec, 0 writes/sec
            print("✅ Updated quotas for analytics_reader role")
        except Exception as e:
            print(f"❌ Failed to update quotas: {e}")

        print(f"\n--- Role Cleanup ---")

        # Clean up test roles
        test_roles = ["database_administrator", "application_user", "analytics_reader", "udf_developer"]
        for role_name in test_roles:
            try:
                await client.drop_role(role_name)
                print(f"✅ Deleted role: {role_name}")
            except Exception as e:
                print(f"❌ Failed to delete role {role_name}: {e}")

        print(f"\n🎉 Role management examples completed!")

    except Exception as e:
        print(f"❌ Error: {e}")
        print("Make sure AEROSPIKE_HOST is set and the server is running")
        print("Note: Role management operations require server security to be enabled")

    finally:
        if 'client' in locals() and client is not None:
            await client.close()
            print("✅ Client connection closed")


def show_role_management_usage():
    """Show usage examples for role management."""

    print("\n" + "="*60)
    print("ROLE MANAGEMENT USAGE")
    print("="*60)

    print("\n1. Role Creation:")
    print("""
    from aerospike_async import Privilege, PrivilegeCode

    # Create a role with privileges
    privileges = [
        Privilege(PrivilegeCode.Read, "namespace", None),
        Privilege(PrivilegeCode.Write, "namespace", None),
    ]
    allowlist = ["192.168.1.0/24"]
    read_quota = 1000  # reads per second
    write_quota = 500  # writes per second

    await client.create_role("role_name", privileges, allowlist, read_quota, write_quota)
    """)

    print("\n2. Privilege Types:")
    print("""
    # Global privileges (no namespace/set)
    Privilege(PrivilegeCode.UserAdmin, None, None)
    Privilege(PrivilegeCode.SysAdmin, None, None)
    Privilege(PrivilegeCode.DataAdmin, None, None)
    Privilege(PrivilegeCode.UDFAdmin, None, None)
    Privilege(PrivilegeCode.SIndexAdmin, None, None)

    # Namespace-specific privileges
    Privilege(PrivilegeCode.Read, "namespace", None)
    Privilege(PrivilegeCode.Write, "namespace", None)
    Privilege(PrivilegeCode.ReadWrite, "namespace", None)
    Privilege(PrivilegeCode.Truncate, "namespace", None)

    # Set-specific privileges
    Privilege(PrivilegeCode.Read, "namespace", "set_name")
    Privilege(PrivilegeCode.Write, "namespace", "set_name")
    """)

    print("\n3. Role Queries:")
    print("""
    # Query all roles
    roles = await client.query_roles(None)

    # Query specific role
    role = await client.query_roles("role_name")
    """)

    print("\n4. Privilege Management:")
    print("""
    # Grant privileges to role
    new_privileges = [Privilege(PrivilegeCode.Read, "new_namespace", None)]
    await client.grant_privileges("role_name", new_privileges)

    # Revoke privileges from role
    privileges_to_revoke = [Privilege(PrivilegeCode.Write, "namespace", None)]
    await client.revoke_privileges("role_name", privileges_to_revoke)
    """)

    print("\n5. Role Configuration:")
    print("""
    # Update IP allowlist
    allowlist = ["192.168.1.0/24", "10.0.0.0/8"]
    await client.set_allowlist("role_name", allowlist)

    # Update quotas (0 = no limit)
    await client.set_quotas("role_name", read_quota, write_quota)
    """)

    print("\n6. Role Deletion:")
    print("""
    # Delete a role
    await client.drop_role("role_name")
    """)

    print("\n7. Common Role Patterns:")
    print("""
    # Database Administrator
    admin_privileges = [
        Privilege(PrivilegeCode.UserAdmin, None, None),
        Privilege(PrivilegeCode.SysAdmin, None, None),
        Privilege(PrivilegeCode.DataAdmin, None, None),
    ]

    # Application User
    app_privileges = [
        Privilege(PrivilegeCode.Read, "app_data", None),
        Privilege(PrivilegeCode.Write, "app_data", None),
    ]

    # Analytics User
    analytics_privileges = [
        Privilege(PrivilegeCode.Read, "analytics", None),
        Privilege(PrivilegeCode.Read, "logs", None),
    ]

    # UDF Developer
    udf_privileges = [
        Privilege(PrivilegeCode.UDFAdmin, None, None),
        Privilege(PrivilegeCode.Read, "test", None),
        Privilege(PrivilegeCode.Write, "test", None),
    ]
    """)


if __name__ == "__main__":
    print("🔐 Role Management Examples")
    print("="*50)

    # Run the main example
    asyncio.run(role_management_examples())

    # Show usage examples
    show_role_management_usage()
