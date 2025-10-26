#!/usr/bin/env python3
"""
User Management Examples - Demonstrates user creation, role management, and privilege assignment.
"""

import asyncio
import os
from aerospike_async import new_client, ClientPolicy, PrivilegeCode, Privilege


async def user_management_examples():
    """Demonstrate user management operations."""
    
    print("👥 User Management Examples")
    print("="*50)
    
    # Connect to Aerospike server
    host = os.environ.get("AEROSPIKE_HOST", "localhost:3000")
    client_policy = ClientPolicy()
    
    try:
        # Create client
        client = await new_client(client_policy, host)
        print(f"✅ Connected to Aerospike server at {host}")
        
        print(f"\n--- User Creation Examples ---")
        
        # Create different types of users with different privilege sets
        users_to_create = [
            {
                "username": "admin_user",
                "password": "admin_pass_123",
                "roles": ["user-admin", "sys-admin", "data-admin"],
                "description": "Full administrative access"
            },
            {
                "username": "app_user",
                "password": "app_pass_123", 
                "roles": ["read:test", "write:test"],
                "description": "Application user with test namespace access"
            },
            {
                "username": "analyst_user",
                "password": "analyst_pass_123",
                "roles": ["read:analytics", "read:logs"],
                "description": "Analytics user with read-only access"
            },
            {
                "username": "developer_user",
                "password": "dev_pass_123",
                "roles": ["udf-admin", "read:test"],
                "description": "Developer with UDF access"
            }
        ]
        
        for user_info in users_to_create:
            try:
                # Note: These would be actual role names, not privilege objects
                # The roles parameter expects string role names
                await client.create_user(
                    user_info["username"], 
                    user_info["password"], 
                    user_info["roles"]
                )
                print(f"✅ Created user: {user_info['username']} - {user_info['description']}")
            except Exception as e:
                print(f"❌ Failed to create user {user_info['username']}: {e}")
        
        print(f"\n--- User Query Examples ---")
        
        # Query all users
        try:
            all_users = await client.query_users(None)
            print(f"All users in the system:")
            for user in all_users:
                print(f"  - {user}")
        except Exception as e:
            print(f"❌ Failed to query users: {e}")
        
        # Query specific user
        try:
            admin_user = await client.query_users("admin_user")
            print(f"Admin user details: {admin_user}")
        except Exception as e:
            print(f"❌ Failed to query admin user: {e}")
        
        print(f"\n--- Role Management Examples ---")
        
        # Query all roles
        try:
            all_roles = await client.query_roles(None)
            print(f"All roles in the system:")
            for role in all_roles:
                print(f"  - {role}")
        except Exception as e:
            print(f"❌ Failed to query roles: {e}")
        
        # Query specific role
        try:
            user_admin_role = await client.query_roles("user-admin")
            print(f"User-admin role details: {user_admin_role}")
        except Exception as e:
            print(f"❌ Failed to query user-admin role: {e}")
        
        print(f"\n--- User Role Management ---")
        
        # Grant additional roles to existing user
        try:
            await client.grant_roles("app_user", ["read:analytics"])
            print("✅ Granted analytics read access to app_user")
        except Exception as e:
            print(f"❌ Failed to grant roles: {e}")
        
        # Revoke roles from user
        try:
            await client.revoke_roles("developer_user", ["read:test"])
            print("✅ Revoked test read access from developer_user")
        except Exception as e:
            print(f"❌ Failed to revoke roles: {e}")
        
        print(f"\n--- Password Management ---")
        
        # Change user password
        try:
            await client.change_password("app_user", "new_app_pass_456")
            print("✅ Changed password for app_user")
        except Exception as e:
            print(f"❌ Failed to change password: {e}")
        
        print(f"\n--- User Cleanup ---")
        
        # Clean up test users
        test_users = ["admin_user", "app_user", "analyst_user", "developer_user"]
        for username in test_users:
            try:
                await client.drop_user(username)
                print(f"✅ Deleted user: {username}")
            except Exception as e:
                print(f"❌ Failed to delete user {username}: {e}")
        
        print(f"\n🎉 User management examples completed!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("Make sure AEROSPIKE_HOST is set and the server is running")
        print("Note: User management operations require server security to be enabled")
    
    finally:
        if 'client' in locals() and client is not None:
            client.close()
            print("✅ Client connection closed")


def show_user_management_usage():
    """Show usage examples for user management."""
    
    print("\n" + "="*60)
    print("USER MANAGEMENT USAGE")
    print("="*60)
    
    print("\n1. User Creation:")
    print("""
    # Create a user with specific roles
    await client.create_user("username", "password", ["role1", "role2"])
    
    # Common role patterns:
    admin_roles = ["user-admin", "sys-admin", "data-admin"]
    app_roles = ["read:namespace", "write:namespace"]
    analyst_roles = ["read:analytics", "read:logs"]
    """)
    
    print("\n2. User Queries:")
    print("""
    # Query all users
    users = await client.query_users(None)
    
    # Query specific user
    user = await client.query_users("username")
    """)
    
    print("\n3. Role Management:")
    print("""
    # Query all roles
    roles = await client.query_roles(None)
    
    # Query specific role
    role = await client.query_roles("role-name")
    """)
    
    print("\n4. User Role Operations:")
    print("""
    # Grant roles to user
    await client.grant_roles("username", ["new-role1", "new-role2"])
    
    # Revoke roles from user
    await client.revoke_roles("username", ["old-role1", "old-role2"])
    """)
    
    print("\n5. Password Management:")
    print("""
    # Change user password
    await client.change_password("username", "new_password")
    """)
    
    print("\n6. User Deletion:")
    print("""
    # Delete a user
    await client.drop_user("username")
    """)
    
    print("\n7. Important Notes:")
    print("""
    - User management requires server security to be enabled
    - Passwords are automatically hashed using bcrypt
    - Roles must exist before they can be assigned to users
    - Global roles (user-admin, sys-admin, etc.) are predefined
    - Namespace/set-specific roles can be created dynamically
    """)


if __name__ == "__main__":
    print("👥 User Management Examples")
    print("="*50)
    
    # Run the main example
    asyncio.run(user_management_examples())
    
    # Show usage examples
    show_user_management_usage()
