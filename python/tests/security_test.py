#!/usr/bin/env python3
"""
Security Tests - Tests for user management, role management, and authentication features.
These tests require a server with security enabled and proper authentication.
"""

import pytest
import os
from aerospike_async import new_client, ClientPolicy, PrivilegeCode, Privilege


@pytest.mark.skip(reason="Security tests disabled - server security not enabled")
class TestSecurityFeatures:
    """Test security-related features that require server authentication."""

    @pytest.fixture(scope="class")
    async def client(self):
        """Create a client for testing with security enabled."""
        host = os.environ.get("AEROSPIKE_HOST_SEC", "localhost:3000")
        client_policy = ClientPolicy()
        client_policy.user = "admin"
        client_policy.password = "admin"

        try:
            client = await new_client(client_policy, host)
            yield client
        except Exception as e:
            pytest.skip(f"Could not connect to security server at {host}: {e}")
        finally:
            if 'client' in locals() and client is not None:
                await client.close()

    @pytest.fixture(autouse=True)
    async def cleanup_users(self, client):
        """Clean up test users after each test."""
        yield
        # Clean up test users
        test_users = ["test_user_1", "test_user_2", "test_user_3", "test_admin", "test_app_user"]
        for username in test_users:
            try:
                await client.drop_user(username)
            except:
                pass  # User might not exist

    @pytest.fixture(autouse=True)
    async def cleanup_roles(self, client):
        """Clean up test roles after each test."""
        yield
        # Clean up test roles
        test_roles = ["test_role_1", "test_role_2", "test_app_role", "test_analytics_role"]
        for role_name in test_roles:
            try:
                await client.drop_role(role_name)
            except:
                pass  # Role might not exist

    @pytest.mark.asyncio
    async def test_create_user_basic(self, client):
        """Test basic user creation.

        Creates a user with basic role and verifies it exists.
        """

        username = "test_user_1"
        password = "test_password_123"
        roles = ["read:test"]

        await client.create_user(username, password, roles)
        users = await client.query_users(username)
        assert username in str(users)

    @pytest.mark.asyncio
    async def test_create_user_multiple_roles(self, client):
        """Test user creation with multiple roles.

        Creates a user with multiple roles and verifies it exists.
        """

        username = "test_user_2"
        password = "test_password_456"
        roles = ["read:test", "write:test", "read:analytics"]

        await client.create_user(username, password, roles)
        users = await client.query_users(username)
        assert username in str(users)

    @pytest.mark.asyncio
    async def test_create_user_duplicate(self, client):
        """Test creating duplicate user fails.

        Creates a user, then attempts to create the same user again.
        The second creation should raise an exception.
        """

        username = "test_user_3"
        password = "test_password_789"
        roles = ["read:test"]

        await client.create_user(username, password, roles)
        with pytest.raises(Exception):
            await client.create_user(username, password, roles)

    @pytest.mark.asyncio
    async def test_query_users_all(self, client):
        """Test querying all users.

        Creates multiple test users and verifies they can all be queried.
        """

        await client.create_user("test_user_1", "pass1", ["read:test"])
        await client.create_user("test_user_2", "pass2", ["write:test"])

        users = await client.query_users(None)
        assert "test_user_1" in str(users)
        assert "test_user_2" in str(users)

    @pytest.mark.asyncio
    async def test_query_users_specific(self, client):
        """Test querying specific user."""
        username = "test_user_1"
        password = "test_password_123"
        roles = ["read:test"]

        # Create user
        await client.create_user(username, password, roles)

        # Query specific user
        user = await client.query_users(username)
        assert username in str(user)

    @pytest.mark.asyncio
    async def test_query_users_nonexistent(self, client):
        """Test querying non-existent user."""
        with pytest.raises(Exception):
            await client.query_users("nonexistent_user")

    @pytest.mark.asyncio
    async def test_drop_user(self, client):
        """Test user deletion."""
        username = "test_user_1"
        password = "test_password_123"
        roles = ["read:test"]

        # Create user
        await client.create_user(username, password, roles)

        # Verify user exists
        users = await client.query_users(username)
        assert username in str(users)

        # Delete user
        await client.drop_user(username)

        # Verify user is deleted
        with pytest.raises(Exception):
            await client.query_users(username)

    @pytest.mark.asyncio
    async def test_drop_user_nonexistent(self, client):
        """Test deleting non-existent user."""
        with pytest.raises(Exception):
            await client.drop_user("nonexistent_user")

    @pytest.mark.asyncio
    async def test_change_password(self, client):
        """Test password change."""
        username = "test_user_1"
        password = "test_password_123"
        new_password = "new_password_456"
        roles = ["read:test"]

        # Create user
        await client.create_user(username, password, roles)

        # Change password
        await client.change_password(username, new_password)

        # Verify user still exists (password change doesn't affect user existence)
        users = await client.query_users(username)
        assert username in str(users)

    @pytest.mark.asyncio
    async def test_change_password_nonexistent(self, client):
        """Test changing password for non-existent user."""
        with pytest.raises(Exception):
            await client.change_password("nonexistent_user", "new_password")

    @pytest.mark.asyncio
    async def test_grant_roles(self, client):
        """Test granting roles to user."""
        username = "test_user_1"
        password = "test_password_123"
        initial_roles = ["read:test"]
        new_roles = ["write:test", "read:analytics"]

        # Create user with initial roles
        await client.create_user(username, password, initial_roles)

        # Grant additional roles
        await client.grant_roles(username, new_roles)

        # Verify user still exists (role grant doesn't affect user existence)
        users = await client.query_users(username)
        assert username in str(users)

    @pytest.mark.asyncio
    async def test_grant_roles_nonexistent_user(self, client):
        """Test granting roles to non-existent user."""
        with pytest.raises(Exception):
            await client.grant_roles("nonexistent_user", ["read:test"])

    @pytest.mark.asyncio
    async def test_revoke_roles(self, client):
        """Test revoking roles from user."""
        username = "test_user_1"
        password = "test_password_123"
        initial_roles = ["read:test", "write:test"]
        roles_to_revoke = ["write:test"]

        # Create user with initial roles
        await client.create_user(username, password, initial_roles)

        # Revoke roles
        await client.revoke_roles(username, roles_to_revoke)

        # Verify user still exists (role revoke doesn't affect user existence)
        users = await client.query_users(username)
        assert username in str(users)

    @pytest.mark.asyncio
    async def test_revoke_roles_nonexistent_user(self, client):
        """Test revoking roles from non-existent user."""
        with pytest.raises(Exception):
            await client.revoke_roles("nonexistent_user", ["read:test"])

    @pytest.mark.asyncio
    async def test_create_role_basic(self, client):
        """Test basic role creation."""
        role_name = "test_role_1"
        privileges = [
            Privilege(PrivilegeCode.Read, "test", None),
            Privilege(PrivilegeCode.Write, "test", None)
        ]
        allowlist = ["192.168.1.0/24"]
        read_quota = 1000
        write_quota = 500

        # Create role
        await client.create_role(role_name, privileges, allowlist, read_quota, write_quota)

        # Verify role exists
        roles = await client.query_roles(role_name)
        assert role_name in str(roles)

    @pytest.mark.asyncio
    async def test_create_role_global_privileges(self, client):
        """Test role creation with global privileges."""
        role_name = "test_role_2"
        privileges = [
            Privilege(PrivilegeCode.UserAdmin, None, None),
            Privilege(PrivilegeCode.SysAdmin, None, None)
        ]
        allowlist = ["192.168.1.0/24"]
        read_quota = 0
        write_quota = 0

        # Create role
        await client.create_role(role_name, privileges, allowlist, read_quota, write_quota)

        # Verify role exists
        roles = await client.query_roles(role_name)
        assert role_name in str(roles)

    @pytest.mark.asyncio
    async def test_create_role_duplicate(self, client):
        """Test creating duplicate role fails."""
        role_name = "test_role_1"
        privileges = [Privilege(PrivilegeCode.Read, "test", None)]
        allowlist = ["192.168.1.0/24"]
        read_quota = 1000
        write_quota = 500

        # Create role first time
        await client.create_role(role_name, privileges, allowlist, read_quota, write_quota)

        # Try to create duplicate role
        with pytest.raises(Exception):
            await client.create_role(role_name, privileges, allowlist, read_quota, write_quota)

    @pytest.mark.asyncio
    async def test_query_roles_all(self, client):
        """Test querying all roles."""
        # Create test roles
        await client.create_role("test_role_1", [Privilege(PrivilegeCode.Read, "test", None)], 
                               ["192.168.1.0/24"], 1000, 500)
        await client.create_role("test_role_2", [Privilege(PrivilegeCode.Write, "test", None)], 
                               ["192.168.1.0/24"], 1000, 500)

        # Query all roles
        roles = await client.query_roles(None)
        assert "test_role_1" in str(roles)
        assert "test_role_2" in str(roles)

    @pytest.mark.asyncio
    async def test_query_roles_specific(self, client):
        """Test querying specific role."""
        role_name = "test_role_1"
        privileges = [Privilege(PrivilegeCode.Read, "test", None)]
        allowlist = ["192.168.1.0/24"]
        read_quota = 1000
        write_quota = 500

        # Create role
        await client.create_role(role_name, privileges, allowlist, read_quota, write_quota)

        # Query specific role
        role = await client.query_roles(role_name)
        assert role_name in str(role)

    @pytest.mark.asyncio
    async def test_query_roles_nonexistent(self, client):
        """Test querying non-existent role."""
        with pytest.raises(Exception):
            await client.query_roles("nonexistent_role")

    @pytest.mark.asyncio
    async def test_drop_role(self, client):
        """Test role deletion."""
        role_name = "test_role_1"
        privileges = [Privilege(PrivilegeCode.Read, "test", None)]
        allowlist = ["192.168.1.0/24"]
        read_quota = 1000
        write_quota = 500

        # Create role
        await client.create_role(role_name, privileges, allowlist, read_quota, write_quota)

        # Verify role exists
        roles = await client.query_roles(role_name)
        assert role_name in str(roles)

        # Delete role
        await client.drop_role(role_name)

        # Verify role is deleted
        with pytest.raises(Exception):
            await client.query_roles(role_name)

    @pytest.mark.asyncio
    async def test_drop_role_nonexistent(self, client):
        """Test deleting non-existent role."""
        with pytest.raises(Exception):
            await client.drop_role("nonexistent_role")

    @pytest.mark.asyncio
    async def test_grant_privileges(self, client):
        """Test granting privileges to role."""
        role_name = "test_role_1"
        initial_privileges = [Privilege(PrivilegeCode.Read, "test", None)]
        new_privileges = [Privilege(PrivilegeCode.Write, "test", None)]
        allowlist = ["192.168.1.0/24"]
        read_quota = 1000
        write_quota = 500

        # Create role with initial privileges
        await client.create_role(role_name, initial_privileges, allowlist, read_quota, write_quota)

        # Grant additional privileges
        await client.grant_privileges(role_name, new_privileges)

        # Verify role still exists (privilege grant doesn't affect role existence)
        roles = await client.query_roles(role_name)
        assert role_name in str(roles)

    @pytest.mark.asyncio
    async def test_grant_privileges_nonexistent_role(self, client):
        """Test granting privileges to non-existent role."""
        with pytest.raises(Exception):
            await client.grant_privileges("nonexistent_role", [Privilege(PrivilegeCode.Read, "test", None)])

    @pytest.mark.asyncio
    async def test_revoke_privileges(self, client):
        """Test revoking privileges from role."""
        role_name = "test_role_1"
        initial_privileges = [
            Privilege(PrivilegeCode.Read, "test", None),
            Privilege(PrivilegeCode.Write, "test", None)
        ]
        privileges_to_revoke = [Privilege(PrivilegeCode.Write, "test", None)]
        allowlist = ["192.168.1.0/24"]
        read_quota = 1000
        write_quota = 500

        # Create role with initial privileges
        await client.create_role(role_name, initial_privileges, allowlist, read_quota, write_quota)

        # Revoke privileges
        await client.revoke_privileges(role_name, privileges_to_revoke)

        # Verify role still exists (privilege revoke doesn't affect role existence)
        roles = await client.query_roles(role_name)
        assert role_name in str(roles)

    @pytest.mark.asyncio
    async def test_revoke_privileges_nonexistent_role(self, client):
        """Test revoking privileges from non-existent role."""
        with pytest.raises(Exception):
            await client.revoke_privileges("nonexistent_role", [Privilege(PrivilegeCode.Read, "test", None)])

    @pytest.mark.asyncio
    async def test_set_allowlist(self, client):
        """Test setting IP allowlist for role."""
        role_name = "test_role_1"
        privileges = [Privilege(PrivilegeCode.Read, "test", None)]
        initial_allowlist = ["192.168.1.0/24"]
        new_allowlist = ["192.168.1.0/24", "10.0.0.0/8"]
        read_quota = 1000
        write_quota = 500

        # Create role with initial allowlist
        await client.create_role(role_name, privileges, initial_allowlist, read_quota, write_quota)

        # Update allowlist
        await client.set_allowlist(role_name, new_allowlist)

        # Verify role still exists (allowlist update doesn't affect role existence)
        roles = await client.query_roles(role_name)
        assert role_name in str(roles)

    @pytest.mark.asyncio
    async def test_set_allowlist_nonexistent_role(self, client):
        """Test setting allowlist for non-existent role."""
        with pytest.raises(Exception):
            await client.set_allowlist("nonexistent_role", ["192.168.1.0/24"])

    @pytest.mark.asyncio
    async def test_set_quotas(self, client):
        """Test setting quotas for role."""
        role_name = "test_role_1"
        privileges = [Privilege(PrivilegeCode.Read, "test", None)]
        allowlist = ["192.168.1.0/24"]
        initial_read_quota = 1000
        initial_write_quota = 500
        new_read_quota = 2000
        new_write_quota = 1000

        # Create role with initial quotas
        await client.create_role(role_name, privileges, allowlist, initial_read_quota, initial_write_quota)

        # Update quotas
        await client.set_quotas(role_name, new_read_quota, new_write_quota)

        # Verify role still exists (quota update doesn't affect role existence)
        roles = await client.query_roles(role_name)
        assert role_name in str(roles)

    @pytest.mark.asyncio
    async def test_set_quotas_nonexistent_role(self, client):
        """Test setting quotas for non-existent role."""
        with pytest.raises(Exception):
            await client.set_quotas("nonexistent_role", 1000, 500)


@pytest.mark.skip(reason="Security tests disabled - server security not enabled")
class TestAuthentication:
    """Test authentication scenarios."""

    @pytest.mark.asyncio
    async def test_connection_without_credentials(self):
        """Test connection without credentials should fail."""
        host = os.environ.get("AEROSPIKE_HOST_SEC", "localhost:3000")
        client_policy = ClientPolicy()
        # Don't set user/password

        with pytest.raises(Exception):
            client = await new_client(client_policy, host)
            await client.close()

    @pytest.mark.asyncio
    async def test_connection_with_wrong_credentials(self):
        """Test connection with wrong credentials should fail."""
        host = os.environ.get("AEROSPIKE_HOST_SEC", "localhost:3000")
        client_policy = ClientPolicy()
        client_policy.user = "wrong_user"
        client_policy.password = "wrong_password"

        with pytest.raises(Exception):
            client = await new_client(client_policy, host)
            await client.close()

    @pytest.mark.asyncio
    async def test_connection_with_correct_credentials(self):
        """Test connection with correct credentials should succeed."""
        host = os.environ.get("AEROSPIKE_HOST_SEC", "localhost:3000")
        client_policy = ClientPolicy()
        client_policy.user = "admin"
        client_policy.password = "admin"

        try:
            client = await new_client(client_policy, host)
            await client.close()
        except Exception as e:
            pytest.skip(f"Could not connect to security server: {e}")


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])
