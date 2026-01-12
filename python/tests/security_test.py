#!/usr/bin/env python3
"""
Security Tests - Tests for user management, role management, and authentication features.
These tests require a server with security enabled and proper authentication.
"""
import pytest
import os
from aerospike_async import new_client, ClientPolicy, PrivilegeCode, Privilege
from aerospike_async.exceptions import ServerError, ResultCode


@pytest.fixture(scope="module")
async def security_enabled():
    """Check if security is enabled on the server."""
    host = os.environ.get("AEROSPIKE_HOST_SEC", "localhost:3000")
    client_policy = ClientPolicy()
    client_policy.user = "admin"
    client_policy.password = "admin"
    client_policy.use_services_alternate = True

    try:
        client = await new_client(client_policy, host)
        try:
            await client.query_users(None)
            yield True
        except ServerError as e:
            if e.result_code == ResultCode.SECURITY_NOT_ENABLED:
                pytest.skip("Security is not enabled on the server")
            else:
                yield True
        finally:
            await client.close()
    except Exception as e:
        pytest.skip(f"Could not connect to security server at {host}: {e}")


class TestSecurityFeatures:
    """Test security-related features that require server authentication."""

    @pytest.fixture(scope="class")
    async def client(self, security_enabled):
        """Create a client for testing with security enabled."""
        host = os.environ.get("AEROSPIKE_HOST_SEC", "localhost:3000")
        client_policy = ClientPolicy()
        client_policy.user = "admin"
        client_policy.password = "admin"
        client_policy.use_services_alternate = True

        client = await new_client(client_policy, host)
        yield client
        await client.close()

    @pytest.fixture(autouse=True)
    async def cleanup_users(self, client):
        """Clean up test users before and after each test."""
        test_users = ["test_user_1", "test_user_2", "test_user_3", "test_admin", "test_app_user"]
        for username in test_users:
            try:
                await client.drop_user(username)
            except:
                pass
        yield
        for username in test_users:
            try:
                await client.drop_user(username)
            except:
                pass

    @pytest.fixture(autouse=True)
    async def cleanup_roles(self, client):
        """Clean up test roles before and after each test."""
        test_roles = ["test_role_1", "test_role_2", "test_app_role", "test_analytics_role"]
        for role_name in test_roles:
            try:
                await client.drop_role(role_name)
            except:
                pass
        yield
        for role_name in test_roles:
            try:
                await client.drop_role(role_name)
            except:
                pass

    @pytest.mark.asyncio
    async def test_create_user_basic(self, client):
        """Test basic user creation.

        Creates a user with basic role and verifies it exists.
        """

        username = "test_user_1"
        password = "test_password_123"
        roles = ["read:test"]

        await client.create_user(username, password, roles)

        # Retry for eventual consistency
        import asyncio
        for attempt in range(3):
            all_users = await client.query_users(None)
            user_names = [u.user for u in all_users]
            if username in user_names:
                break
            if attempt < 2:  # Don't sleep on last attempt
                await asyncio.sleep(0.1)
        assert username in user_names, f"User {username} not found in {user_names} after creation"
    @pytest.mark.asyncio
    async def test_create_user_multiple_roles(self, client):
        """Test user creation with multiple roles.

        Creates a user with multiple roles and verifies it exists.
        """

        username = "test_user_2"
        password = "test_password_456"
        roles = ["read:test", "write:test", "read:analytics"]

        await client.create_user(username, password, roles)
        all_users = await client.query_users(None)
        user_names = [u.user for u in all_users]
        assert username in user_names
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

        # Retry for eventual consistency
        import asyncio
        for attempt in range(3):
            users = await client.query_users(None)
            user_names = [u.user for u in users]
            if "test_user_1" in user_names and "test_user_2" in user_names:
                break
            if attempt < 2:
                await asyncio.sleep(0.1)
        assert "test_user_1" in user_names, f"User test_user_1 not found in {user_names}"
        assert "test_user_2" in user_names, f"User test_user_2 not found in {user_names}"
    @pytest.mark.asyncio
    async def test_query_users_specific(self, client):
        """Test querying specific user."""
        username = "test_user_1"
        password = "test_password_123"
        roles = ["read:test"]

        # Create user
        await client.create_user(username, password, roles)

        # Retry for eventual consistency
        import asyncio
        for attempt in range(3):
            try:
                users = await client.query_users(username)
                if len(users) > 0 and users[0].user == username:
                    break
            except ServerError as e:
                if e.result_code == ResultCode.INVALID_USER and attempt < 2:
                    await asyncio.sleep(0.1)
                    continue
                raise
            if attempt < 2:
                await asyncio.sleep(0.1)

        users = await client.query_users(username)
        assert len(users) > 0
        assert users[0].user == username
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

        # Retry for eventual consistency
        import asyncio
        for attempt in range(3):
            try:
                users = await client.query_users(username)
                if len(users) > 0 and users[0].user == username:
                    break
            except ServerError as e:
                if e.result_code == ResultCode.INVALID_USER and attempt < 2:
                    await asyncio.sleep(0.1)
                    continue
                raise
            if attempt < 2:
                await asyncio.sleep(0.1)

        users = await client.query_users(username)
        assert len(users) > 0
        assert users[0].user == username

        await client.drop_user(username)

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

        await client.create_user(username, password, roles)

        # Retry for eventual consistency before changing password
        import asyncio
        for attempt in range(3):
            try:
                await client.change_password(username, new_password)
                break
            except ServerError as e:
                if e.result_code == ResultCode.INVALID_USER and attempt < 2:
                    await asyncio.sleep(0.1)
                    continue
                raise
            if attempt < 2:
                await asyncio.sleep(0.1)

        users = await client.query_users(username)
        assert len(users) > 0
        assert users[0].user == username
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

        await client.create_user(username, password, initial_roles)

        # Retry for eventual consistency before granting roles
        import asyncio
        for attempt in range(3):
            try:
                await client.grant_roles(username, new_roles)
                break
            except ServerError as e:
                if e.result_code == ResultCode.INVALID_USER and attempt < 2:
                    await asyncio.sleep(0.1)
                    continue
                raise
            if attempt < 2:
                await asyncio.sleep(0.1)

        users = await client.query_users(username)
        assert len(users) > 0
        assert users[0].user == username
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

        await client.create_user(username, password, initial_roles)

        # Retry for eventual consistency before revoking roles
        import asyncio
        for attempt in range(3):
            try:
                await client.revoke_roles(username, roles_to_revoke)
                break
            except ServerError as e:
                if e.result_code == ResultCode.INVALID_USER and attempt < 2:
                    await asyncio.sleep(0.1)
                    continue
                raise
            if attempt < 2:
                await asyncio.sleep(0.1)

        users = await client.query_users(username)
        assert len(users) > 0
        assert users[0].user == username
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
        try:
            await client.create_role(role_name, privileges, allowlist, read_quota, write_quota)
        except ServerError as e:
            if "QuotasNotEnabled" in str(e):
                pytest.skip("Quotas are not enabled on the server")
            raise

        # Verify role exists
        roles = await client.query_roles(role_name)
        assert len(roles) > 0
        assert roles[0].name == role_name
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

        try:
            await client.create_role(role_name, privileges, allowlist, read_quota, write_quota)
        except ServerError as e:
            error_str = str(e)
            if "QuotasNotEnabled" in error_str:
                pytest.skip("Quotas are not enabled on the server")
            # Server may reject zero quotas - retry with non-zero
            if "InvalidQuota" in error_str:
                try:
                    await client.create_role(role_name, privileges, allowlist, 1, 1)
                except ServerError as e2:
                    if "QuotasNotEnabled" in str(e2):
                        pytest.skip("Quotas are not enabled on the server")
                    raise e
            else:
                raise

        # Verify role exists
        roles = await client.query_roles(role_name)
        assert len(roles) > 0
        assert roles[0].name == role_name
    @pytest.mark.asyncio
    async def test_create_role_duplicate(self, client):
        """Test creating duplicate role fails."""
        role_name = "test_role_1"
        privileges = [Privilege(PrivilegeCode.Read, "test", None)]
        allowlist = ["192.168.1.0/24"]
        read_quota = 1000
        write_quota = 500

        try:
            await client.drop_role(role_name)
        except:
            pass

        # Create the role first
        try:
            await client.create_role(role_name, privileges, allowlist, read_quota, write_quota)
        except ServerError as e:
            if "QuotasNotEnabled" in str(e):
                pytest.skip("Quotas are not enabled on the server")
            raise

        # Now try to create it again - should raise an exception
        with pytest.raises(Exception):
            await client.create_role(role_name, privileges, allowlist, read_quota, write_quota)
    @pytest.mark.asyncio
    async def test_query_roles_all(self, client):
        """Test querying all roles."""
        # TODO: CLIENT-4052 - Waiting on parse_privileges fix in Rust core
        # The parse_privileges fix ensures we consume exactly len bytes to prevent buffer misalignment
        # This test is skipped until the fix is integrated and verified
        pytest.skip("Waiting on parse_privileges fix as described in CLIENT-4052")

        # Clean up: drop roles if they exist
        try:
            await client.drop_role("test_role_1")
        except ServerError:
            pass
        try:
            await client.drop_role("test_role_2")
        except ServerError:
            pass

        # Create test roles
        try:
            await client.create_role("test_role_1", [Privilege(PrivilegeCode.Read, "test", None)],
                                   ["192.168.1.0/24"], 1000, 500)
        except ServerError as e:
            if "QuotasNotEnabled" in str(e):
                pytest.skip("Quotas are not enabled on the server")
            elif "RoleAlreadyExists" in str(e):
                pass  # Role already exists, continue
            else:
                raise

        try:
            await client.create_role("test_role_2", [Privilege(PrivilegeCode.Write, "test", None)],
                                   ["192.168.1.0/24"], 1000, 500)
        except ServerError as e:
            if "QuotasNotEnabled" in str(e):
                pytest.skip("Quotas are not enabled on the server")
            elif "RoleAlreadyExists" in str(e):
                pass  # Role already exists, continue
            else:
                raise

        # Query all roles - should return a list of all roles
        roles = await client.query_roles(None)
        role_names = [r.name for r in roles]
        assert "test_role_1" in role_names
        assert "test_role_2" in role_names
    @pytest.mark.asyncio

    async def test_query_roles_specific(self, client):
        """Test querying specific role."""
        role_name = "test_role_1"
        privileges = [Privilege(PrivilegeCode.Read, "test", None)]
        allowlist = ["192.168.1.0/24"]
        read_quota = 1000
        write_quota = 500

        try:
            await client.create_role(role_name, privileges, allowlist, read_quota, write_quota)
        except ServerError as e:
            if "QuotasNotEnabled" in str(e):
                pytest.skip("Quotas are not enabled on the server")
            raise

    @pytest.mark.asyncio
    async def test_admin_policy_timeout(self, client):
        """Test AdminPolicy with custom timeout."""
        from aerospike_async import AdminPolicy

        # Test default AdminPolicy
        default_policy = AdminPolicy()
        assert default_policy.timeout == 3000

        # Test custom timeout
        custom_policy = AdminPolicy()
        custom_policy.timeout = 10000
        assert custom_policy.timeout == 10000

        # Test that AdminPolicy can be passed to query_roles
        try:
            roles = await client.query_roles(None, policy=custom_policy)
            assert isinstance(roles, list)
        except ServerError as e:
            if e.result_code == ResultCode.ALWAYS_FORBIDDEN:
                pytest.skip("Server returned ALWAYS_FORBIDDEN (CLIENT-4052)")
            raise

        # Test that default behavior still works (backward compatibility)
        try:
            roles_default = await client.query_roles(None)
            assert isinstance(roles_default, list)
            assert len(roles) == len(roles_default)
        except ServerError as e:
            if e.result_code == ResultCode.ALWAYS_FORBIDDEN:
                pytest.skip("Server returned ALWAYS_FORBIDDEN (CLIENT-4052)")
            raise

        roles = await client.query_roles(role_name)
        roles = await client.query_roles(role_name)
        assert len(roles) > 0
        assert roles[0].name == role_name
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

        try:
            await client.create_role(role_name, privileges, allowlist, read_quota, write_quota)
        except ServerError as e:
            if "QuotasNotEnabled" in str(e):
                pytest.skip("Quotas are not enabled on the server")
            raise

        roles = await client.query_roles(role_name)
        assert len(roles) > 0
        assert roles[0].name == role_name

        await client.drop_role(role_name)

        with pytest.raises(Exception):
            await client.query_roles(role_name)

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

        try:
            await client.create_role(role_name, initial_privileges, allowlist, read_quota, write_quota)
        except ServerError as e:
            if "QuotasNotEnabled" in str(e):
                pytest.skip("Quotas are not enabled on the server")
            raise

        await client.grant_privileges(role_name, new_privileges)
        await client.grant_privileges(role_name, new_privileges)

        roles = await client.query_roles(role_name)
        assert len(roles) > 0
        assert roles[0].name == role_name
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

        try:
            await client.create_role(role_name, initial_privileges, allowlist, read_quota, write_quota)
        except ServerError as e:
            if "QuotasNotEnabled" in str(e):
                pytest.skip("Quotas are not enabled on the server")
            raise

        await client.revoke_privileges(role_name, privileges_to_revoke)
        await client.revoke_privileges(role_name, privileges_to_revoke)

        roles = await client.query_roles(role_name)
        assert len(roles) > 0
        assert roles[0].name == role_name
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

        try:
            await client.create_role(role_name, privileges, initial_allowlist, read_quota, write_quota)
        except ServerError as e:
            if "QuotasNotEnabled" in str(e):
                pytest.skip("Quotas are not enabled on the server")
            raise

        await client.set_allowlist(role_name, new_allowlist)
        await client.set_allowlist(role_name, new_allowlist)

        roles = await client.query_roles(role_name)
        assert len(roles) > 0
        assert roles[0].name == role_name
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

        try:
            await client.create_role(role_name, privileges, allowlist, initial_read_quota, initial_write_quota)
        except ServerError as e:
            if "QuotasNotEnabled" in str(e):
                pytest.skip("Quotas are not enabled on the server")
            raise

        await client.set_quotas(role_name, new_read_quota, new_write_quota)
        await client.set_quotas(role_name, new_read_quota, new_write_quota)

        roles = await client.query_roles(role_name)
        assert len(roles) > 0
        assert roles[0].name == role_name
    @pytest.mark.asyncio

    async def test_set_quotas_nonexistent_role(self, client):
        """Test setting quotas for non-existent role."""
        with pytest.raises(Exception):
            await client.set_quotas("nonexistent_role", 1000, 500)

    @pytest.mark.asyncio
    async def test_create_role_invalid_quota(self, client):
        """Test that creating a role with invalid quota values raises an error."""
        role_name = "test_role_invalid_quota"
        privileges = [Privilege(PrivilegeCode.Read, "test", None)]
        allowlist = ["192.168.1.0/24"]

        check_role_name = "test_role_check_quotas"
        quotas_enabled = False

        try:
            await client.drop_role(check_role_name)
        except:
            pass

        try:
            await client.create_role(check_role_name, privileges, allowlist, 1000, 500)
            quotas_enabled = True
            try:
                await client.drop_role(check_role_name)
            except:
                pass
        except ServerError as e:
            if "QuotasNotEnabled" in str(e):
                pytest.skip("Quotas are not enabled on the server")
            if "RoleAlreadyExists" in str(e):
                try:
                    await client.drop_role(check_role_name)
                    await client.create_role(check_role_name, privileges, allowlist, 1000, 500)
                    quotas_enabled = True
                    await client.drop_role(check_role_name)
                except ServerError as e2:
                    if "QuotasNotEnabled" in str(e2):
                        pytest.skip("Quotas are not enabled on the server")
                    raise
            else:
                raise

        if not quotas_enabled:
            pytest.skip("Quotas are not enabled on the server")

        with pytest.raises(ServerError) as exc_info:
            await client.create_role(role_name, privileges, allowlist, 0, 0)

        error_str = str(exc_info.value)
        assert "InvalidQuota" in error_str
        assert "QuotasNotEnabled" not in error_str


class TestAuthentication:
    """Test authentication scenarios."""

    @pytest.mark.asyncio

    async def test_connection_without_credentials(self, security_enabled):
        """Test connection without credentials should fail."""
        host = os.environ.get("AEROSPIKE_HOST_SEC", "localhost:3000")
        client_policy = ClientPolicy()
        client_policy.use_services_alternate = True

        with pytest.raises(Exception):
            client = await new_client(client_policy, host)
            await client.close()

    @pytest.mark.asyncio

    async def test_connection_with_wrong_credentials(self, security_enabled):
        """Test connection with wrong credentials should fail."""
        host = os.environ.get("AEROSPIKE_HOST_SEC", "localhost:3000")
        client_policy = ClientPolicy()
        client_policy.use_services_alternate = True
        client_policy.user = "wrong_user"
        client_policy.password = "wrong_password"

        with pytest.raises(Exception):
            client = await new_client(client_policy, host)
            await client.close()

    @pytest.mark.asyncio

    async def test_connection_with_correct_credentials(self, security_enabled):
        """Test connection with correct credentials should succeed."""
        host = os.environ.get("AEROSPIKE_HOST_SEC", "localhost:3000")
        client_policy = ClientPolicy()
        client_policy.use_services_alternate = True
        client_policy.user = "admin"
        client_policy.password = "admin"

        client = await new_client(client_policy, host)
        await client.close()


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])

