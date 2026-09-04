#!/usr/bin/env python3
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

"""
Security Tests - Tests for user management, role management, and authentication features.
These tests require a server with security enabled and proper authentication.
"""
import asyncio
import contextlib
import pytest
import pytest_asyncio
import os
import time
import uuid
from aerospike_async import new_client, ClientPolicy, PrivilegeCode, Privilege
from aerospike_async.exceptions import ServerError, ResultCode, SecurityNotEnabled

PROPAGATION_RETRIES = 10
PROPAGATION_DELAY = 0.5
# Deadline-based polling for role/user propagation: a retry count hides how
# long the wait actually is, and the interesting failure is "visible but not
# settled", not "absent".
PROPAGATION_TIMEOUT = 5.0
PROPAGATION_INTERVAL = 0.1


async def wait_for_role(
    client, role_name, *, until=None, timeout=PROPAGATION_TIMEOUT,
    interval=PROPAGATION_INTERVAL,
):
    """Poll query_roles until the role is visible. Returns the role list.

    Visibility is not the same as completeness: a role appears in
    ``query_roles`` before all of its fields have propagated. Measured on
    8.1.3, a role created with quotas is queryable after ~5 ms but still
    reports ``read_quota``/``write_quota`` of 0 for a further ~200 ms. A test
    that waits only for existence therefore reads a half-populated role and
    asserts against zeros -- an ordinary-looking assertion that fails on
    timing, not on behavior.

    Pass ``until`` to wait for the property being asserted rather than for
    mere existence::

        roles = await wait_for_role(
            client, name, until=lambda r: r.write_quota == 500,
        )

    ``until`` receives the first role and returns truthy when the state is
    settled. Without it the check is existence only, which is correct for
    callers that assert nothing about the role's contents.
    """
    deadline = time.monotonic() + timeout
    last_seen = None
    while time.monotonic() < deadline:
        try:
            roles = await client.query_roles(role_name)
            if roles and (until is None or until(roles[0])):
                return roles
            last_seen = roles[0] if roles else None
        except ServerError:
            pass
        await asyncio.sleep(interval)
    detail = "never became visible" if last_seen is None else (
        f"was visible but never satisfied the condition; last saw "
        f"allowlist={last_seen.allowlist!r} read_quota={last_seen.read_quota} "
        f"write_quota={last_seen.write_quota}"
    )
    raise TimeoutError(f"Role {role_name!r} {detail} within {timeout}s")


async def wait_for_role_gone(client, role_name, *, retries=PROPAGATION_RETRIES, delay=PROPAGATION_DELAY):
    """Retry query_roles until it raises ServerError (role deleted)."""
    for attempt in range(retries):
        try:
            await client.query_roles(role_name)
        except ServerError:
            return
        if attempt < retries - 1:
            await asyncio.sleep(delay)
    pytest.fail(f"Role {role_name!r} still queryable after {retries} retries")


async def wait_for_user_gone(client, username, *, retries=PROPAGATION_RETRIES, delay=PROPAGATION_DELAY):
    """Retry query_users(username) until it raises ServerError (user deleted)."""
    for attempt in range(retries):
        try:
            await client.query_users(username)
        except ServerError:
            return
        if attempt < retries - 1:
            await asyncio.sleep(delay)
    pytest.fail(f"User {username!r} still queryable after {retries} retries")


async def wait_for_user(client, username, *, retries=PROPAGATION_RETRIES):
    """Retry query_users until the user is visible. Returns the user list."""
    for attempt in range(retries):
        all_users = await client.query_users(None)
        user_names = [u.user for u in all_users]
        if username in user_names:
            return all_users
        if attempt < retries - 1:
            await asyncio.sleep(PROPAGATION_DELAY)
    pytest.fail(f"User {username!r} not found in {user_names} after {retries} retries")


def _short_id():
    """8-char hex unique per invocation — avoids cross-test/cross-run SMD races."""
    return uuid.uuid4().hex[:8]


@pytest_asyncio.fixture(scope="module", loop_scope="module")
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
                assert isinstance(e, SecurityNotEnabled)
                pytest.skip("Security is not enabled on the server")
            else:
                yield True
        finally:
            await client.close()
    except Exception as e:
        pytest.skip(f"Could not connect to security server at {host}: {e}")


@pytest.mark.asyncio(loop_scope="class")
class TestSecurityFeatures:
    """Test security-related features that require server authentication."""

    @pytest_asyncio.fixture(scope="class", loop_scope="class")
    async def client(self, security_enabled):
        """Single client shared across all tests in this class."""
        host = os.environ.get("AEROSPIKE_HOST_SEC", "localhost:3000")
        client_policy = ClientPolicy()
        client_policy.user = "admin"
        client_policy.password = "admin"
        client_policy.use_services_alternate = True

        client = await new_client(client_policy, host)
        yield client
        await client.close()

    @pytest_asyncio.fixture(loop_scope="class")
    async def unique_user(self, client):
        """Factory: returns unique usernames and drops them at teardown."""
        created = []

        def _make(prefix="u"):
            name = f"{prefix}_{_short_id()}"
            created.append(name)
            return name

        yield _make

        for name in created:
            with contextlib.suppress(Exception):
                await client.drop_user(name)

    @pytest_asyncio.fixture(loop_scope="class")
    async def unique_role(self, client):
        """Factory: returns unique role names and drops them at teardown."""
        created = []

        def _make(prefix="r"):
            name = f"{prefix}_{_short_id()}"
            created.append(name)
            return name

        yield _make

        for name in created:
            with contextlib.suppress(Exception):
                await client.drop_role(name)

    async def test_create_user_basic(self, client, unique_user):
        """Test basic user creation.

        Creates a user with basic role and verifies it exists.
        """
        username = unique_user()
        await client.create_user(username, "test_password_123", ["read:test"])
        await wait_for_user(client, username)

    async def test_create_user_multiple_roles(self, client, unique_user):
        """Test user creation with multiple roles.

        Creates a user with multiple roles and verifies it exists.
        """
        username = unique_user()
        await client.create_user(username, "test_password_456", ["read:test", "write:test", "read:analytics"])
        await wait_for_user(client, username)

    async def test_create_user_duplicate(self, client, unique_user):
        """Test creating duplicate user fails.

        Creates a user, then attempts to create the same user again.
        The second creation should raise an exception.
        """
        username = unique_user()
        await client.create_user(username, "test_password_789", ["read:test"])
        with pytest.raises(Exception):
            await client.create_user(username, "test_password_789", ["read:test"])

    async def test_query_users_all(self, client, unique_user):
        """Test querying all users.

        Creates multiple test users and verifies they can all be queried.
        """
        user1 = unique_user()
        user2 = unique_user()
        await client.create_user(user1, "pass1", ["read:test"])
        await client.create_user(user2, "pass2", ["write:test"])
        await wait_for_user(client, user1)
        await wait_for_user(client, user2)

        users = await client.query_users(None)
        user_names = [u.user for u in users]
        assert user1 in user_names
        assert user2 in user_names

    async def test_query_users_specific(self, client, unique_user):
        """Test querying specific user."""
        username = unique_user()
        await client.create_user(username, "test_password_123", ["read:test"])
        await wait_for_user(client, username)

        users = await client.query_users(username)
        assert len(users) > 0
        assert users[0].user == username

    async def test_query_users_nonexistent(self, client):
        """Test querying non-existent user."""
        with pytest.raises(Exception):
            await client.query_users(f"no_such_{_short_id()}")

    async def test_drop_user(self, client, unique_user):
        """Test user deletion."""
        username = unique_user()
        await client.create_user(username, "test_password_123", ["read:test"])
        await wait_for_user(client, username)

        await client.drop_user(username)
        await wait_for_user_gone(client, username, retries=20, delay=1.0)

    async def test_drop_user_nonexistent(self, client):
        """Test deleting non-existent user."""
        with pytest.raises(Exception):
            await client.drop_user(f"no_such_{_short_id()}")

    async def test_create_pki_user(self, client, unique_user):
        """Create a PKI-only user and verify via query_users. Requires server 8.1+."""
        username = unique_user("pki")
        await client.create_pki_user(username, ["read:test"])
        await wait_for_user(client, username)

    async def test_change_password_on_pki_user_fails(self, client, unique_user):
        """create_pki_user sends hash of 'nopassword'; server rejects change_password."""
        username = unique_user("pki")
        await client.create_pki_user(username, ["read:test"])
        await wait_for_user(client, username)

        with pytest.raises(ServerError) as exc_info:
            await client.change_password(username, "new_password_123")
        assert exc_info.value.result_code == ResultCode.FORBIDDEN_PASSWORD

    async def test_change_password(self, client, unique_user):
        """Test password change."""
        username = unique_user()
        await client.create_user(username, "test_password_123", ["read:test"])
        await wait_for_user(client, username)

        await client.change_password(username, "new_password_456")

        users = await client.query_users(username)
        assert len(users) > 0
        assert users[0].user == username

    async def test_change_password_nonexistent(self, client):
        """Test changing password for non-existent user."""
        with pytest.raises(Exception):
            await client.change_password(f"no_such_{_short_id()}", "new_password")

    async def test_grant_roles(self, client, unique_user):
        """Test granting roles to user."""
        username = unique_user()
        await client.create_user(username, "test_password_123", ["read:test"])
        await wait_for_user(client, username)

        await client.grant_roles(username, ["write:test", "read:analytics"])

        users = await client.query_users(username)
        assert len(users) > 0
        assert users[0].user == username

    async def test_grant_roles_nonexistent_user(self, client):
        """Test granting roles to non-existent user."""
        with pytest.raises(Exception):
            await client.grant_roles(f"no_such_{_short_id()}", ["read:test"])

    async def test_revoke_roles(self, client, unique_user):
        """Test revoking roles from user."""
        username = unique_user()
        await client.create_user(username, "test_password_123", ["read:test", "write:test"])
        await wait_for_user(client, username)

        await client.revoke_roles(username, ["write:test"])

        users = await client.query_users(username)
        assert len(users) > 0
        assert users[0].user == username

    async def test_revoke_roles_nonexistent_user(self, client):
        """Test revoking roles from non-existent user."""
        with pytest.raises(Exception):
            await client.revoke_roles(f"no_such_{_short_id()}", ["read:test"])

    async def test_create_role_basic(self, client, unique_role):
        """Test basic role creation."""
        role_name = unique_role()
        privileges = [
            Privilege(PrivilegeCode.Read, "test", None),
            Privilege(PrivilegeCode.Write, "test", None)
        ]

        try:
            await client.create_role(role_name, privileges, ["192.168.1.0/24"], 1000, 500)
        except ServerError as e:
            if "QuotasNotEnabled" in str(e):
                pytest.skip("Quotas are not enabled on the server")
            raise

        roles = await wait_for_role(client, role_name)
        assert roles[0].name == role_name

    async def test_create_role_global_privileges(self, client, unique_role):
        """Test role creation with global privileges."""
        role_name = unique_role()
        privileges = [
            Privilege(PrivilegeCode.UserAdmin, None, None),
            Privilege(PrivilegeCode.SysAdmin, None, None)
        ]

        try:
            await client.create_role(role_name, privileges, ["192.168.1.0/24"], 1000, 500)
        except ServerError as e:
            error_str = str(e)
            if "QuotasNotEnabled" in error_str:
                pytest.skip("Quotas are not enabled on the server")
            if "InvalidQuota" in error_str:
                try:
                    await client.create_role(role_name, privileges, ["192.168.1.0/24"], 1, 1)
                except ServerError as e2:
                    if "QuotasNotEnabled" in str(e2):
                        pytest.skip("Quotas are not enabled on the server")
                    raise e
            else:
                raise

        roles = await wait_for_role(client, role_name)
        assert roles[0].name == role_name

    async def test_create_role_duplicate(self, client, unique_role):
        """Test creating duplicate role fails."""
        role_name = unique_role()
        privileges = [Privilege(PrivilegeCode.Read, "test", None)]

        try:
            await client.create_role(role_name, privileges, ["192.168.1.0/24"], 1000, 500)
            await wait_for_role(client, role_name)
        except ServerError as e:
            if "QuotasNotEnabled" in str(e):
                pytest.skip("Quotas are not enabled on the server")
            raise

        with pytest.raises(ServerError, match="RoleAlreadyExists"):
            await client.create_role(role_name, privileges, ["192.168.1.0/24"], 1000, 500)

    async def test_query_roles_all(self, client, unique_role):
        """Test querying all roles."""
        role1 = unique_role()
        role2 = unique_role()

        try:
            await client.create_role(role1, [Privilege(PrivilegeCode.Read, "test", None)],
                                     ["192.168.1.0/24"], 1000, 500)
        except ServerError as e:
            if "QuotasNotEnabled" in str(e):
                pytest.skip("Quotas are not enabled on the server")
            raise

        await client.create_role(role2, [Privilege(PrivilegeCode.Write, "test", None)],
                                 ["192.168.1.0/24"], 1000, 500)

        await wait_for_role(client, role1)
        await wait_for_role(client, role2)

        roles = await client.query_roles(None)
        role_names = [r.name for r in roles]
        assert role1 in role_names
        assert role2 in role_names

    async def test_query_roles_specific(self, client, unique_role):
        """Test querying specific role."""
        role_name = unique_role()

        try:
            await client.create_role(role_name, [Privilege(PrivilegeCode.Read, "test", None)],
                                     ["192.168.1.0/24"], 1000, 500)
        except ServerError as e:
            if "QuotasNotEnabled" in str(e):
                pytest.skip("Quotas are not enabled on the server")
            raise

        roles = await wait_for_role(client, role_name)
        assert roles[0].name == role_name

    async def test_admin_policy_timeout(self, client):
        """Test AdminPolicy with custom timeout."""
        from aerospike_async import AdminPolicy

        default_policy = AdminPolicy()
        assert isinstance(default_policy.timeout, int)
        assert default_policy.timeout >= 0

        custom_policy = AdminPolicy()
        custom_policy.timeout = 10000
        assert custom_policy.timeout == 10000

        roles = await client.query_roles(None, policy=custom_policy)
        assert isinstance(roles, list)
        assert roles, "query_roles with a custom AdminPolicy should return the built-in roles"
        assert all(hasattr(r, "name") for r in roles)

    async def test_query_roles_nonexistent(self, client):
        """Test querying non-existent role."""
        with pytest.raises(Exception):
            await client.query_roles(f"no_such_{_short_id()}")

    @pytest.mark.slow
    async def test_drop_role(self, client, unique_role):
        """Test role deletion."""
        role_name = unique_role()

        try:
            await client.create_role(role_name, [Privilege(PrivilegeCode.Read, "test", None)],
                                     ["192.168.1.0/24"], 1000, 500)
        except ServerError as e:
            if "QuotasNotEnabled" in str(e):
                pytest.skip("Quotas are not enabled on the server")
            raise

        await wait_for_role(client, role_name)

        await client.drop_role(role_name)
        await wait_for_role_gone(client, role_name, retries=20, delay=1.0)

    async def test_drop_role_nonexistent(self, client):
        """Test deleting non-existent role."""
        with pytest.raises(Exception):
            await client.drop_role(f"no_such_{_short_id()}")

    async def test_grant_privileges(self, client, unique_role):
        """Test granting privileges to role."""
        role_name = unique_role()

        try:
            await client.create_role(role_name, [Privilege(PrivilegeCode.Read, "test", None)],
                                     ["192.168.1.0/24"], 1000, 500)
        except ServerError as e:
            if "QuotasNotEnabled" in str(e):
                pytest.skip("Quotas are not enabled on the server")
            raise

        await wait_for_role(client, role_name)

        new_privileges = [Privilege(PrivilegeCode.Write, "test", None)]
        await client.grant_privileges(role_name, new_privileges)
        await client.grant_privileges(role_name, new_privileges)

        roles = await client.query_roles(role_name)
        assert len(roles) > 0
        assert roles[0].name == role_name

    async def test_grant_privileges_nonexistent_role(self, client):
        """Test granting privileges to non-existent role."""
        with pytest.raises(Exception):
            await client.grant_privileges(f"no_such_{_short_id()}", [Privilege(PrivilegeCode.Read, "test", None)])

    async def test_revoke_privileges(self, client, unique_role):
        """Test revoking privileges from role."""
        role_name = unique_role()
        initial_privileges = [
            Privilege(PrivilegeCode.Read, "test", None),
            Privilege(PrivilegeCode.Write, "test", None)
        ]

        try:
            await client.create_role(role_name, initial_privileges, ["192.168.1.0/24"], 1000, 500)
        except ServerError as e:
            if "QuotasNotEnabled" in str(e):
                pytest.skip("Quotas are not enabled on the server")
            raise

        await wait_for_role(client, role_name)

        await client.revoke_privileges(role_name, [Privilege(PrivilegeCode.Write, "test", None)])

        roles = await client.query_roles(role_name)
        assert len(roles) > 0
        assert roles[0].name == role_name

    async def test_revoke_privileges_nonexistent_role(self, client):
        """Test revoking privileges from non-existent role."""
        with pytest.raises(Exception):
            await client.revoke_privileges(f"no_such_{_short_id()}", [Privilege(PrivilegeCode.Read, "test", None)])

    async def test_set_allowlist(self, client, unique_role):
        """Test setting IP allowlist for role."""
        role_name = unique_role()

        try:
            await client.create_role(role_name, [Privilege(PrivilegeCode.Read, "test", None)],
                                     ["192.168.1.0/24"], 1000, 500)
        except ServerError as e:
            if "QuotasNotEnabled" in str(e):
                pytest.skip("Quotas are not enabled on the server")
            raise

        await wait_for_role(client, role_name)

        await client.set_allowlist(role_name, ["192.168.1.0/24", "10.0.0.0/8"])

        roles = await client.query_roles(role_name)
        assert len(roles) > 0
        assert roles[0].name == role_name

    async def test_set_allowlist_nonexistent_role(self, client):
        """Test setting allowlist for non-existent role."""
        with pytest.raises(Exception):
            await client.set_allowlist(f"no_such_{_short_id()}", ["192.168.1.0/24"])

    async def test_set_quotas(self, client, unique_role):
        """Test setting quotas for role."""
        role_name = unique_role()

        try:
            await client.create_role(role_name, [Privilege(PrivilegeCode.Read, "test", None)],
                                     ["192.168.1.0/24"], 1000, 500)
        except ServerError as e:
            if "QuotasNotEnabled" in str(e):
                pytest.skip("Quotas are not enabled on the server")
            raise

        await wait_for_role(client, role_name)

        await client.set_quotas(role_name, 2000, 1000)

        roles = await client.query_roles(role_name)
        assert len(roles) > 0
        assert roles[0].name == role_name

    async def test_set_quotas_nonexistent_role(self, client):
        """Test setting quotas for non-existent role."""
        with pytest.raises(Exception):
            await client.set_quotas(f"no_such_{_short_id()}", 1000, 500)

    async def test_create_role_allowlist_and_quotas_round_trip(self, client, unique_role):
        """Allowlist and quotas set at create time read back intact.

        A wire-encoding defect used to land the allowlist bytes in the
        read-quota field, so quota values of 0 appeared to be rejected as
        InvalidQuota while the allowlist itself was silently lost. This
        pins the corrected encoding: both survive the round trip, and a
        zero quota (no limit) is accepted.
        """
        quota_role = unique_role()
        unlimited_role = unique_role()
        privileges = [Privilege(PrivilegeCode.Read, "test", None)]
        allowlist = ["192.168.1.0/24"]

        try:
            await client.create_role(quota_role, privileges, allowlist, 1000, 500)
        except ServerError as e:
            if "QuotasNotEnabled" in str(e):
                pytest.skip("Quotas are not enabled on the server")
            raise

        # Wait for the quotas themselves: the role is queryable ~200ms before
        # they propagate, so waiting on existence alone reads zeros.
        roles = await wait_for_role(
            client, quota_role,
            until=lambda r: r.read_quota == 1000 and r.write_quota == 500,
        )
        assert roles[0].allowlist == allowlist
        assert roles[0].read_quota == 1000
        assert roles[0].write_quota == 500

        # Zero quotas mean "no limit" and must be accepted.
        await client.create_role(unlimited_role, privileges, allowlist, 0, 0)
        # 0 is indistinguishable from "not yet propagated", so settle on the
        # allowlist instead -- it lands with the role and is asserted below.
        roles = await wait_for_role(
            client, unlimited_role, until=lambda r: r.allowlist == allowlist,
        )
        assert roles[0].allowlist == allowlist
        assert roles[0].read_quota == 0
        assert roles[0].write_quota == 0

        # An empty allowlist clears the existing one (sent as role-only).
        await client.set_allowlist(unlimited_role, [])
        for _ in range(PROPAGATION_RETRIES):
            roles = await client.query_roles(unlimited_role)
            if roles[0].allowlist == []:
                break
            await asyncio.sleep(PROPAGATION_DELAY)
        assert roles[0].allowlist == []


@pytest.mark.asyncio(loop_scope="class")
class TestAuthentication:
    """Test authentication scenarios."""

    async def test_connection_without_credentials(self, security_enabled):
        """Test connection without credentials should fail."""
        host = os.environ.get("AEROSPIKE_HOST_SEC", "localhost:3000")
        client_policy = ClientPolicy()
        client_policy.use_services_alternate = True

        with pytest.raises(Exception):
            client = await new_client(client_policy, host)
            await client.close()

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
    pytest.main([__file__, "-v"])
