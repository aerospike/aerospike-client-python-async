#!/usr/bin/env python3
"""
User Test - Tests for privilege objects and basic functionality.
Security-related tests are in security_test.py
"""

import pytest
from aerospike_async import PrivilegeCode, Privilege


class TestPrivilegeObjects:
    """Test privilege object creation and properties."""

    def test_privilege_creation_global(self):
        """Test creating global privileges."""

        priv = Privilege(PrivilegeCode.UserAdmin, None, None)
        assert str(priv.code) == str(PrivilegeCode.UserAdmin)
        assert priv.namespace is None
        assert priv.set_name is None
        assert str(priv) == "user-admin"

    def test_privilege_creation_namespace(self):
        """Test creating namespace-specific privileges."""

        priv = Privilege(PrivilegeCode.Read, "test", None)
        assert str(priv.code) == str(PrivilegeCode.Read)
        assert priv.namespace == "test"
        assert priv.set_name is None
        assert str(priv) == "read:test"

    def test_privilege_creation_set(self):
        """Test creating set-specific privileges."""

        priv = Privilege(PrivilegeCode.Write, "test", "users")
        assert str(priv.code) == str(PrivilegeCode.Write)
        assert priv.namespace == "test"
        assert priv.set_name == "users"
        assert str(priv) == "write:test.users"

    def test_privilege_equality(self):
        """Test privilege equality."""

        priv1 = Privilege(PrivilegeCode.Read, "test", None)
        priv2 = Privilege(PrivilegeCode.Read, "test", None)
        priv3 = Privilege(PrivilegeCode.Write, "test", None)

        # Test string representation equality since __eq__ might not be implemented
        assert str(priv1) == str(priv2)
        assert str(priv1) != str(priv3)
        # Test individual property equality
        assert priv1.namespace == priv2.namespace
        assert priv1.set_name == priv2.set_name
        assert str(priv1.code) == str(priv2.code)

    def test_privilege_code_enum_values(self):
        """Test all privilege code enum values."""

        expected_codes = [
            PrivilegeCode.UserAdmin,
            PrivilegeCode.SysAdmin,
            PrivilegeCode.DataAdmin,
            PrivilegeCode.UDFAdmin,
            PrivilegeCode.SIndexAdmin,
            PrivilegeCode.Read,
            PrivilegeCode.ReadWrite,
            PrivilegeCode.ReadWriteUDF,
            PrivilegeCode.Write,
            PrivilegeCode.Truncate
        ]

        for code in expected_codes:
            priv = Privilege(code, "test", None)
            assert str(priv.code) == str(code)

    def test_privilege_string_representation(self):
        """Test privilege string representations."""

        # Global privilege
        priv_global = Privilege(PrivilegeCode.UserAdmin, None, None)
        assert str(priv_global) == "user-admin"

        # Namespace privilege
        priv_ns = Privilege(PrivilegeCode.Read, "analytics", None)
        assert str(priv_ns) == "read:analytics"

        # Set privilege
        priv_set = Privilege(PrivilegeCode.Write, "orders", "pending")
        assert str(priv_set) == "write:orders.pending"

    def test_privilege_repr(self):
        """Test privilege repr representation."""

        priv = Privilege(PrivilegeCode.Read, "test", "users")
        repr_str = repr(priv)
        assert "Privilege" in repr_str
        assert "read:test.users" in repr_str


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])