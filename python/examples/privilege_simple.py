#!/usr/bin/env python3
"""
Simple examples of PrivilegeCode and Privilege functionality (no server required).

NOTE: No server connection is needed for these examples to run.
These examples work with privilege objects locally without connecting to an Aerospike server.
"""

from aerospike_async import PrivilegeCode, Privilege


def test_privilege_creation():
    """Test creating and using Privilege and PrivilegeCode objects."""
    
    print("🔐 Simple Privilege Examples (No Server Required)")
    print("="*60)
    
    print("\n--- PrivilegeCode Enum Values ---")
    
    # Show all available privilege codes
    codes = [
        PrivilegeCode.UserAdmin,
        PrivilegeCode.SysAdmin,
        PrivilegeCode.DataAdmin,
        PrivilegeCode.UDFAdmin,
        PrivilegeCode.SIndexAdmin,
        PrivilegeCode.Read,
        PrivilegeCode.ReadWrite,
        PrivilegeCode.ReadWriteUDF,
        PrivilegeCode.Write,
        PrivilegeCode.Truncate,
    ]
    
    for code in codes:
        print(f"  {code}")
    
    print("\n--- Creating Privilege Objects ---")
    
    # Global privileges
    user_admin = Privilege(PrivilegeCode.UserAdmin, None, None)
    sys_admin = Privilege(PrivilegeCode.SysAdmin, None, None)
    
    print(f"UserAdmin: {user_admin}")
    print(f"SysAdmin: {sys_admin}")
    
    # Namespace-specific privileges
    read_priv = Privilege(PrivilegeCode.Read, "test", None)
    write_priv = Privilege(PrivilegeCode.Write, "test", None)
    
    print(f"Read (test): {read_priv}")
    print(f"Write (test): {write_priv}")
    
    # Set-specific privileges
    users_read = Privilege(PrivilegeCode.Read, "test", "users")
    users_write = Privilege(PrivilegeCode.Write, "test", "users")
    
    print(f"Read (test.users): {users_read}")
    print(f"Write (test.users): {users_write}")
    
    print("\n--- Accessing Privilege Properties ---")
    
    # Access properties
    print(f"Code: {read_priv.code}")
    print(f"Namespace: {read_priv.namespace}")
    print(f"Set Name: {read_priv.set_name}")
    
    print(f"String: {read_priv.as_string()}")
    print(f"Repr: {repr(read_priv)}")
    
    print("\n--- Privilege Validation ---")
    
    # Test privilege validation
    privileges = [
        Privilege(PrivilegeCode.Read, "test", None),
        Privilege(PrivilegeCode.Write, "test", None),
    ]
    
    def has_privilege(privs, code):
        return any(p.code == code for p in privs)
    
    print(f"Has Read: {has_privilege(privileges, PrivilegeCode.Read)}")
    print(f"Has Write: {has_privilege(privileges, PrivilegeCode.Write)}")
    print(f"Has UserAdmin: {has_privilege(privileges, PrivilegeCode.UserAdmin)}")
    
    print("\n--- Common Use Cases ---")
    
    # Database administrator
    admin_privs = [
        Privilege(PrivilegeCode.UserAdmin, None, None),
        Privilege(PrivilegeCode.SysAdmin, None, None),
        Privilege(PrivilegeCode.DataAdmin, None, None),
    ]
    print("Database Administrator privileges:")
    for priv in admin_privs:
        print(f"  - {priv}")
    
    # Application user
    app_privs = [
        Privilege(PrivilegeCode.Read, "app_data", None),
        Privilege(PrivilegeCode.Write, "app_data", None),
    ]
    print("\nApplication User privileges:")
    for priv in app_privs:
        print(f"  - {priv}")
    
    # Analytics user
    analytics_privs = [
        Privilege(PrivilegeCode.Read, "analytics", None),
        Privilege(PrivilegeCode.Read, "logs", None),
    ]
    print("\nAnalytics User privileges:")
    for priv in analytics_privs:
        print(f"  - {priv}")
    
    print("\n--- Privilege Code Comparison ---")
    
    # Test privilege code comparison
    code1 = PrivilegeCode.Read
    code2 = PrivilegeCode.Write
    code3 = PrivilegeCode.Read
    
    print(f"Read == Read: {code1 == code3}")
    print(f"Read == Write: {code1 == code2}")
    print(f"Read != Write: {code1 != code2}")
    
    print("\n✅ All privilege examples completed successfully!")


if __name__ == "__main__":
    test_privilege_creation()
