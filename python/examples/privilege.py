#!/usr/bin/env python3
"""
Examples demonstrating PrivilegeCode and Privilege functionality in aerospike_async.

NOTE: No server connection is needed for these examples to run.
These examples work with privilege objects locally without connecting to an Aerospike server.
"""

from aerospike_async import PrivilegeCode, Privilege


def privilege():
    """Examples of using PrivilegeCode and Privilege classes."""
    
    print("🔐 Privilege and PrivilegeCode Examples")
    print("="*50)
    print("NOTE: No server connection required - these examples work locally!")
    print("="*50)
        
    print("\n--- PrivilegeCode Enum Examples ---")
    
    # Show all available privilege codes
    print("Available Privilege Codes:")
    privilege_codes = [
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
    
    for code in privilege_codes:
        print(f"  - {code}")
    
    print("\n--- Privilege Object Examples ---")
    
    # Create different types of privileges
    print("Creating different privilege objects:")
    
    # Global privileges (no namespace/set)
    user_admin_privilege = Privilege(PrivilegeCode.UserAdmin, None, None)
    sys_admin_privilege = Privilege(PrivilegeCode.SysAdmin, None, None)
    data_admin_privilege = Privilege(PrivilegeCode.DataAdmin, None, None)
    
    print(f"  UserAdmin: {user_admin_privilege}")
    print(f"  SysAdmin: {sys_admin_privilege}")
    print(f"  DataAdmin: {data_admin_privilege}")
    
    # Namespace-specific privileges
    read_privilege = Privilege(PrivilegeCode.Read, "test", None)
    write_privilege = Privilege(PrivilegeCode.Write, "test", None)
    read_write_privilege = Privilege(PrivilegeCode.ReadWrite, "test", None)
    
    print(f"  Read (test namespace): {read_privilege}")
    print(f"  Write (test namespace): {write_privilege}")
    print(f"  ReadWrite (test namespace): {read_write_privilege}")
    
    # Set-specific privileges
    users_read_privilege = Privilege(PrivilegeCode.Read, "test", "users")
    users_write_privilege = Privilege(PrivilegeCode.Write, "test", "users")
    
    print(f"  Read (test.users set): {users_read_privilege}")
    print(f"  Write (test.users set): {users_write_privilege}")
    
    print("\n--- Privilege Property Access ---")
    
    # Access privilege properties
    print("Privilege properties:")
    print(f"  Code: {read_privilege.code}")
    print(f"  Namespace: {read_privilege.namespace}")
    print(f"  Set Name: {read_privilege.set_name}")
    
    print(f"  String representation: {read_privilege.as_string()}")
    print(f"  Repr: {repr(read_privilege)}")
    
    print("\n--- Privilege Code Properties ---")
    
    # Show privilege code details
    print("Privilege code details:")
    for code in [PrivilegeCode.UserAdmin, PrivilegeCode.Read, PrivilegeCode.ReadWrite]:
        print(f"  {code}: {code}")
    
    print("\n--- Common Privilege Patterns ---")
    
    # Common privilege patterns for different use cases
    print("Common privilege patterns:")
    
    # Database administrator
    db_admin_privileges = [
        Privilege(PrivilegeCode.UserAdmin, None, None),
        Privilege(PrivilegeCode.SysAdmin, None, None),
        Privilege(PrivilegeCode.DataAdmin, None, None),
    ]
    print("  Database Administrator:")
    for priv in db_admin_privileges:
        print(f"    - {priv}")
    
    # Application user with read/write access to specific namespace
    app_user_privileges = [
        Privilege(PrivilegeCode.Read, "app_data", None),
        Privilege(PrivilegeCode.Write, "app_data", None),
    ]
    print("  Application User (app_data namespace):")
    for priv in app_user_privileges:
        print(f"    - {priv}")
    
    # Analytics user with read access to multiple namespaces
    analytics_privileges = [
        Privilege(PrivilegeCode.Read, "analytics", None),
        Privilege(PrivilegeCode.Read, "logs", None),
    ]
    print("  Analytics User:")
    for priv in analytics_privileges:
        print(f"    - {priv}")
    
    # UDF developer with UDF admin access
    udf_developer_privileges = [
        Privilege(PrivilegeCode.UDFAdmin, None, None),
        Privilege(PrivilegeCode.Read, "test", None),
    ]
    print("  UDF Developer:")
    for priv in udf_developer_privileges:
        print(f"    - {priv}")
    
    print("\n--- Privilege Validation Examples ---")
    
    # Validate privilege combinations
    def validate_privilege_combination(privileges, required_code):
        """Check if a privilege list contains the required privilege code."""
        return any(priv.code == required_code for priv in privileges)
    
    # Test validation
    test_privileges = [
        Privilege(PrivilegeCode.Read, "test", None),
        Privilege(PrivilegeCode.Write, "test", None),
    ]
    
    has_read = validate_privilege_combination(test_privileges, PrivilegeCode.Read)
    has_admin = validate_privilege_combination(test_privileges, PrivilegeCode.UserAdmin)
    
    print(f"  Privileges contain Read: {has_read}")
    print(f"  Privileges contain UserAdmin: {has_admin}")
    
    print("\n--- Error Handling Examples ---")
    
    # Show how to handle privilege creation errors
    try:
        # This should work fine
        valid_privilege = Privilege(PrivilegeCode.Read, "test", "users")
        print(f"  ✅ Valid privilege created: {valid_privilege}")
    except Exception as e:
        print(f"  ❌ Error creating privilege: {e}")
    
    # Note: The actual privilege validation would happen on the server side
    # when these privileges are used for authentication


def show_privilege_usage():
    """Show usage examples and patterns for Privilege and PrivilegeCode."""
    
    print("\n" + "="*60)
    print("PRIVILEGE USAGE EXAMPLES")
    print("="*60)
    
    print("\n1. Basic Privilege Creation:")
    print("""
    # Global privileges (no namespace/set)
    user_admin = Privilege(PrivilegeCode.UserAdmin, None, None)
    sys_admin = Privilege(PrivilegeCode.SysAdmin, None, None)
    
    # Namespace-specific privileges
    read_privilege = Privilege(PrivilegeCode.Read, "test", None)
    write_privilege = Privilege(PrivilegeCode.Write, "test", None)
    
    # Set-specific privileges
    users_read = Privilege(PrivilegeCode.Read, "test", "users")
    users_write = Privilege(PrivilegeCode.Write, "test", "users")
    """)
    
    print("\n2. Available Privilege Codes:")
    print("""
    PrivilegeCode.UserAdmin      # User management (global only)
    PrivilegeCode.SysAdmin       # System administration (global only)
    PrivilegeCode.DataAdmin      # UDF and SINDEX administration (global only)
    PrivilegeCode.UDFAdmin       # UDF administration (global only, server 6+)
    PrivilegeCode.SIndexAdmin    # Secondary index administration (global only, server 6+)
    PrivilegeCode.Read           # Read data
    PrivilegeCode.Write          # Write data
    PrivilegeCode.ReadWrite      # Read and write data
    PrivilegeCode.ReadWriteUDF   # Read/write through UDFs
    PrivilegeCode.Truncate       # Truncate data (server 6+)
    """)
    
    print("\n3. Privilege Properties:")
    print("""
    privilege = Privilege(PrivilegeCode.Read, "test", "users")
    
    # Access properties
    code = privilege.code           # PrivilegeCode.Read
    namespace = privilege.namespace # "test"
    set_name = privilege.set_name   # "users"
    
    # String representations
    str_repr = str(privilege)       # String representation
    repr_str = repr(privilege)      # Python representation
    as_string = privilege.as_string() # Custom string format
    """)
    
    print("\n4. Common Use Cases:")
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
    ]
    """)
    
    print("\n5. Important Notes:")
    print("""
    - Global privileges (UserAdmin, SysAdmin, etc.) must have None for namespace/set
    - Data privileges (Read, Write, etc.) can be scoped to namespace or set
    - Privilege validation happens on the server side during authentication
    - Some privileges require server version 6+ (UDFAdmin, SIndexAdmin, Truncate)
    - For user management operations, you typically need a TLS-enabled server
    """)


if __name__ == "__main__":
    print("🔐 Privilege and PrivilegeCode Examples")
    print("="*50)
    
    # Run the examples
    privilege()
    
    # Show usage examples
    show_privilege_usage()
