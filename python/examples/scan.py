#!/usr/bin/env python3
"""
Comprehensive scan examples showing different scan patterns.
"""

import asyncio
import os
from aerospike_async import new_client, ClientPolicy, ScanPolicy, PartitionFilter


async def scan_all_records():
    """Scan all records in a namespace/set."""
    
    host = os.environ.get("AEROSPIKE_HOST", "localhost:3000")
    client_policy = ClientPolicy()
    scan_policy = ScanPolicy()
    
    try:
        client = await new_client(client_policy, host)
        print("✅ Connected to Aerospike")
        
        print("\n--- Scanning all records ---")
        recordset = await client.scan(
            policy=scan_policy,
            partition_filter=PartitionFilter.all(),
            namespace="test",
            set_name="users",
            bins=None  # Return all bins
        )
        
        record_count = 0
        while True:
            try:
                record = next(recordset)
                record_count += 1
                print(f"Record {record_count}: {record.bins}")
                
                if record_count >= 3:
                    print("... (showing first 3 records)")
                    break
            except StopIteration:
                break
        
        print(f"Total records scanned: {record_count}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if 'client' in locals() and client is not None:
            await client.close()


async def scan_specific_bins():
    """Scan with specific bins only (more efficient)."""
    
    host = os.environ.get("AEROSPIKE_HOST", "localhost:3000")
    client_policy = ClientPolicy()
    scan_policy = ScanPolicy()
    
    try:
        client = await new_client(client_policy, host)
        print("\n--- Scanning specific bins only ---")
        
        recordset = await client.scan(
            policy=scan_policy,
            partition_filter=PartitionFilter.all(),
            namespace="test",
            set_name="users",
            bins=["name", "age"]  # Only return these bins
        )
        
        record_count = 0
        while True:
            try:
                record = next(recordset)
                record_count += 1
                print(f"Record {record_count}: {record.bins}")
                
                if record_count >= 3:
                    print("... (showing first 3 records)")
                    break
            except StopIteration:
                break
        
        print(f"Total records scanned: {record_count}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if 'client' in locals() and client is not None:
            await client.close()


async def scan_specific_partition():
    """Scan a specific partition."""
    
    host = os.environ.get("AEROSPIKE_HOST", "localhost:3000")
    client_policy = ClientPolicy()
    scan_policy = ScanPolicy()
    
    try:
        client = await new_client(client_policy, host)
        print("\n--- Scanning specific partition ---")
        
        # Scan partition 0
        recordset = await client.scan(
            policy=scan_policy,
            partition_filter=PartitionFilter.by_id(0),
            namespace="test",
            set_name="users",
            bins=None
        )
        
        record_count = 0
        while True:
            try:
                record = next(recordset)
                record_count += 1
                print(f"Record {record_count}: {record.key}")
                
                if record_count >= 3:
                    print("... (showing first 3 records)")
                    break
            except StopIteration:
                break
        
        print(f"Total records in partition 0: {record_count}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if 'client' in locals() and client is not None:
            await client.close()


async def scan_partition_range():
    """Scan a range of partitions."""
    
    host = os.environ.get("AEROSPIKE_HOST", "localhost:3000")
    client_policy = ClientPolicy()
    scan_policy = ScanPolicy()
    
    try:
        client = await new_client(client_policy, host)
        print("\n--- Scanning partition range ---")
        
        # Scan partitions 0-4
        recordset = await client.scan(
            policy=scan_policy,
            partition_filter=PartitionFilter.by_range(0, 5),
            namespace="test",
            set_name="users",
            bins=None
        )
        
        record_count = 0
        while True:
            try:
                record = next(recordset)
                record_count += 1
                print(f"Record {record_count}: {record.key}")
                
                if record_count >= 3:
                    print("... (showing first 3 records)")
                    break
            except StopIteration:
                break
        
        print(f"Total records in partitions 0-4: {record_count}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if 'client' in locals() and client is not None:
            await client.close()


def show_usage_examples():
    """Show usage examples and patterns."""
    
    print("\n" + "="*60)
    print("SCAN USAGE EXAMPLES")
    print("="*60)
    
    print("\n1. Basic scan (all records):")
    print("""
    recordset = await client.scan(
        policy=ScanPolicy(),
        partition_filter=PartitionFilter.all(),
        namespace=\"test\",
        set_name=\"users\",
        bins=None
    )
    
    while True:
        try:
            record = next(recordset)
            print(f\"Record: {record.bins}\")
        except StopIteration:
            break
    """)
    
    print("\n2. Scan specific bins (more efficient):")
    print("""
    recordset = await client.scan(
        policy=ScanPolicy(),
        partition_filter=PartitionFilter.all(),
        namespace=\"test\",
        set_name=\"users\",
        bins=[\"name\", \"age\"]
    )
    """)
    
    print("\n3. Scan specific partition:")
    print("""
    recordset = await client.scan(
        policy=ScanPolicy(),
        partition_filter=PartitionFilter.by_id(0),
        namespace=\"test\",
        set_name=\"users\",
        bins=None
    )
    """)
    
    print("\n4. Scan partition range:")
    print("""
    recordset = await client.scan(
        policy=ScanPolicy(),
        partition_filter=PartitionFilter.by_range(0, 10),
        namespace=\"test\",
        set_name=\"users\",
        bins=None
    )
    """)
    
    print("\n5. Scan by key (specific partition):")
    print("""
    from aerospike_async import Key
    
    key = Key(\"test\", \"users\", \"user123\")
    recordset = await client.scan(
        policy=ScanPolicy(),
        partition_filter=PartitionFilter.by_key(key),
        namespace=\"test\",
        set_name=\"users\",
        bins=None
    )
    """)


if __name__ == "__main__":
    print("🔍 Comprehensive Scan Examples")
    print("="*50)
    
    # Run all scan examples
    asyncio.run(scan_all_records())
    asyncio.run(scan_specific_bins())
    asyncio.run(scan_specific_partition())
    asyncio.run(scan_partition_range())
    
    # Show usage examples
    show_usage_examples()
