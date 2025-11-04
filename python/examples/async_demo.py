#!/usr/bin/env python3
"""
Async Aerospike Client Demo

This example demonstrates how to use the Aerospike async client with asyncio.
It creates its own event loop and performs various operations:
- Store data (put)
- Update data (add, append, prepend)
- Retrieve data (get, scan)
- Batch operations
- Error handling

Modeled after Java client async patterns:
Reference: https://aerospike.com/docs/develop/client/java/async/

The Java client uses EventLoops with callback-based listeners, while this Python
example uses Python's native async/await syntax with asyncio.gather() for
concurrent operations, which is the Pythonic equivalent.
"""

import asyncio
import os
from typing import List, Dict, Any

from aerospike_async import (
    new_client,
    Client,
    ClientPolicy,
    Key,
    WritePolicy,
    ReadPolicy,
    ScanPolicy,
    PartitionFilter,
    Record,
)


class AsyncAerospikeDemo:
    """Demonstrates async operations with Aerospike."""

    def __init__(self, host: str = "localhost:3000"):
        """Initialize the demo with connection details."""
        self.host = host
        self.client: Client = None
        self.namespace = "test"
        self.set_name = "async_demo"

    async def connect(self):
        """Establish connection to Aerospike cluster."""
        print(f"Connecting to Aerospike at {self.host}...")
        cp = ClientPolicy()
        self.client = await new_client(cp, self.host)
        print("✓ Connected successfully\n")

    async def disconnect(self):
        """Close the connection."""
        if self.client:
            self.client.close()
            print("✓ Disconnected\n")

    async def store_records(self, count: int = 10):
        """Store multiple records asynchronously."""
        print(f"=== Storing {count} records ===")

        wp = WritePolicy()
        tasks = []

        for i in range(count):
            key = Key(self.namespace, self.set_name, f"user_{i}")
            bins = {
                "id": i,
                "name": f"User {i}",
                "email": f"user{i}@example.com",
                "age": 20 + (i % 50),
                "score": 100 + i,  # Integer for add() operations
                "points": 100.0 + i,  # Float for other operations
                "active": i % 2 == 0,
            }
            tasks.append(self.client.put(wp, key, bins))

        # Execute all put operations concurrently
        await asyncio.gather(*tasks)
        print(f"✓ Stored {count} records\n")

    async def update_records(self, count: int = 5):
        """Update records using various operations."""
        print(f"=== Updating {count} records ===")

        wp = WritePolicy()

        # Update 1: Add to integer bin (add() only works on integers)
        print("  Adding 10 to 'score' bin (integer)...")
        for i in range(count):
            key = Key(self.namespace, self.set_name, f"user_{i}")
            await self.client.add(wp, key, {"score": 10})

        # Update 2: Append to string bin
        print("  Appending ' (verified)' to 'name' bin...")
        for i in range(count):
            key = Key(self.namespace, self.set_name, f"user_{i}")
            await self.client.append(wp, key, {"name": " (verified)"})

        # Update 3: Prepend to string bin
        print("  Prepending '[VIP] ' to 'name' bin...")
        for i in range(count):
            key = Key(self.namespace, self.set_name, f"user_{i}")
            await self.client.prepend(wp, key, {"name": "[VIP] "})

        print(f"✓ Updated {count} records\n")

    async def retrieve_single_record(self, user_id: int) -> Record:
        """Retrieve a single record by key."""
        print(f"=== Retrieving record for user_{user_id} ===")

        rp = ReadPolicy()
        key = Key(self.namespace, self.set_name, f"user_{user_id}")

        record = await self.client.get(rp, key)
        if record:
            print(f"  Key: {record.key}")
            print(f"  Bins: {record.bins}")
            print(f"  Generation: {record.generation}")
            print(f"  TTL: {record.ttl}")
            print("✓ Retrieved record\n")
        else:
            print("✗ Record not found\n")

        return record

    async def retrieve_multiple_records(self, user_ids: List[int]) -> List[Record]:
        """Retrieve multiple records concurrently."""
        print(f"=== Retrieving {len(user_ids)} records concurrently ===")

        rp = ReadPolicy()
        tasks = []

        for user_id in user_ids:
            key = Key(self.namespace, self.set_name, f"user_{user_id}")
            tasks.append(self.client.get(rp, key))

        # Execute all get operations concurrently
        records = await asyncio.gather(*tasks)

        print(f"✓ Retrieved {len([r for r in records if r])} records")
        for i, record in enumerate(records):
            if record:
                print(f"  user_{user_ids[i]}: {record.bins.get('name', 'N/A')}")
        print()

        return records

    async def scan_all_records(self):
        """Scan all records in the set."""
        print("=== Scanning all records ===")

        sp = ScanPolicy()
        pf = PartitionFilter.all()

        records = await self.client.scan(sp, pf, self.namespace, self.set_name, [])

        count = 0
        # Recordset uses synchronous iteration (not async)
        while True:
            try:
                record = next(records)
                count += 1
                if count <= 5:  # Print first 5 records
                    print(f"  {record.key}: {record.bins.get('name', 'N/A')}")
            except StopIteration:
                break

        # Wait for scan to complete
        while records.active:
            await asyncio.sleep(0.1)
        records.close()

        print(f"✓ Scanned {count} records\n")

    async def check_existence(self, user_ids: List[int]):
        """Check if records exist."""
        print(f"=== Checking existence of {len(user_ids)} records ===")

        rp = ReadPolicy()
        tasks = []

        for user_id in user_ids:
            key = Key(self.namespace, self.set_name, f"user_{user_id}")
            tasks.append(self.client.exists(rp, key))

        results = await asyncio.gather(*tasks)

        for user_id, exists in zip(user_ids, results):
            status = "✓" if exists else "✗"
            print(f"  {status} user_{user_id}: {'exists' if exists else 'not found'}")

        print()

    async def batch_operations(self):
        """Demonstrate batch operations with error handling."""
        print("=== Batch operations with error handling ===")

        wp = WritePolicy()
        rp = ReadPolicy()

        # Create a mix of valid and invalid operations
        tasks = []

        # Valid operations
        for i in range(3):
            key = Key(self.namespace, self.set_name, f"batch_user_{i}")
            bins = {"id": i, "batch": True}
            tasks.append(self.client.put(wp, key, bins))

        # Retrieve operations
        for i in range(3):
            key = Key(self.namespace, self.set_name, f"batch_user_{i}")
            tasks.append(self.client.get(rp, key))

        # Execute all operations concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)

        success_count = 0
        error_count = 0

        for result in results:
            if isinstance(result, Exception):
                error_count += 1
                print(f"  ✗ Error: {result}")
            else:
                success_count += 1

        print(f"✓ Batch operations: {success_count} succeeded, {error_count} failed\n")

    async def cleanup(self):
        """Clean up demo records."""
        print("=== Cleaning up demo records ===")

        wp = WritePolicy()

        # Delete all user records
        for i in range(20):
            key = Key(self.namespace, self.set_name, f"user_{i}")
            await self.client.delete(wp, key)

        # Delete batch records
        for i in range(10):
            key = Key(self.namespace, self.set_name, f"batch_user_{i}")
            await self.client.delete(wp, key)

        print("✓ Cleanup complete\n")


async def main():
    """Main async function demonstrating Aerospike operations."""
    # Get host from environment or use default
    host = os.environ.get("AEROSPIKE_HOST", "localhost:3000")

    demo = AsyncAerospikeDemo(host)

    try:
        # Connect to cluster
        await demo.connect()

        # Store data
        await demo.store_records(count=10)

        # Update data
        await demo.update_records(count=5)

        # Retrieve single record
        await demo.retrieve_single_record(user_id=0)

        # Retrieve multiple records concurrently
        await demo.retrieve_multiple_records([1, 2, 3, 4, 5])

        # Check existence
        await demo.check_existence([0, 5, 10, 99])

        # Scan all records
        await demo.scan_all_records()

        # Batch operations
        await demo.batch_operations()

        # Cleanup
        await demo.cleanup()

    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Always disconnect
        await demo.disconnect()


if __name__ == "__main__":
    # Create and run the event loop
    print("=" * 60)
    print("Aerospike Async Client Demo")
    print("=" * 60)
    print()

    # Use asyncio.run() which creates a new event loop
    asyncio.run(main())

    print("=" * 60)
    print("Demo completed")
    print("=" * 60)

