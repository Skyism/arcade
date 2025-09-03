#!/usr/bin/env python3
"""
Example demonstrating async functionality of the transactional key-value store.
"""

import asyncio
import sys
import os
import tempfile
import time

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from kvstore import AsyncStore, AsyncSQLiteStorage, AsyncInMemoryStorage


async def demonstrate_async_basic_operations():
    """Demonstrate basic async operations."""
    print("=== Async Basic Operations Demo ===\n")
    
    # Create async store
    store = AsyncStore()
    await store.initialize()
    
    print("1. Async store initialized")
    
    # Basic operations
    await store.begin()
    print("   - Transaction started")
    
    await store.set("name", "Alice")
    await store.set("age", 30)
    await store.set("preferences", {"theme": "dark", "language": "en"})
    print("   - Set name='Alice', age=30, preferences")
    
    name = await store.get("name")
    age = await store.get("age")
    prefs = await store.get("preferences")
    print(f"   - Get name: {name}")
    print(f"   - Get age: {age}")
    print(f"   - Get preferences: {prefs}")
    
    await store.commit()
    print("   - Transaction committed")
    
    await store.close()


async def demonstrate_async_requirements_example():
    """Demonstrate the requirements example with async."""
    print("\n=== Async Requirements Example (nested transactions) ===\n")
    
    store = AsyncStore()
    await store.initialize()
    
    # Requirements example
    await store.begin()
    await store.set("a", 50)
    print("   - Outer transaction: set a=50")
    print(f"   - Current value of a: {await store.get('a')}")
    
    await store.begin()
    await store.set("a", 60)
    print("   - Inner transaction: set a=60")
    print(f"   - Current value of a: {await store.get('a')}")
    
    print("   - Rolling back inner transaction...")
    await store.rollback()
    print(f"   - Value of a after rollback: {await store.get('a')}")
    
    print("   - Committing outer transaction...")
    await store.commit()
    
    # Verify final state
    committed_data = await store._get_committed_data()
    print(f"   - Final committed value: {committed_data['a']}")
    
    await store.close()


async def demonstrate_async_persistence():
    """Demonstrate async persistence."""
    print("\n=== Async Persistence Demo ===\n")
    
    # Create temporary database
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    temp_db.close()
    db_path = temp_db.name
    
    try:
        print(f"Using database: {db_path}")
        
        # First store instance - add data
        print("\n1. First store instance - adding data:")
        storage1 = AsyncSQLiteStorage(db_path)
        async with AsyncStore(storage1) as store1:
            await store1.begin()
            await store1.set("user", "Bob")
            await store1.set("session_count", 1)
            await store1.set("data", {"async": True, "version": "2.0"})
            await store1.commit()
            print("   - Added user='Bob', session_count=1, data")
        
        # Second store instance - read persisted data
        print("\n2. Second store instance - reading persisted data:")
        storage2 = AsyncSQLiteStorage(db_path)
        async with AsyncStore(storage2) as store2:
            await store2.begin()
            user = await store2.get("user")
            session_count = await store2.get("session_count")
            data = await store2.get("data")
            
            print(f"   - user: {user}")
            print(f"   - session_count: {session_count}")
            print(f"   - data: {data}")
            
            # Modify data
            await store2.set("session_count", session_count + 1)
            await store2.set("last_access", "2024-01-15T15:30:00Z")
            await store2.commit()
            print("   - Updated session_count and added last_access")
        
        # Third store instance - verify updates
        print("\n3. Third store instance - verifying updates:")
        storage3 = AsyncSQLiteStorage(db_path)
        async with AsyncStore(storage3) as store3:
            await store3.begin()
            final_session_count = await store3.get("session_count")
            last_access = await store3.get("last_access")
            
            print(f"   - session_count: {final_session_count}")
            print(f"   - last_access: {last_access}")
            await store3.rollback()
        
    finally:
        # Clean up
        if os.path.exists(db_path):
            os.unlink(db_path)
            print(f"\n   - Cleaned up database: {db_path}")


async def demonstrate_concurrent_transactions():
    """Demonstrate concurrent transactions."""
    print("\n=== Concurrent Transactions Demo ===\n")
    
    store = AsyncStore()
    await store.initialize()
    
    async def worker(worker_id: int, operation_count: int = 50):
        """Worker function for concurrent operations."""
        await store.begin()
        
        # Set operations
        for i in range(operation_count):
            await store.set(f"worker_{worker_id}_key_{i}", f"value_{i}")
        
        # Simulate some processing time
        await asyncio.sleep(0.01)
        
        # Get operations to verify
        for i in range(operation_count):
            value = await store.get(f"worker_{worker_id}_key_{i}")
            assert value == f"value_{i}"
        
        await store.commit()
        return f"Worker {worker_id} completed {operation_count} operations"
    
    print("1. Starting concurrent transactions...")
    start_time = time.time()
    
    # Run 5 concurrent workers
    tasks = [worker(i, 50) for i in range(5)]
    results = await asyncio.gather(*tasks)
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"2. Completed in {duration:.3f} seconds")
    for result in results:
        print(f"   - {result}")
    
    # Verify final state
    committed_data = await store._get_committed_data()
    total_keys = len(committed_data)
    print(f"3. Total keys committed: {total_keys}")
    
    await store.close()


async def demonstrate_nested_concurrent_transactions():
    """Demonstrate nested concurrent transactions."""
    print("\n=== Nested Concurrent Transactions Demo ===\n")
    
    store = AsyncStore()
    await store.initialize()
    
    async def nested_worker(worker_id: int):
        """Worker with nested transactions."""
        # Outer transaction
        await store.begin()
        await store.set(f"outer_{worker_id}", f"outer_value_{worker_id}")
        
        # Inner transaction
        await store.begin()
        await store.set(f"inner_{worker_id}", f"inner_value_{worker_id}")
        
        # Simulate processing
        await asyncio.sleep(0.005)
        
        # Commit or rollback inner based on worker ID
        if worker_id % 2 == 0:
            await store.commit()  # Inner commit
            print(f"   - Worker {worker_id}: committed inner transaction")
        else:
            await store.rollback()  # Inner rollback
            print(f"   - Worker {worker_id}: rolled back inner transaction")
        
        # Always commit outer
        await store.commit()
        return worker_id
    
    print("1. Starting nested concurrent transactions...")
    
    # Run concurrent nested transactions
    tasks = [nested_worker(i) for i in range(6)]
    results = await asyncio.gather(*tasks)
    
    print("2. All workers completed")
    
    # Check final state
    committed_data = await store._get_committed_data()
    
    print("3. Final state analysis:")
    outer_keys = [k for k in committed_data.keys() if k.startswith('outer_')]
    inner_keys = [k for k in committed_data.keys() if k.startswith('inner_')]
    
    print(f"   - Outer keys: {len(outer_keys)} (should be 6)")
    print(f"   - Inner keys: {len(inner_keys)} (should be 3 - even workers only)")
    
    # Verify pattern
    for i in range(6):
        assert f"outer_{i}" in committed_data
        if i % 2 == 0:
            assert f"inner_{i}" in committed_data
        else:
            assert f"inner_{i}" not in committed_data
    
    print("   - Pattern verified: even workers committed inner, odd workers rolled back inner")
    
    await store.close()


async def main():
    """Run all async demonstrations."""
    print("=== Async Transactional Key-Value Store Demo ===")
    
    await demonstrate_async_basic_operations()
    await demonstrate_async_requirements_example()
    await demonstrate_async_persistence()
    await demonstrate_concurrent_transactions()
    await demonstrate_nested_concurrent_transactions()
    
    print("\n=== All async demos completed successfully! ===")


if __name__ == "__main__":
    asyncio.run(main())
