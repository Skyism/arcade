"""
Tests for async Store implementation.
"""

import pytest
import asyncio
import sys
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor
import time

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from kvstore import AsyncStore, AsyncSQLiteStorage, AsyncInMemoryStorage
from kvstore.exceptions import (
    KeyNotFoundError,
    NoActiveTransactionError,
    TransactionError,
)


class TestAsyncStoreBasicOperations:
    """Test basic async store operations."""
    
    @pytest.mark.asyncio
    async def test_async_store_initialization(self):
        """Test async store can be initialized."""
        store = AsyncStore()
        await store.initialize()
        
        assert store is not None
        assert not store.has_active_transaction()
        assert store.get_current_transaction_id() is None
        
        await store.close()
    
    @pytest.mark.asyncio
    async def test_async_set_without_transaction_raises_error(self):
        """Test that async set() raises error without active transaction."""
        store = AsyncStore()
        await store.initialize()
        
        with pytest.raises(NoActiveTransactionError):
            await store.set("key", "value")
        
        await store.close()
    
    @pytest.mark.asyncio
    async def test_async_get_without_transaction_raises_error(self):
        """Test that async get() raises error without active transaction."""
        store = AsyncStore()
        await store.initialize()
        
        with pytest.raises(NoActiveTransactionError):
            await store.get("key")
        
        await store.close()
    
    @pytest.mark.asyncio
    async def test_async_delete_without_transaction_raises_error(self):
        """Test that async delete() raises error without active transaction."""
        store = AsyncStore()
        await store.initialize()
        
        with pytest.raises(NoActiveTransactionError):
            await store.delete("key")
        
        await store.close()


class TestAsyncStoreTransactionLifecycle:
    """Test async transaction lifecycle operations."""
    
    @pytest.mark.asyncio
    async def test_async_begin_transaction(self):
        """Test beginning an async transaction."""
        store = AsyncStore()
        await store.initialize()
        
        tx_id = await store.begin()
        
        assert tx_id is not None
        assert isinstance(tx_id, str)
        assert store.has_active_transaction()
        assert store.get_current_transaction_id() == tx_id
        
        await store.rollback()
        await store.close()
    
    @pytest.mark.asyncio
    async def test_async_commit_without_transaction_raises_error(self):
        """Test that async commit() raises error without active transaction."""
        store = AsyncStore()
        await store.initialize()
        
        with pytest.raises(NoActiveTransactionError):
            await store.commit()
        
        await store.close()
    
    @pytest.mark.asyncio
    async def test_async_rollback_without_transaction_raises_error(self):
        """Test that async rollback() raises error without active transaction."""
        store = AsyncStore()
        await store.initialize()
        
        with pytest.raises(NoActiveTransactionError):
            await store.rollback()
        
        await store.close()


class TestAsyncStoreKeyValueOperations:
    """Test async key-value operations within transactions."""
    
    @pytest.mark.asyncio
    async def test_async_set_and_get_basic(self):
        """Test basic async set and get operations."""
        store = AsyncStore()
        await store.initialize()
        await store.begin()
        
        await store.set("key1", "value1")
        assert await store.get("key1") == "value1"
        
        await store.set("key2", 42)
        assert await store.get("key2") == 42
        
        await store.set("key3", {"nested": "dict"})
        assert await store.get("key3") == {"nested": "dict"}
        
        await store.rollback()
        await store.close()
    
    @pytest.mark.asyncio
    async def test_async_get_nonexistent_key_raises_error(self):
        """Test that getting a nonexistent key raises KeyNotFoundError."""
        store = AsyncStore()
        await store.initialize()
        await store.begin()
        
        with pytest.raises(KeyNotFoundError):
            await store.get("nonexistent")
        
        await store.rollback()
        await store.close()
    
    @pytest.mark.asyncio
    async def test_async_delete_existing_key(self):
        """Test deleting an existing key."""
        store = AsyncStore()
        await store.initialize()
        await store.begin()
        
        await store.set("key", "value")
        assert await store.get("key") == "value"
        
        await store.delete("key")
        with pytest.raises(KeyNotFoundError):
            await store.get("key")
        
        await store.rollback()
        await store.close()


class TestAsyncStoreNestedTransactions:
    """Test async nested transaction behavior."""
    
    @pytest.mark.asyncio
    async def test_async_nested_transaction_basic(self):
        """Test basic async nested transaction functionality."""
        store = AsyncStore()
        await store.initialize()
        
        # Outer transaction
        tx1_id = await store.begin()
        await store.set("key1", "outer_value")
        
        # Inner transaction
        tx2_id = await store.begin()
        await store.set("key2", "inner_value")
        
        # Different transaction IDs
        assert tx1_id != tx2_id
        assert store.get_current_transaction_id() == tx2_id
        
        # Both values should be visible
        assert await store.get("key1") == "outer_value"
        assert await store.get("key2") == "inner_value"
        
        await store.rollback()  # Inner rollback
        await store.rollback()  # Outer rollback
        await store.close()
    
    @pytest.mark.asyncio
    async def test_async_requirements_example_scenario(self):
        """
        Test the exact scenario from requirements with async:
        store.begin()
        store.set("a", 50)
        store.begin()
        store.set("a", 60)
        """
        store = AsyncStore()
        await store.initialize()
        
        # Outer transaction
        await store.begin()
        await store.set("a", 50)
        
        # At this point, a should be 50
        assert await store.get("a") == 50
        
        # Inner transaction
        await store.begin()
        await store.set("a", 60)
        
        # In inner transaction, a should be 60
        assert await store.get("a") == 60
        
        # Test rollback of inner transaction
        await store.rollback()
        
        # After rollback, should see outer value
        assert await store.get("a") == 50
        
        # Commit outer transaction
        await store.commit()
        
        # Value should be committed
        committed_data = await store._get_committed_data()
        assert committed_data["a"] == 50
        
        await store.close()


class TestAsyncStorePersistence:
    """Test async store with persistence."""
    
    @pytest.mark.asyncio
    async def test_async_sqlite_persistence(self):
        """Test async SQLite persistence."""
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        temp_db.close()
        db_path = temp_db.name
        
        try:
            # First store instance
            storage1 = AsyncSQLiteStorage(db_path)
            store1 = AsyncStore(storage1)
            await store1.initialize()
            
            await store1.begin()
            await store1.set("persistent_key", "persistent_value")
            await store1.set("number_key", 123)
            await store1.commit()
            await store1.close()
            
            # Second store instance
            storage2 = AsyncSQLiteStorage(db_path)
            store2 = AsyncStore(storage2)
            await store2.initialize()
            
            await store2.begin()
            assert await store2.get("persistent_key") == "persistent_value"
            assert await store2.get("number_key") == 123
            await store2.rollback()
            await store2.close()
            
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)
    
    @pytest.mark.asyncio
    async def test_async_inmemory_storage(self):
        """Test async in-memory storage."""
        storage = AsyncInMemoryStorage()
        store = AsyncStore(storage)
        await store.initialize()
        
        await store.begin()
        await store.set("memory_key", "memory_value")
        await store.commit()
        
        # Verify data is in storage
        data = await storage.get_committed_data()
        assert data["memory_key"] == "memory_value"
        
        await store.close()


class TestAsyncStoreConcurrency:
    """Test async store concurrency features."""
    
    @pytest.mark.asyncio
    async def test_concurrent_transactions(self):
        """Test concurrent transactions on the same store."""
        store = AsyncStore()
        await store.initialize()
        
        async def transaction_worker(worker_id: int, key_prefix: str):
            """Worker function for concurrent transactions."""
            await store.begin()
            await store.set(f"{key_prefix}_{worker_id}", f"value_{worker_id}")
            
            # Simulate some work
            await asyncio.sleep(0.01)
            
            value = await store.get(f"{key_prefix}_{worker_id}")
            assert value == f"value_{worker_id}"
            
            await store.commit()
            return f"worker_{worker_id}_completed"
        
        # Run multiple concurrent transactions
        tasks = [
            transaction_worker(i, "concurrent")
            for i in range(5)
        ]
        
        results = await asyncio.gather(*tasks)
        
        # All workers should complete successfully
        assert len(results) == 5
        for i, result in enumerate(results):
            assert result == f"worker_{i}_completed"
        
        # Verify all data was committed
        committed_data = await store._get_committed_data()
        for i in range(5):
            assert committed_data[f"concurrent_{i}"] == f"value_{i}"
        
        await store.close()
    
    @pytest.mark.asyncio
    async def test_concurrent_nested_transactions(self):
        """Test concurrent nested transactions."""
        store = AsyncStore()
        await store.initialize()
        
        async def nested_transaction_worker(worker_id: int):
            """Worker with nested transactions."""
            # Outer transaction
            await store.begin()
            await store.set(f"outer_{worker_id}", f"outer_value_{worker_id}")
            
            # Inner transaction
            await store.begin()
            await store.set(f"inner_{worker_id}", f"inner_value_{worker_id}")
            
            # Simulate work
            await asyncio.sleep(0.01)
            
            # Commit inner, rollback outer (or vice versa)
            if worker_id % 2 == 0:
                await store.commit()  # Inner commit
                await store.commit()  # Outer commit
            else:
                await store.rollback()  # Inner rollback
                await store.commit()   # Outer commit
            
            return worker_id
        
        # Run concurrent nested transactions
        tasks = [nested_transaction_worker(i) for i in range(4)]
        results = await asyncio.gather(*tasks)
        
        assert len(results) == 4
        
        # Check final state
        committed_data = await store._get_committed_data()
        
        # Even workers should have both keys
        for i in range(0, 4, 2):
            assert f"outer_{i}" in committed_data
            assert f"inner_{i}" in committed_data
        
        # Odd workers should only have outer keys
        for i in range(1, 4, 2):
            assert f"outer_{i}" in committed_data
            assert f"inner_{i}" not in committed_data
        
        await store.close()
    
    @pytest.mark.asyncio
    async def test_performance_large_concurrent_operations(self):
        """Test performance with large number of concurrent operations."""
        store = AsyncStore()
        await store.initialize()
        
        async def batch_operations(batch_id: int, operations_per_batch: int = 100):
            """Perform batch operations."""
            await store.begin()
            
            # Set operations
            for i in range(operations_per_batch):
                await store.set(f"batch_{batch_id}_key_{i}", f"value_{i}")
            
            # Get operations
            for i in range(operations_per_batch):
                value = await store.get(f"batch_{batch_id}_key_{i}")
                assert value == f"value_{i}"
            
            await store.commit()
            return batch_id
        
        # Measure performance
        start_time = time.time()
        
        # Run 10 concurrent batches of 100 operations each
        tasks = [batch_operations(i, 100) for i in range(10)]
        results = await asyncio.gather(*tasks)
        
        end_time = time.time()
        duration = end_time - start_time
        
        # All batches should complete
        assert len(results) == 10
        assert sorted(results) == list(range(10))
        
        # Performance should be reasonable (1000 operations in reasonable time)
        assert duration < 5.0, f"Operations took too long: {duration}s"
        
        # Verify all data was committed
        committed_data = await store._get_committed_data()
        assert len(committed_data) == 1000  # 10 batches * 100 operations
        
        await store.close()


class TestAsyncStoreContextManager:
    """Test async context manager functionality."""
    
    @pytest.mark.asyncio
    async def test_async_context_manager(self):
        """Test AsyncStore as async context manager."""
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        temp_db.close()
        db_path = temp_db.name
        
        try:
            storage = AsyncSQLiteStorage(db_path)
            
            async with AsyncStore(storage) as store:
                await store.begin()
                await store.set("context_key", "context_value")
                await store.commit()
            
            # Verify data persisted and connection closed properly
            new_storage = AsyncSQLiteStorage(db_path)
            async with AsyncStore(new_storage) as new_store:
                await new_store.begin()
                assert await new_store.get("context_key") == "context_value"
                await new_store.rollback()
                
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)


if __name__ == "__main__":
    pytest.main([__file__])
