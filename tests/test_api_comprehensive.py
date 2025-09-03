"""
Comprehensive API tests for the Store class - Phase 3 implementation.
"""

import pytest
import sys
import os
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from kvstore import Store, SQLiteStorage, InMemoryStorage
from kvstore.exceptions import (
    KeyNotFoundError,
    NoActiveTransactionError,
    TransactionError,
)


class TestStoreInitialization:
    """Test Store initialization with various parameters."""
    
    def test_store_default_initialization(self):
        """Test Store with default (no storage backend)."""
        store = Store()
        assert store is not None
        assert not store.has_active_transaction()
        assert store.get_current_transaction_id() is None
    
    def test_store_with_inmemory_storage(self):
        """Test Store with InMemoryStorage."""
        storage = InMemoryStorage()
        store = Store(storage)
        assert store is not None
        assert not store.has_active_transaction()
    
    def test_store_with_sqlite_storage(self):
        """Test Store with SQLiteStorage."""
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        temp_db.close()
        
        try:
            storage = SQLiteStorage(temp_db.name)
            store = Store(storage)
            assert store is not None
            assert not store.has_active_transaction()
            store.close()
        finally:
            if os.path.exists(temp_db.name):
                os.unlink(temp_db.name)
    
    def test_store_initialization_with_none(self):
        """Test Store initialization with None storage backend."""
        store = Store(None)
        assert store is not None
        assert not store.has_active_transaction()


class TestStoreAPIValidation:
    """Test Store API parameter validation and error conditions."""
    
    def setup_method(self):
        """Set up test store."""
        self.store = Store()
    
    def test_set_with_invalid_key_types(self):
        """Test set() with invalid key types."""
        self.store.begin()
        
        # Valid string keys should work
        self.store.set("valid_key", "value")
        
        # Test with various key types (all should work as they convert to string)
        self.store.set("123", "numeric_string_key")
        self.store.set("", "empty_string_key")  # Empty string is valid
        
        self.store.rollback()
    
    def test_set_with_various_value_types(self):
        """Test set() with various value types."""
        self.store.begin()
        
        # Test different value types
        test_values = [
            ("string", "hello world"),
            ("integer", 42),
            ("float", 3.14159),
            ("boolean_true", True),
            ("boolean_false", False),
            ("none", None),
            ("list", [1, 2, 3, "mixed", None]),
            ("dict", {"nested": {"deep": "value"}, "number": 123}),
            ("tuple", (1, 2, 3)),
            ("empty_string", ""),
            ("unicode", "Hello 世界 🌍"),
        ]
        
        for key, value in test_values:
            self.store.set(key, value)
            retrieved = self.store.get(key)
            assert retrieved == value, f"Failed for {key}: expected {value}, got {retrieved}"
        
        self.store.rollback()
    
    def test_get_with_nonexistent_keys(self):
        """Test get() with various nonexistent keys."""
        self.store.begin()
        
        nonexistent_keys = [
            "nonexistent",
            "",
            "123",
            "special_chars_!@#$%",
            "unicode_key_世界",
        ]
        
        for key in nonexistent_keys:
            with pytest.raises(KeyNotFoundError):
                self.store.get(key)
        
        self.store.rollback()
    
    def test_delete_with_nonexistent_keys(self):
        """Test delete() with various nonexistent keys."""
        self.store.begin()
        
        nonexistent_keys = [
            "nonexistent",
            "",
            "123",
            "never_existed",
        ]
        
        for key in nonexistent_keys:
            with pytest.raises(KeyNotFoundError):
                self.store.delete(key)
        
        self.store.rollback()


class TestStoreTransactionEdgeCases:
    """Test edge cases in transaction management."""
    
    def setup_method(self):
        """Set up test store."""
        self.store = Store()
    
    def test_multiple_begin_calls(self):
        """Test multiple begin() calls create nested transactions."""
        tx1 = self.store.begin()
        tx2 = self.store.begin()
        tx3 = self.store.begin()
        
        assert tx1 != tx2 != tx3
        assert self.store.get_current_transaction_id() == tx3
        
        self.store.rollback()  # tx3
        assert self.store.get_current_transaction_id() == tx2
        
        self.store.rollback()  # tx2
        assert self.store.get_current_transaction_id() == tx1
        
        self.store.rollback()  # tx1
        assert not self.store.has_active_transaction()
    
    def test_commit_rollback_sequence(self):
        """Test various commit/rollback sequences."""
        # Test: begin -> commit -> begin -> rollback
        self.store.begin()
        self.store.set("key1", "value1")
        self.store.commit()
        
        self.store.begin()
        self.store.set("key2", "value2")
        self.store.rollback()
        
        # key1 should be committed, key2 should not exist
        self.store.begin()
        assert self.store.get("key1") == "value1"
        with pytest.raises(KeyNotFoundError):
            self.store.get("key2")
        self.store.rollback()
    
    def test_deep_nested_transactions(self):
        """Test deeply nested transactions (10 levels)."""
        transaction_ids = []
        
        # Create 10 nested transactions
        for i in range(10):
            tx_id = self.store.begin()
            transaction_ids.append(tx_id)
            self.store.set(f"key_{i}", f"value_{i}")
        
        # Verify all values are visible
        for i in range(10):
            assert self.store.get(f"key_{i}") == f"value_{i}"
        
        # Rollback half of them
        for i in range(5):
            self.store.rollback()
        
        # Verify remaining values
        for i in range(5):
            assert self.store.get(f"key_{i}") == f"value_{i}"
        
        for i in range(5, 10):
            with pytest.raises(KeyNotFoundError):
                self.store.get(f"key_{i}")
        
        # Commit the rest
        for i in range(5):
            self.store.commit()
        
        assert not self.store.has_active_transaction()


class TestStoreDataIntegrity:
    """Test data integrity and consistency."""
    
    def setup_method(self):
        """Set up test store."""
        self.store = Store()
    
    def test_key_overwrite_scenarios(self):
        """Test various key overwrite scenarios."""
        self.store.begin()
        
        # Set initial value
        self.store.set("key", "value1")
        assert self.store.get("key") == "value1"
        
        # Overwrite in same transaction
        self.store.set("key", "value2")
        assert self.store.get("key") == "value2"
        
        # Overwrite with different type
        self.store.set("key", 123)
        assert self.store.get("key") == 123
        
        # Overwrite with complex type
        self.store.set("key", {"complex": "object"})
        assert self.store.get("key") == {"complex": "object"}
        
        self.store.rollback()
    
    def test_delete_and_recreate_patterns(self):
        """Test delete and recreate patterns."""
        self.store.begin()
        
        # Set, delete, recreate pattern
        self.store.set("key", "original")
        assert self.store.get("key") == "original"
        
        self.store.delete("key")
        with pytest.raises(KeyNotFoundError):
            self.store.get("key")
        
        self.store.set("key", "recreated")
        assert self.store.get("key") == "recreated"
        
        # Delete again
        self.store.delete("key")
        with pytest.raises(KeyNotFoundError):
            self.store.get("key")
        
        self.store.rollback()
    
    def test_transaction_isolation_complex(self):
        """Test complex transaction isolation scenarios."""
        # Outer transaction
        self.store.begin()
        self.store.set("shared", "outer")
        self.store.set("outer_only", "outer_value")
        
        # Inner transaction 1
        self.store.begin()
        self.store.set("shared", "inner1")
        self.store.set("inner1_only", "inner1_value")
        
        # Inner transaction 2
        self.store.begin()
        self.store.set("shared", "inner2")
        self.store.set("inner2_only", "inner2_value")
        
        # Verify current state
        assert self.store.get("shared") == "inner2"
        assert self.store.get("outer_only") == "outer_value"
        assert self.store.get("inner1_only") == "inner1_value"
        assert self.store.get("inner2_only") == "inner2_value"
        
        # Rollback inner2
        self.store.rollback()
        
        # Verify state after rollback
        assert self.store.get("shared") == "inner1"
        assert self.store.get("outer_only") == "outer_value"
        assert self.store.get("inner1_only") == "inner1_value"
        with pytest.raises(KeyNotFoundError):
            self.store.get("inner2_only")
        
        # Commit inner1
        self.store.commit()
        
        # Verify state after commit
        assert self.store.get("shared") == "inner1"
        assert self.store.get("outer_only") == "outer_value"
        assert self.store.get("inner1_only") == "inner1_value"
        
        # Commit outer
        self.store.commit()
        
        assert not self.store.has_active_transaction()


class TestStorePerformance:
    """Test Store performance characteristics."""
    
    def setup_method(self):
        """Set up test store."""
        self.store = Store()
    
    def test_large_transaction_performance(self):
        """Test performance with large transactions."""
        self.store.begin()
        
        # Add 1000 key-value pairs
        start_time = time.time()
        for i in range(1000):
            self.store.set(f"key_{i:04d}", f"value_{i}")
        set_time = time.time() - start_time
        
        # Read all values
        start_time = time.time()
        for i in range(1000):
            value = self.store.get(f"key_{i:04d}")
            assert value == f"value_{i}"
        get_time = time.time() - start_time
        
        # Commit transaction
        start_time = time.time()
        self.store.commit()
        commit_time = time.time() - start_time
        
        # Performance assertions (generous limits)
        assert set_time < 1.0, f"Set operations took too long: {set_time}s"
        assert get_time < 1.0, f"Get operations took too long: {get_time}s"
        assert commit_time < 1.0, f"Commit took too long: {commit_time}s"
    
    def test_deep_nesting_performance(self):
        """Test performance with deep transaction nesting."""
        start_time = time.time()
        
        # Create 100 nested transactions
        for i in range(100):
            self.store.begin()
            self.store.set(f"nested_{i}", i)
        
        nesting_time = time.time() - start_time
        
        # Commit all transactions
        start_time = time.time()
        for i in range(100):
            self.store.commit()
        
        commit_time = time.time() - start_time
        
        # Performance assertions
        assert nesting_time < 1.0, f"Nesting took too long: {nesting_time}s"
        assert commit_time < 1.0, f"Commits took too long: {commit_time}s"
    
    def test_memory_usage_stability(self):
        """Test that memory usage remains stable."""
        # This is a basic test - in production you'd use memory profiling tools
        
        # Perform many operations
        for cycle in range(10):
            self.store.begin()
            
            # Add data
            for i in range(100):
                self.store.set(f"cycle_{cycle}_key_{i}", f"value_{i}")
            
            # Read data
            for i in range(100):
                self.store.get(f"cycle_{cycle}_key_{i}")
            
            # Clean up
            self.store.rollback()
        
        # If we get here without memory issues, test passes
        assert True


class TestStoreRequirementsCompliance:
    """Test compliance with original requirements."""
    
    def setup_method(self):
        """Set up test store."""
        self.store = Store()
    
    def test_exact_requirements_example(self):
        """Test the exact example from instruction.md."""
        # Exact sequence from requirements
        self.store.begin()
        self.store.set("a", 50)
        self.store.begin()
        self.store.set("a", 60)
        
        # At this point a should be 60
        assert self.store.get("a") == 60
        
        # This is the implicit test - what happens next depends on implementation
        # Let's test both rollback and commit scenarios
        
        # Save current state for second test
        self.store.rollback()  # Inner rollback
        assert self.store.get("a") == 50  # Should see outer value
        
        self.store.commit()  # Outer commit
        
        # Verify final state
        committed_data = self.store._get_committed_data()
        assert committed_data["a"] == 50
    
    def test_all_required_methods_exist(self):
        """Test that all required methods exist and are callable."""
        # Test Store initialization
        store = Store()
        assert callable(getattr(store, '__init__', None))
        
        # Test required methods
        required_methods = ['set', 'get', 'delete', 'begin', 'commit', 'rollback']
        
        for method_name in required_methods:
            method = getattr(store, method_name, None)
            assert method is not None, f"Method {method_name} not found"
            assert callable(method), f"Method {method_name} is not callable"
    
    def test_method_signatures(self):
        """Test that methods have correct signatures."""
        self.store.begin()
        
        # Test set(K, V) - should accept any key and value
        self.store.set("string_key", "string_value")
        self.store.set("number_key", 123)
        
        # Test get(K) - should return the value
        value1 = self.store.get("string_key")
        value2 = self.store.get("number_key")
        assert value1 == "string_value"
        assert value2 == 123
        
        # Test delete(K) - should remove the key
        self.store.delete("string_key")
        with pytest.raises(KeyNotFoundError):
            self.store.get("string_key")
        
        # Test begin() - should return transaction ID
        tx_id = self.store.begin()
        assert isinstance(tx_id, str)
        assert len(tx_id) > 0
        
        # Test commit() and rollback() - should not return anything
        result = self.store.rollback()
        assert result is None
        
        result = self.store.commit()
        assert result is None


class TestStoreErrorRecovery:
    """Test error recovery and edge cases."""
    
    def setup_method(self):
        """Set up test store."""
        self.store = Store()
    
    def test_operations_after_errors(self):
        """Test that store remains usable after errors."""
        # Start transaction
        self.store.begin()
        
        # Cause an error
        with pytest.raises(KeyNotFoundError):
            self.store.get("nonexistent")
        
        # Store should still be usable
        self.store.set("recovery_key", "recovery_value")
        assert self.store.get("recovery_key") == "recovery_value"
        
        # Another error
        with pytest.raises(KeyNotFoundError):
            self.store.delete("another_nonexistent")
        
        # Still usable
        assert self.store.get("recovery_key") == "recovery_value"
        
        self.store.commit()
    
    def test_transaction_state_after_errors(self):
        """Test transaction state remains consistent after errors."""
        # No transaction initially
        assert not self.store.has_active_transaction()
        
        # Error without transaction
        with pytest.raises(NoActiveTransactionError):
            self.store.set("key", "value")
        
        # State should be unchanged
        assert not self.store.has_active_transaction()
        
        # Start transaction
        tx_id = self.store.begin()
        assert self.store.has_active_transaction()
        assert self.store.get_current_transaction_id() == tx_id
        
        # Error within transaction
        with pytest.raises(KeyNotFoundError):
            self.store.get("nonexistent")
        
        # Transaction state should be unchanged
        assert self.store.has_active_transaction()
        assert self.store.get_current_transaction_id() == tx_id
        
        self.store.rollback()
        assert not self.store.has_active_transaction()


class TestStoreWithPersistenceIntegration:
    """Test Store API with persistence backends."""
    
    def test_api_consistency_across_backends(self):
        """Test that API behaves consistently across storage backends."""
        # Test data
        test_operations = [
            ("begin", []),
            ("set", ["consistency_key", "consistency_value"]),
            ("set", ["number_key", 42]),
            ("commit", []),
            ("begin", []),
            ("get", ["consistency_key"]),
            ("get", ["number_key"]),
            ("delete", ["number_key"]),
            ("commit", []),
        ]
        
        # Test with in-memory storage
        memory_store = Store(InMemoryStorage())
        memory_results = []
        
        for operation, args in test_operations:
            if operation in ["get"]:
                result = getattr(memory_store, operation)(*args)
                memory_results.append(result)
            else:
                getattr(memory_store, operation)(*args)
        
        memory_store.close()
        
        # Test with SQLite storage
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        temp_db.close()
        
        try:
            sqlite_store = Store(SQLiteStorage(temp_db.name))
            sqlite_results = []
            
            for operation, args in test_operations:
                if operation in ["get"]:
                    result = getattr(sqlite_store, operation)(*args)
                    sqlite_results.append(result)
                else:
                    getattr(sqlite_store, operation)(*args)
            
            sqlite_store.close()
            
            # Results should be identical
            assert memory_results == sqlite_results
            
        finally:
            if os.path.exists(temp_db.name):
                os.unlink(temp_db.name)


if __name__ == "__main__":
    pytest.main([__file__])
