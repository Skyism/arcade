"""
Comprehensive tests for the Store class.
"""

import pytest
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from kvstore import Store
from kvstore.exceptions import (
    KeyNotFoundError,
    NoActiveTransactionError,
    TransactionError,
)


class TestStoreBasicOperations:
    """Test basic store operations."""
    
    def test_store_initialization(self):
        """Test store can be initialized."""
        store = Store()
        assert store is not None
        assert not store.has_active_transaction()
        assert store.get_current_transaction_id() is None
    
    def test_set_without_transaction_raises_error(self):
        """Test that set() raises error without active transaction."""
        store = Store()
        with pytest.raises(NoActiveTransactionError):
            store.set("key", "value")
    
    def test_get_without_transaction_raises_error(self):
        """Test that get() raises error without active transaction."""
        store = Store()
        with pytest.raises(NoActiveTransactionError):
            store.get("key")
    
    def test_delete_without_transaction_raises_error(self):
        """Test that delete() raises error without active transaction."""
        store = Store()
        with pytest.raises(NoActiveTransactionError):
            store.delete("key")


class TestStoreTransactionLifecycle:
    """Test transaction lifecycle operations."""
    
    def test_begin_transaction(self):
        """Test beginning a transaction."""
        store = Store()
        tx_id = store.begin()
        
        assert tx_id is not None
        assert isinstance(tx_id, str)
        assert store.has_active_transaction()
        assert store.get_current_transaction_id() == tx_id
    
    def test_commit_without_transaction_raises_error(self):
        """Test that commit() raises error without active transaction."""
        store = Store()
        with pytest.raises(NoActiveTransactionError):
            store.commit()
    
    def test_rollback_without_transaction_raises_error(self):
        """Test that rollback() raises error without active transaction."""
        store = Store()
        with pytest.raises(NoActiveTransactionError):
            store.rollback()


class TestStoreKeyValueOperations:
    """Test key-value operations within transactions."""
    
    def test_set_and_get_basic(self):
        """Test basic set and get operations."""
        store = Store()
        store.begin()
        
        store.set("key1", "value1")
        assert store.get("key1") == "value1"
        
        store.set("key2", 42)
        assert store.get("key2") == 42
        
        store.set("key3", {"nested": "dict"})
        assert store.get("key3") == {"nested": "dict"}
    
    def test_get_nonexistent_key_raises_error(self):
        """Test that getting a nonexistent key raises KeyNotFoundError."""
        store = Store()
        store.begin()
        
        with pytest.raises(KeyNotFoundError):
            store.get("nonexistent")
    
    def test_delete_existing_key(self):
        """Test deleting an existing key."""
        store = Store()
        store.begin()
        
        store.set("key", "value")
        assert store.get("key") == "value"
        
        store.delete("key")
        with pytest.raises(KeyNotFoundError):
            store.get("key")
    
    def test_delete_nonexistent_key_raises_error(self):
        """Test that deleting a nonexistent key raises KeyNotFoundError."""
        store = Store()
        store.begin()
        
        with pytest.raises(KeyNotFoundError):
            store.delete("nonexistent")
    
    def test_overwrite_existing_key(self):
        """Test overwriting an existing key."""
        store = Store()
        store.begin()
        
        store.set("key", "value1")
        assert store.get("key") == "value1"
        
        store.set("key", "value2")
        assert store.get("key") == "value2"


class TestStoreTransactionCommit:
    """Test transaction commit behavior."""
    
    def test_commit_single_transaction(self):
        """Test committing a single transaction."""
        store = Store()
        store.begin()
        
        store.set("key1", "value1")
        store.set("key2", "value2")
        
        store.commit()
        
        # After commit, no active transaction
        assert not store.has_active_transaction()
        
        # Data should be committed
        committed_data = store._get_committed_data()
        assert committed_data["key1"] == "value1"
        assert committed_data["key2"] == "value2"
    
    def test_commit_with_deletions(self):
        """Test committing a transaction with deletions."""
        store = Store()
        
        # First, commit some data
        store.begin()
        store.set("key1", "value1")
        store.set("key2", "value2")
        store.commit()
        
        # Then delete in a new transaction
        store.begin()
        store.delete("key1")
        store.commit()
        
        committed_data = store._get_committed_data()
        assert "key1" not in committed_data
        assert committed_data["key2"] == "value2"


class TestStoreTransactionRollback:
    """Test transaction rollback behavior."""
    
    def test_rollback_single_transaction(self):
        """Test rolling back a single transaction."""
        store = Store()
        store.begin()
        
        store.set("key1", "value1")
        store.set("key2", "value2")
        
        store.rollback()
        
        # After rollback, no active transaction
        assert not store.has_active_transaction()
        
        # Data should not be committed
        committed_data = store._get_committed_data()
        assert len(committed_data) == 0
    
    def test_rollback_preserves_previous_data(self):
        """Test that rollback preserves previously committed data."""
        store = Store()
        
        # First, commit some data
        store.begin()
        store.set("key1", "value1")
        store.commit()
        
        # Then make changes and rollback
        store.begin()
        store.set("key1", "new_value")
        store.set("key2", "value2")
        store.rollback()
        
        # Original data should be preserved
        committed_data = store._get_committed_data()
        assert committed_data["key1"] == "value1"
        assert "key2" not in committed_data


class TestStoreNestedTransactions:
    """Test nested transaction behavior."""
    
    def test_nested_transaction_basic(self):
        """Test basic nested transaction functionality."""
        store = Store()
        
        # Outer transaction
        tx1_id = store.begin()
        store.set("key1", "outer_value")
        
        # Inner transaction
        tx2_id = store.begin()
        store.set("key2", "inner_value")
        
        # Different transaction IDs
        assert tx1_id != tx2_id
        assert store.get_current_transaction_id() == tx2_id
        
        # Both values should be visible
        assert store.get("key1") == "outer_value"
        assert store.get("key2") == "inner_value"
    
    def test_nested_transaction_commit_propagation(self):
        """Test that nested transaction commits propagate to parent."""
        store = Store()
        
        store.begin()
        store.set("key1", "outer_value")
        
        store.begin()
        store.set("key2", "inner_value")
        store.commit()  # Commit inner transaction
        
        # Should still be in outer transaction
        assert store.has_active_transaction()
        
        # Both values should be visible in outer transaction
        assert store.get("key1") == "outer_value"
        assert store.get("key2") == "inner_value"
        
        store.commit()  # Commit outer transaction
        
        # Now data should be committed to store
        committed_data = store._get_committed_data()
        assert committed_data["key1"] == "outer_value"
        assert committed_data["key2"] == "inner_value"
    
    def test_nested_transaction_rollback_isolation(self):
        """Test that nested transaction rollback doesn't affect parent."""
        store = Store()
        
        store.begin()
        store.set("key1", "outer_value")
        
        store.begin()
        store.set("key2", "inner_value")
        store.rollback()  # Rollback inner transaction
        
        # Should still be in outer transaction
        assert store.has_active_transaction()
        
        # Outer value should still be visible
        assert store.get("key1") == "outer_value"
        
        # Inner value should not be visible
        with pytest.raises(KeyNotFoundError):
            store.get("key2")
        
        store.commit()  # Commit outer transaction
        
        committed_data = store._get_committed_data()
        assert committed_data["key1"] == "outer_value"
        assert "key2" not in committed_data


class TestStoreRequirementExample:
    """Test the specific example from the requirements."""
    
    def test_requirement_example_scenario(self):
        """
        Test the exact scenario from requirements:
        store.begin()
        store.set("a", 50)
        store.begin()
        store.set("a", 60)
        """
        store = Store()
        
        # Outer transaction
        store.begin()
        store.set("a", 50)
        
        # At this point, a should be 50
        assert store.get("a") == 50
        
        # Inner transaction
        store.begin()
        store.set("a", 60)
        
        # In inner transaction, a should be 60
        assert store.get("a") == 60
        
        # Test rollback of inner transaction
        store.rollback()
        
        # After rollback, should see outer value
        assert store.get("a") == 50
        
        # Commit outer transaction
        store.commit()
        
        # Value should be committed
        committed_data = store._get_committed_data()
        assert committed_data["a"] == 50
    
    def test_requirement_example_with_inner_commit(self):
        """Test the example scenario with inner transaction commit."""
        store = Store()
        
        store.begin()
        store.set("a", 50)
        
        store.begin()
        store.set("a", 60)
        store.commit()  # Commit inner transaction
        
        # After inner commit, outer transaction should see new value
        assert store.get("a") == 60
        
        store.commit()  # Commit outer transaction
        
        # Final committed value should be 60
        committed_data = store._get_committed_data()
        assert committed_data["a"] == 60


class TestStoreEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_multiple_nested_transactions(self):
        """Test multiple levels of nested transactions."""
        store = Store()
        
        store.begin()  # Level 1
        store.set("key", "level1")
        
        store.begin()  # Level 2
        store.set("key", "level2")
        
        store.begin()  # Level 3
        store.set("key", "level3")
        
        assert store.get("key") == "level3"
        
        store.rollback()  # Rollback level 3
        assert store.get("key") == "level2"
        
        store.commit()  # Commit level 2
        assert store.get("key") == "level2"
        
        store.commit()  # Commit level 1
        
        committed_data = store._get_committed_data()
        assert committed_data["key"] == "level2"
    
    def test_delete_and_recreate_in_transaction(self):
        """Test deleting and recreating a key in the same transaction."""
        store = Store()
        
        # First commit some data
        store.begin()
        store.set("key", "original")
        store.commit()
        
        # Delete and recreate in new transaction
        store.begin()
        store.delete("key")
        
        # Key should not be accessible after deletion
        with pytest.raises(KeyNotFoundError):
            store.get("key")
        
        # Recreate the key
        store.set("key", "new_value")
        assert store.get("key") == "new_value"
        
        store.commit()
        
        committed_data = store._get_committed_data()
        assert committed_data["key"] == "new_value"
    
    def test_empty_string_and_none_values(self):
        """Test storing empty strings and None values."""
        store = Store()
        store.begin()
        
        store.set("empty_string", "")
        store.set("none_value", None)
        store.set("zero", 0)
        store.set("false", False)
        
        assert store.get("empty_string") == ""
        assert store.get("none_value") is None
        assert store.get("zero") == 0
        assert store.get("false") is False
        
        store.commit()
        
        committed_data = store._get_committed_data()
        assert committed_data["empty_string"] == ""
        assert committed_data["none_value"] is None
        assert committed_data["zero"] == 0
        assert committed_data["false"] is False


if __name__ == "__main__":
    pytest.main([__file__])
