"""
Test the specific example from requirements (a=50, nested a=60).
"""

import pytest
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from kvstore import Store


class TestRequirementsExample:
    """Test the exact scenario from instruction.md."""
    
    def test_requirements_example_step_by_step(self):
        """
        Test the exact sequence from instruction.md:
        store.begin()
        store.set("a", 50)
        store.begin()
        store.set("a", 60)
        """
        store = Store()
        
        # Step 1: store.begin()
        tx1_id = store.begin()
        assert store.has_active_transaction()
        assert store.get_current_transaction_id() == tx1_id
        
        # Step 2: store.set("a", 50)
        store.set("a", 50)
        assert store.get("a") == 50
        
        # Step 3: store.begin() (nested)
        tx2_id = store.begin()
        assert store.has_active_transaction()
        assert store.get_current_transaction_id() == tx2_id
        assert tx1_id != tx2_id  # Different transaction IDs
        
        # Step 4: store.set("a", 60)
        store.set("a", 60)
        assert store.get("a") == 60  # Inner transaction sees new value
        
        # At this point, the requirements example ends
        # Let's test what happens with different operations
        
        # Test inner rollback scenario
        store.rollback()  # Rollback inner transaction
        assert store.get("a") == 50  # Should see outer transaction value
        assert store.get_current_transaction_id() == tx1_id  # Back to outer transaction
        
        # Test outer commit
        store.commit()  # Commit outer transaction
        assert not store.has_active_transaction()
        
        # Verify final committed state
        committed_data = store._get_committed_data()
        assert committed_data["a"] == 50
    
    def test_requirements_example_with_inner_commit(self):
        """Test the example with inner transaction commit instead of rollback."""
        store = Store()
        
        # Same setup as requirements
        store.begin()
        store.set("a", 50)
        store.begin()
        store.set("a", 60)
        
        # But commit inner transaction instead of rollback
        store.commit()  # Commit inner transaction
        
        # Outer transaction should now see the inner value
        assert store.get("a") == 60
        
        # Commit outer transaction
        store.commit()
        
        # Final state should have inner value
        committed_data = store._get_committed_data()
        assert committed_data["a"] == 60
    
    def test_requirements_example_multiple_keys(self):
        """Test the example pattern with multiple keys."""
        store = Store()
        
        # Outer transaction with multiple keys
        store.begin()
        store.set("a", 50)
        store.set("b", "outer")
        store.set("c", [1, 2, 3])
        
        # Inner transaction modifies some keys
        store.begin()
        store.set("a", 60)  # Modify existing
        store.set("d", "inner_only")  # Add new
        # Leave "b" and "c" unchanged
        
        # Verify inner transaction state
        assert store.get("a") == 60
        assert store.get("b") == "outer"  # Inherited from outer
        assert store.get("c") == [1, 2, 3]  # Inherited from outer
        assert store.get("d") == "inner_only"
        
        # Rollback inner transaction
        store.rollback()
        
        # Verify outer transaction state
        assert store.get("a") == 50  # Back to outer value
        assert store.get("b") == "outer"
        assert store.get("c") == [1, 2, 3]
        
        # Inner-only key should not exist
        with pytest.raises(Exception):  # KeyNotFoundError
            store.get("d")
        
        # Commit outer transaction
        store.commit()
        
        # Verify final state
        committed_data = store._get_committed_data()
        assert committed_data["a"] == 50
        assert committed_data["b"] == "outer"
        assert committed_data["c"] == [1, 2, 3]
        assert "d" not in committed_data
    
    def test_requirements_example_with_deletions(self):
        """Test the example pattern with deletions."""
        store = Store()
        
        # Setup initial data
        store.begin()
        store.set("a", 50)
        store.set("to_delete", "will_be_deleted")
        store.commit()
        
        # Requirements example with deletions
        store.begin()
        store.set("a", 50)  # Same as before
        
        store.begin()
        store.set("a", 60)  # Modify
        store.delete("to_delete")  # Delete
        
        # Verify inner state
        assert store.get("a") == 60
        with pytest.raises(Exception):  # KeyNotFoundError
            store.get("to_delete")
        
        # Rollback inner transaction
        store.rollback()
        
        # Verify outer state (deletion should be undone)
        assert store.get("a") == 50
        assert store.get("to_delete") == "will_be_deleted"
        
        store.commit()
    
    def test_requirements_example_deep_nesting(self):
        """Test the example pattern with deeper nesting."""
        store = Store()
        
        # Level 1
        store.begin()
        store.set("a", 50)
        
        # Level 2 (like requirements example)
        store.begin()
        store.set("a", 60)
        
        # Level 3 (additional nesting)
        store.begin()
        store.set("a", 70)
        
        # Verify deepest level
        assert store.get("a") == 70
        
        # Rollback level 3
        store.rollback()
        assert store.get("a") == 60  # Back to level 2
        
        # Rollback level 2
        store.rollback()
        assert store.get("a") == 50  # Back to level 1
        
        # Commit level 1
        store.commit()
        
        # Verify final state
        committed_data = store._get_committed_data()
        assert committed_data["a"] == 50
    
    def test_requirements_example_error_conditions(self):
        """Test error conditions in the requirements example scenario."""
        store = Store()
        
        # Test operations without transaction
        with pytest.raises(Exception):  # NoActiveTransactionError
            store.set("a", 50)
        
        with pytest.raises(Exception):  # NoActiveTransactionError
            store.get("a")
        
        with pytest.raises(Exception):  # NoActiveTransactionError
            store.commit()
        
        with pytest.raises(Exception):  # NoActiveTransactionError
            store.rollback()
        
        # Start proper sequence
        store.begin()
        store.set("a", 50)
        
        # Test getting nonexistent key
        with pytest.raises(Exception):  # KeyNotFoundError
            store.get("nonexistent")
        
        # Test deleting nonexistent key
        with pytest.raises(Exception):  # KeyNotFoundError
            store.delete("nonexistent")
        
        # Transaction should still be valid after errors
        assert store.has_active_transaction()
        assert store.get("a") == 50
        
        store.rollback()


if __name__ == "__main__":
    pytest.main([__file__])
