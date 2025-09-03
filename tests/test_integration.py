"""
Integration tests for the complete transactional key-value store system.
"""

import pytest
import sys
import os
import tempfile
import json

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from kvstore import Store, SQLiteStorage, InMemoryStorage
from kvstore.exceptions import KeyNotFoundError, NoActiveTransactionError


class TestEndToEndScenarios:
    """Test complete end-to-end scenarios."""
    
    def test_complete_application_lifecycle(self):
        """Test a complete application lifecycle with persistence."""
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        temp_db.close()
        db_path = temp_db.name
        
        try:
            # Application startup - initialize store
            storage = SQLiteStorage(db_path)
            store = Store(storage)
            
            # User session 1 - add initial data
            store.begin()
            store.set("user_count", 0)
            store.set("app_config", {
                "version": "1.0.0",
                "features": ["transactions", "persistence"],
                "debug": False
            })
            store.commit()
            
            # User session 2 - modify data
            store.begin()
            user_count = store.get("user_count")
            store.set("user_count", user_count + 1)
            
            config = store.get("app_config")
            config["debug"] = True
            store.set("app_config", config)
            
            store.set("last_login", "2024-01-15T10:30:00Z")
            store.commit()
            
            # Application shutdown
            store.close()
            
            # Application restart - verify persistence
            new_storage = SQLiteStorage(db_path)
            new_store = Store(new_storage)
            
            new_store.begin()
            assert new_store.get("user_count") == 1
            
            config = new_store.get("app_config")
            assert config["version"] == "1.0.0"
            assert config["debug"] is True
            assert "transactions" in config["features"]
            
            assert new_store.get("last_login") == "2024-01-15T10:30:00Z"
            
            new_store.rollback()
            new_store.close()
            
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)
    
    def test_complex_nested_transaction_scenario(self):
        """Test complex nested transaction scenario."""
        store = Store()
        
        # Simulate a complex business transaction
        store.begin()  # Main transaction
        
        # Initialize account balances
        store.set("account_a", 1000)
        store.set("account_b", 500)
        store.set("account_c", 200)
        
        # Transfer 1: A -> B (100)
        store.begin()  # Transfer transaction 1
        balance_a = store.get("account_a")
        balance_b = store.get("account_b")
        
        store.set("account_a", balance_a - 100)
        store.set("account_b", balance_b + 100)
        store.commit()  # Commit transfer 1
        
        # Transfer 2: B -> C (50) - but this will be rolled back
        store.begin()  # Transfer transaction 2
        balance_b = store.get("account_b")
        balance_c = store.get("account_c")
        
        store.set("account_b", balance_b - 50)
        store.set("account_c", balance_c + 50)
        
        # Simulate error condition - rollback transfer 2
        store.rollback()
        
        # Transfer 3: A -> C (25)
        store.begin()  # Transfer transaction 3
        balance_a = store.get("account_a")
        balance_c = store.get("account_c")
        
        store.set("account_a", balance_a - 25)
        store.set("account_c", balance_c + 25)
        store.commit()  # Commit transfer 3
        
        # Commit main transaction
        store.commit()
        
        # Verify final balances
        committed_data = store._get_committed_data()
        assert committed_data["account_a"] == 875  # 1000 - 100 - 25
        assert committed_data["account_b"] == 600  # 500 + 100
        assert committed_data["account_c"] == 225  # 200 + 25
        
        # Verify total balance is conserved
        total = committed_data["account_a"] + committed_data["account_b"] + committed_data["account_c"]
        assert total == 1700  # Original total
    
    def test_error_recovery_scenario(self):
        """Test error recovery in complex scenarios."""
        store = Store()
        
        # Setup initial state
        store.begin()
        store.set("counter", 0)
        store.set("status", "initialized")
        store.commit()
        
        # Scenario with errors
        store.begin()
        
        # Increment counter
        counter = store.get("counter")
        store.set("counter", counter + 1)
        
        # Try to access nonexistent key (error)
        try:
            store.get("nonexistent_key")
            assert False, "Should have raised KeyNotFoundError"
        except KeyNotFoundError:
            pass  # Expected error
        
        # Store should still be functional
        store.set("status", "processing")
        assert store.get("counter") == 1
        assert store.get("status") == "processing"
        
        # Try to delete nonexistent key (another error)
        try:
            store.delete("another_nonexistent")
            assert False, "Should have raised KeyNotFoundError"
        except KeyNotFoundError:
            pass  # Expected error
        
        # Store should still be functional
        store.set("status", "completed")
        
        # Commit should work despite previous errors
        store.commit()
        
        # Verify final state
        committed_data = store._get_committed_data()
        assert committed_data["counter"] == 1
        assert committed_data["status"] == "completed"
    
    def test_data_type_preservation(self):
        """Test that various data types are preserved correctly."""
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        temp_db.close()
        db_path = temp_db.name
        
        try:
            storage = SQLiteStorage(db_path)
            store = Store(storage)
            
            # Test various data types
            test_data = {
                "string": "Hello, World!",
                "integer": 42,
                "float": 3.14159,
                "boolean_true": True,
                "boolean_false": False,
                "none_value": None,
                "list": [1, 2, "three", None, True],
                "dict": {
                    "nested": {
                        "deep": "value",
                        "number": 123
                    },
                    "array": [1, 2, 3]
                },
                "unicode": "Hello 世界 🌍",
                "empty_string": "",
                "empty_list": [],
                "empty_dict": {}
            }
            
            # Store all test data
            store.begin()
            for key, value in test_data.items():
                store.set(key, value)
            store.commit()
            store.close()
            
            # Restart and verify data preservation
            new_storage = SQLiteStorage(db_path)
            new_store = Store(new_storage)
            
            new_store.begin()
            for key, expected_value in test_data.items():
                actual_value = new_store.get(key)
                assert actual_value == expected_value, f"Data type not preserved for {key}: expected {expected_value}, got {actual_value}"
            
            new_store.rollback()
            new_store.close()
            
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)
    
    def test_concurrent_transaction_simulation(self):
        """Test simulation of concurrent transactions (single-threaded)."""
        store = Store()
        
        # Simulate two concurrent transactions by interleaving operations
        
        # Transaction 1 starts
        tx1 = store.begin()
        store.set("shared_resource", "tx1_initial")
        
        # Transaction 2 starts (nested)
        tx2 = store.begin()
        store.set("shared_resource", "tx2_modified")
        store.set("tx2_only", "tx2_data")
        
        # Transaction 1 continues (but we're in tx2 context)
        # This simulates what would happen if tx1 tried to read
        assert store.get("shared_resource") == "tx2_modified"  # Sees tx2's changes
        
        # Transaction 2 commits
        store.commit()  # tx2 commits
        
        # Now we're back in tx1 context
        assert store.get("shared_resource") == "tx2_modified"  # tx1 sees tx2's committed changes
        assert store.get("tx2_only") == "tx2_data"
        
        # Transaction 1 makes final changes
        store.set("shared_resource", "tx1_final")
        store.set("tx1_only", "tx1_data")
        
        # Transaction 1 commits
        store.commit()
        
        # Verify final state
        committed_data = store._get_committed_data()
        assert committed_data["shared_resource"] == "tx1_final"
        assert committed_data["tx1_only"] == "tx1_data"
        assert committed_data["tx2_only"] == "tx2_data"


class TestStorageBackendIntegration:
    """Test integration between Store and storage backends."""
    
    def test_storage_backend_switching(self):
        """Test switching between storage backends."""
        # Start with in-memory storage
        memory_storage = InMemoryStorage()
        store1 = Store(memory_storage)
        
        store1.begin()
        store1.set("memory_key", "memory_value")
        store1.commit()
        store1.close()
        
        # Switch to SQLite storage
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        temp_db.close()
        db_path = temp_db.name
        
        try:
            sqlite_storage = SQLiteStorage(db_path)
            store2 = Store(sqlite_storage)
            
            store2.begin()
            store2.set("sqlite_key", "sqlite_value")
            store2.commit()
            store2.close()
            
            # Verify each storage has its own data
            memory_data = memory_storage.get_committed_data()
            sqlite_data = sqlite_storage.get_committed_data()
            
            assert "memory_key" in memory_data
            assert "memory_key" not in sqlite_data
            assert "sqlite_key" in sqlite_data
            assert "sqlite_key" not in memory_data
            
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)
    
    def test_storage_backend_error_handling(self):
        """Test error handling in storage backends."""
        # Test with invalid database path (should still work, creates file)
        invalid_path = "/tmp/test_kvstore_integration.db"
        
        try:
            storage = SQLiteStorage(invalid_path)
            store = Store(storage)
            
            # Should work despite "invalid" path
            store.begin()
            store.set("test_key", "test_value")
            store.commit()
            
            # Verify data was stored
            data = storage.get_committed_data()
            assert data["test_key"] == "test_value"
            
            store.close()
            
        finally:
            if os.path.exists(invalid_path):
                os.unlink(invalid_path)
    
    def test_context_manager_integration(self):
        """Test context manager integration with storage backends."""
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        temp_db.close()
        db_path = temp_db.name
        
        try:
            # Test context manager with SQLite storage
            storage = SQLiteStorage(db_path)
            
            with Store(storage) as store:
                store.begin()
                store.set("context_key", "context_value")
                store.commit()
            
            # Verify data was persisted and connection closed properly
            new_storage = SQLiteStorage(db_path)
            with Store(new_storage) as new_store:
                new_store.begin()
                assert new_store.get("context_key") == "context_value"
                new_store.rollback()
            
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)


class TestRealWorldUseCases:
    """Test real-world use case scenarios."""
    
    def test_configuration_management(self):
        """Test using the store for configuration management."""
        store = Store()
        
        # Initialize default configuration
        store.begin()
        default_config = {
            "database": {
                "host": "localhost",
                "port": 5432,
                "name": "myapp"
            },
            "cache": {
                "enabled": True,
                "ttl": 3600
            },
            "features": {
                "new_ui": False,
                "analytics": True
            }
        }
        store.set("config", default_config)
        store.commit()
        
        # Update configuration
        store.begin()
        config = store.get("config")
        
        # Enable new feature
        config["features"]["new_ui"] = True
        
        # Update database settings
        config["database"]["host"] = "prod-db.example.com"
        config["database"]["port"] = 5433
        
        store.set("config", config)
        store.commit()
        
        # Verify configuration
        final_config = store._get_committed_data()["config"]
        assert final_config["features"]["new_ui"] is True
        assert final_config["database"]["host"] == "prod-db.example.com"
        assert final_config["database"]["port"] == 5433
        assert final_config["cache"]["enabled"] is True  # Unchanged
    
    def test_user_session_management(self):
        """Test using the store for user session management."""
        store = Store()
        
        # User login
        store.begin()
        store.set("user_id", "user123")
        store.set("session_data", {
            "login_time": "2024-01-15T10:00:00Z",
            "permissions": ["read", "write"],
            "preferences": {"theme": "dark"}
        })
        store.set("is_authenticated", True)
        store.commit()
        
        # User performs actions
        store.begin()
        session_data = store.get("session_data")
        # Create a copy to avoid mutation issues
        session_data = dict(session_data)
        session_data["last_activity"] = "2024-01-15T10:30:00Z"
        session_data["page_views"] = 5
        store.set("session_data", session_data)
        store.commit()
        
        # User updates preferences (with rollback scenario)
        store.begin()
        session_data = store.get("session_data")
        old_theme = session_data["preferences"]["theme"]
        
        # Create a deep copy to avoid mutation issues
        import copy
        new_session_data = copy.deepcopy(session_data)
        new_session_data["preferences"]["theme"] = "light"
        store.set("session_data", new_session_data)
        
        # Simulate validation failure - rollback
        store.rollback()
        
        # Verify theme wasn't changed
        store.begin()
        session_data = store.get("session_data")
        assert session_data["preferences"]["theme"] == old_theme
        store.rollback()
        
        # User logout
        store.begin()
        store.delete("user_id")
        store.delete("session_data")
        store.set("is_authenticated", False)
        store.commit()
        
        # Verify cleanup
        committed_data = store._get_committed_data()
        assert "user_id" not in committed_data
        assert "session_data" not in committed_data
        assert committed_data["is_authenticated"] is False


if __name__ == "__main__":
    pytest.main([__file__])
