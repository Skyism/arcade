"""
Tests for persistence layer functionality.
"""

import pytest
import os
import tempfile
import sys

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from kvstore import Store, SQLiteStorage, InMemoryStorage
from kvstore.exceptions import KeyNotFoundError, NoActiveTransactionError


class TestSQLiteStorage:
    """Test SQLite storage backend."""
    
    def setup_method(self):
        """Set up test with temporary database."""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_path = self.temp_db.name
        self.storage = SQLiteStorage(self.db_path)
    
    def teardown_method(self):
        """Clean up temporary database."""
        if hasattr(self, 'storage'):
            self.storage.close()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
    
    def test_sqlite_initialization(self):
        """Test SQLite storage initialization."""
        self.storage.initialize()
        
        # Check that tables were created
        cursor = self.storage.connection.cursor()
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name IN ('kv_data', 'transaction_log')
        """)
        tables = [row[0] for row in cursor.fetchall()]
        
        assert 'kv_data' in tables
        assert 'transaction_log' in tables
    
    def test_sqlite_commit_and_retrieve(self):
        """Test committing and retrieving data from SQLite."""
        self.storage.initialize()
        
        # Commit some data
        changes = {"key1": "value1", "key2": 42, "key3": {"nested": "dict"}}
        deletions = set()
        
        self.storage.commit_transaction(changes, deletions)
        
        # Retrieve data
        data = self.storage.get_committed_data()
        
        assert data["key1"] == "value1"
        assert data["key2"] == 42
        assert data["key3"] == {"nested": "dict"}
    
    def test_sqlite_deletions(self):
        """Test deletions in SQLite storage."""
        self.storage.initialize()
        
        # First commit some data
        changes = {"key1": "value1", "key2": "value2"}
        self.storage.commit_transaction(changes, set())
        
        # Then delete one key
        changes = {}
        deletions = {"key1"}
        self.storage.commit_transaction(changes, deletions)
        
        # Check results
        data = self.storage.get_committed_data()
        assert "key1" not in data
        assert data["key2"] == "value2"
    
    def test_sqlite_backup_restore(self):
        """Test backup and restore functionality."""
        self.storage.initialize()
        
        # Add some data
        changes = {"backup_key": "backup_value"}
        self.storage.commit_transaction(changes, set())
        
        # Create backup
        backup_path = self.db_path + ".backup"
        self.storage.backup_data(backup_path)
        
        # Modify original data
        changes = {"backup_key": "modified_value"}
        self.storage.commit_transaction(changes, set())
        
        # Restore from backup
        self.storage.restore_data(backup_path)
        
        # Check restored data
        data = self.storage.get_committed_data()
        assert data["backup_key"] == "backup_value"
        
        # Clean up backup
        os.unlink(backup_path)


class TestInMemoryStorage:
    """Test in-memory storage backend."""
    
    def test_inmemory_basic_operations(self):
        """Test basic in-memory storage operations."""
        storage = InMemoryStorage()
        storage.initialize()
        
        # Commit some data
        changes = {"key1": "value1", "key2": 42}
        deletions = set()
        storage.commit_transaction(changes, deletions)
        
        # Retrieve data
        data = storage.get_committed_data()
        assert data["key1"] == "value1"
        assert data["key2"] == 42
    
    def test_inmemory_deletions(self):
        """Test deletions in in-memory storage."""
        storage = InMemoryStorage()
        storage.initialize()
        
        # Add data
        changes = {"key1": "value1", "key2": "value2"}
        storage.commit_transaction(changes, set())
        
        # Delete one key
        changes = {}
        deletions = {"key1"}
        storage.commit_transaction(changes, deletions)
        
        # Check results
        data = storage.get_committed_data()
        assert "key1" not in data
        assert data["key2"] == "value2"


class TestStoreWithSQLitePersistence:
    """Test Store class with SQLite persistence."""
    
    def setup_method(self):
        """Set up test with temporary database."""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_path = self.temp_db.name
        self.storage = SQLiteStorage(self.db_path)
        self.store = Store(self.storage)
    
    def teardown_method(self):
        """Clean up temporary database."""
        if hasattr(self, 'store'):
            self.store.close()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
    
    def test_persistent_basic_operations(self):
        """Test basic operations with persistence."""
        # Set some data
        self.store.begin()
        self.store.set("persistent_key", "persistent_value")
        self.store.set("number_key", 123)
        self.store.commit()
        
        # Create new store instance with same database
        self.store.close()
        new_storage = SQLiteStorage(self.db_path)
        new_store = Store(new_storage)
        
        # Data should be persisted
        new_store.begin()
        assert new_store.get("persistent_key") == "persistent_value"
        assert new_store.get("number_key") == 123
        new_store.rollback()
        
        new_store.close()
    
    def test_persistent_deletions(self):
        """Test deletions with persistence."""
        # Set and delete data
        self.store.begin()
        self.store.set("temp_key", "temp_value")
        self.store.set("keep_key", "keep_value")
        self.store.commit()
        
        self.store.begin()
        self.store.delete("temp_key")
        self.store.commit()
        
        # Create new store instance
        self.store.close()
        new_storage = SQLiteStorage(self.db_path)
        new_store = Store(new_storage)
        
        # Check persistence
        new_store.begin()
        with pytest.raises(KeyNotFoundError):
            new_store.get("temp_key")
        assert new_store.get("keep_key") == "keep_value"
        new_store.rollback()
        
        new_store.close()
    
    def test_persistent_nested_transactions(self):
        """Test nested transactions with persistence."""
        # Nested transaction scenario
        self.store.begin()
        self.store.set("a", 50)
        
        self.store.begin()
        self.store.set("a", 60)
        self.store.rollback()  # Inner rollback
        
        self.store.commit()  # Outer commit
        
        # Create new store instance
        self.store.close()
        new_storage = SQLiteStorage(self.db_path)
        new_store = Store(new_storage)
        
        # Check persisted value
        new_store.begin()
        assert new_store.get("a") == 50  # Should be outer value
        new_store.rollback()
        
        new_store.close()
    
    def test_rollback_no_persistence(self):
        """Test that rolled back transactions don't persist."""
        # Set data but rollback
        self.store.begin()
        self.store.set("rollback_key", "rollback_value")
        self.store.rollback()
        
        # Create new store instance
        self.store.close()
        new_storage = SQLiteStorage(self.db_path)
        new_store = Store(new_storage)
        
        # Data should not be persisted
        new_store.begin()
        with pytest.raises(KeyNotFoundError):
            new_store.get("rollback_key")
        new_store.rollback()
        
        new_store.close()


class TestStoreWithInMemoryStorage:
    """Test Store class with in-memory storage."""
    
    def test_inmemory_store_operations(self):
        """Test store operations with in-memory storage."""
        storage = InMemoryStorage()
        store = Store(storage)
        
        # Basic operations
        store.begin()
        store.set("memory_key", "memory_value")
        store.commit()
        
        # Verify data is in storage
        data = storage.get_committed_data()
        assert data["memory_key"] == "memory_value"
        
        store.close()


class TestStorageComparison:
    """Test that both storage backends behave identically."""
    
    def setup_method(self):
        """Set up both storage types."""
        # SQLite storage
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_path = self.temp_db.name
        self.sqlite_storage = SQLiteStorage(self.db_path)
        self.sqlite_store = Store(self.sqlite_storage)
        
        # In-memory storage
        self.memory_storage = InMemoryStorage()
        self.memory_store = Store(self.memory_storage)
    
    def teardown_method(self):
        """Clean up resources."""
        if hasattr(self, 'sqlite_store'):
            self.sqlite_store.close()
        if hasattr(self, 'memory_store'):
            self.memory_store.close()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
    
    def test_identical_behavior(self):
        """Test that both storage backends produce identical results."""
        test_operations = [
            ("begin", []),
            ("set", ["key1", "value1"]),
            ("set", ["key2", 42]),
            ("commit", []),
            ("begin", []),
            ("set", ["key3", {"nested": "data"}]),
            ("delete", ["key1"]),
            ("commit", []),
        ]
        
        # Apply same operations to both stores
        for operation, args in test_operations:
            getattr(self.sqlite_store, operation)(*args)
            getattr(self.memory_store, operation)(*args)
        
        # Compare final state
        sqlite_data = self.sqlite_store._get_committed_data()
        memory_data = self.memory_store._get_committed_data()
        
        assert sqlite_data == memory_data
        assert "key1" not in sqlite_data  # Should be deleted
        assert sqlite_data["key2"] == 42
        assert sqlite_data["key3"] == {"nested": "data"}


class TestContextManager:
    """Test context manager functionality."""
    
    def test_store_context_manager(self):
        """Test Store as context manager."""
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        temp_db.close()
        db_path = temp_db.name
        
        try:
            storage = SQLiteStorage(db_path)
            
            with Store(storage) as store:
                store.begin()
                store.set("context_key", "context_value")
                store.commit()
            
            # Verify data persisted and connection closed properly
            new_storage = SQLiteStorage(db_path)
            with Store(new_storage) as new_store:
                new_store.begin()
                assert new_store.get("context_key") == "context_value"
                new_store.rollback()
                
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)


if __name__ == "__main__":
    pytest.main([__file__])
