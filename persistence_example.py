#!/usr/bin/env python3
"""
Example demonstrating persistence functionality of the transactional key-value store.
"""

import sys
import os
import tempfile

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from kvstore import Store, SQLiteStorage, InMemoryStorage


def demonstrate_sqlite_persistence():
    """Demonstrate SQLite persistence across store instances."""
    print("=== SQLite Persistence Demo ===\n")
    
    # Create temporary database file
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    temp_db.close()
    db_path = temp_db.name
    
    print(f"Using database: {db_path}")
    
    try:
        # First store instance - add data
        print("\n1. First store instance - adding data:")
        storage1 = SQLiteStorage(db_path)
        store1 = Store(storage1)
        
        store1.begin()
        store1.set("user", "Alice")
        store1.set("age", 30)
        store1.set("preferences", {"theme": "dark", "language": "en"})
        store1.commit()
        print("   - Added user='Alice', age=30, preferences")
        
        store1.begin()
        store1.set("session_count", 1)
        store1.commit()
        print("   - Added session_count=1")
        
        store1.close()
        print("   - Closed first store instance")
        
        # Second store instance - read persisted data
        print("\n2. Second store instance - reading persisted data:")
        storage2 = SQLiteStorage(db_path)
        store2 = Store(storage2)
        
        store2.begin()
        user = store2.get("user")
        age = store2.get("age")
        preferences = store2.get("preferences")
        session_count = store2.get("session_count")
        
        print(f"   - user: {user}")
        print(f"   - age: {age}")
        print(f"   - preferences: {preferences}")
        print(f"   - session_count: {session_count}")
        
        # Modify data
        store2.set("session_count", session_count + 1)
        store2.set("last_login", "2024-01-15")
        store2.commit()
        print("   - Updated session_count and added last_login")
        
        store2.close()
        
        # Third store instance - verify updates
        print("\n3. Third store instance - verifying updates:")
        storage3 = SQLiteStorage(db_path)
        store3 = Store(storage3)
        
        store3.begin()
        final_session_count = store3.get("session_count")
        last_login = store3.get("last_login")
        
        print(f"   - session_count: {final_session_count}")
        print(f"   - last_login: {last_login}")
        
        store3.rollback()
        store3.close()
        
    finally:
        # Clean up
        if os.path.exists(db_path):
            os.unlink(db_path)
            print(f"\n   - Cleaned up database: {db_path}")


def demonstrate_nested_transactions_with_persistence():
    """Demonstrate nested transactions with persistence."""
    print("\n=== Nested Transactions with Persistence ===\n")
    
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    temp_db.close()
    db_path = temp_db.name
    
    try:
        storage = SQLiteStorage(db_path)
        store = Store(storage)
        
        print("1. Nested transaction scenario:")
        store.begin()
        store.set("counter", 10)
        print("   - Outer transaction: counter=10")
        
        store.begin()
        store.set("counter", 20)
        print("   - Inner transaction: counter=20")
        print(f"   - Current value: {store.get('counter')}")
        
        store.rollback()  # Rollback inner transaction
        print("   - Rolled back inner transaction")
        print(f"   - Current value: {store.get('counter')}")
        
        store.commit()  # Commit outer transaction
        print("   - Committed outer transaction")
        
        store.close()
        
        # Verify persistence
        print("\n2. Verifying persistence after restart:")
        new_storage = SQLiteStorage(db_path)
        new_store = Store(new_storage)
        
        new_store.begin()
        persisted_value = new_store.get("counter")
        print(f"   - Persisted counter value: {persisted_value}")
        new_store.rollback()
        
        new_store.close()
        
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


def demonstrate_rollback_behavior():
    """Demonstrate that rollbacks don't persist."""
    print("\n=== Rollback Behavior Demo ===\n")
    
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    temp_db.close()
    db_path = temp_db.name
    
    try:
        storage = SQLiteStorage(db_path)
        store = Store(storage)
        
        print("1. Adding data and rolling back:")
        store.begin()
        store.set("temp_data", "should_not_persist")
        store.set("temp_number", 999)
        print("   - Added temp_data and temp_number")
        
        store.rollback()
        print("   - Rolled back transaction")
        
        store.close()
        
        # Verify rollback didn't persist
        print("\n2. Checking if rolled back data persisted:")
        new_storage = SQLiteStorage(db_path)
        new_store = Store(new_storage)
        
        new_store.begin()
        try:
            value = new_store.get("temp_data")
            print(f"   - ERROR: Found temp_data: {value}")
        except Exception as e:
            print(f"   - Correct: temp_data not found ({e})")
        
        try:
            value = new_store.get("temp_number")
            print(f"   - ERROR: Found temp_number: {value}")
        except Exception as e:
            print(f"   - Correct: temp_number not found ({e})")
        
        new_store.rollback()
        new_store.close()
        
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


def compare_storage_backends():
    """Compare in-memory vs SQLite storage."""
    print("\n=== Storage Backend Comparison ===\n")
    
    # In-memory storage
    print("1. In-memory storage:")
    memory_storage = InMemoryStorage()
    memory_store = Store(memory_storage)
    
    memory_store.begin()
    memory_store.set("memory_key", "memory_value")
    memory_store.commit()
    
    print("   - Data stored in memory")
    print(f"   - Storage data: {memory_storage.get_committed_data()}")
    memory_store.close()
    
    # SQLite storage
    print("\n2. SQLite storage:")
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    temp_db.close()
    db_path = temp_db.name
    
    try:
        sqlite_storage = SQLiteStorage(db_path)
        sqlite_store = Store(sqlite_storage)
        
        sqlite_store.begin()
        sqlite_store.set("sqlite_key", "sqlite_value")
        sqlite_store.commit()
        
        print("   - Data stored in SQLite")
        print(f"   - Storage data: {sqlite_storage.get_committed_data()}")
        sqlite_store.close()
        
        print(f"   - Database file exists: {os.path.exists(db_path)}")
        
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


def main():
    """Run all persistence demonstrations."""
    print("=== Transactional Key-Value Store - Persistence Demo ===")
    
    demonstrate_sqlite_persistence()
    demonstrate_nested_transactions_with_persistence()
    demonstrate_rollback_behavior()
    compare_storage_backends()
    
    print("\n=== All persistence demos completed successfully! ===")


if __name__ == "__main__":
    main()
