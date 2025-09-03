#!/usr/bin/env python3
"""
Simple example showing how to link a database to the code.
"""

import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from kvstore import Store, SQLiteStorage


def main():
    print("=== Database Connection Example ===\n")
    
    # 1. Simple database connection
    print("1. Creating/connecting to database:")
    db_path = "example.db"
    storage = SQLiteStorage(db_path)
    store = Store(storage)
    
    print(f"   Database file: {db_path}")
    print(f"   File exists: {os.path.exists(db_path)}")
    
    # 2. Add some data
    print("\n2. Adding data to database:")
    store.begin()
    store.set("app_name", "My Application")
    store.set("version", "1.0.0")
    store.set("users", ["Alice", "Bob", "Charlie"])
    store.commit()
    print("   Data added and committed")
    
    # 3. Close and reopen to verify persistence
    store.close()
    print("   Database connection closed")
    
    print("\n3. Reopening database to verify persistence:")
    new_storage = SQLiteStorage(db_path)
    new_store = Store(new_storage)
    
    new_store.begin()
    app_name = new_store.get("app_name")
    version = new_store.get("version")
    users = new_store.get("users")
    
    print(f"   app_name: {app_name}")
    print(f"   version: {version}")
    print(f"   users: {users}")
    
    new_store.rollback()
    new_store.close()
    
    print(f"\n4. Database file size: {os.path.getsize(db_path)} bytes")
    print(f"   Database location: {os.path.abspath(db_path)}")
    
    # Clean up (optional)
    # os.remove(db_path)
    # print(f"   Cleaned up database file")


if __name__ == "__main__":
    main()
