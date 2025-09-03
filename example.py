#!/usr/bin/env python3
"""
Example usage of the transactional key-value store.
"""

import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from kvstore import Store


def main():
    """Demonstrate the Store functionality."""
    print("=== Transactional Key-Value Store Demo ===\n")
    
    # Initialize store
    store = Store()
    print("1. Store initialized")
    
    # Basic operations
    print("\n2. Basic operations:")
    store.begin()
    print("   - Transaction started")
    
    store.set("name", "Alice")
    store.set("age", 30)
    print("   - Set name='Alice', age=30")
    
    print(f"   - Get name: {store.get('name')}")
    print(f"   - Get age: {store.get('age')}")
    
    store.commit()
    print("   - Transaction committed")
    
    # Demonstrate the requirements example
    print("\n3. Requirements example (nested transactions):")
    store.begin()
    store.set("a", 50)
    print("   - Outer transaction: set a=50")
    print(f"   - Current value of a: {store.get('a')}")
    
    store.begin()
    store.set("a", 60)
    print("   - Inner transaction: set a=60")
    print(f"   - Current value of a: {store.get('a')}")
    
    print("   - Rolling back inner transaction...")
    store.rollback()
    print(f"   - Value of a after rollback: {store.get('a')}")
    
    print("   - Committing outer transaction...")
    store.commit()
    
    # Verify final state
    print("\n4. Final committed data:")
    committed_data = store._get_committed_data()
    for key, value in committed_data.items():
        print(f"   - {key}: {value}")
    
    # Demonstrate rollback
    print("\n5. Rollback demonstration:")
    store.begin()
    store.set("temp", "temporary_value")
    print("   - Set temp='temporary_value'")
    print(f"   - Current value: {store.get('temp')}")
    
    store.rollback()
    print("   - Transaction rolled back")
    
    # Try to access the rolled back data
    store.begin()
    try:
        value = store.get("temp")
        print(f"   - temp value: {value}")
    except Exception as e:
        print(f"   - temp not found (as expected): {e}")
    store.rollback()
    
    print("\n=== Demo completed successfully! ===")


if __name__ == "__main__":
    main()
