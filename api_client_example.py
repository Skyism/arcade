#!/usr/bin/env python3
"""
Example client for the REST API.
"""
import requests
import json


class KVStoreClient:
    """Client for the KV Store REST API."""
    
    def __init__(self, base_url: str = "http://localhost:8000/api"):
        self.base_url = base_url
        self.session = requests.Session()
    
    def init_store(self):
        """Initialize store."""
        response = self.session.post(f"{self.base_url}/store/init/")
        return response.json()
    
    def health_check(self):
        """Check API health."""
        response = self.session.get(f"{self.base_url}/store/health/")
        return response.json()
    
    def begin_transaction(self):
        """Begin a new transaction."""
        response = self.session.post(f"{self.base_url}/store/begin/")
        return response.json()
    
    def commit_transaction(self):
        """Commit current transaction."""
        response = self.session.post(f"{self.base_url}/store/commit/")
        return response.json()
    
    def rollback_transaction(self):
        """Rollback current transaction."""
        response = self.session.post(f"{self.base_url}/store/rollback/")
        return response.json()
    
    def get_transaction_status(self):
        """Get transaction status."""
        response = self.session.get(f"{self.base_url}/store/transaction/status/")
        return response.json()
    
    def set_key(self, key: str, value):
        """Set a key-value pair."""
        data = {"key": key, "value": value}
        response = self.session.put(f"{self.base_url}/store/set/",
                                   json=data)
        return response.json()
    
    def get_key(self, key: str):
        """Get value by key."""
        response = self.session.get(f"{self.base_url}/store/get/{key}/")
        if response.status_code == 404:
            raise KeyError(f"Key '{key}' not found")
        return response.json()
    
    def delete_key(self, key: str):
        """Delete a key."""
        response = self.session.delete(f"{self.base_url}/store/delete/{key}/")
        if response.status_code == 404:
            raise KeyError(f"Key '{key}' not found")
        return response.json()
    
    def batch_operations(self, operations):
        """Execute batch operations."""
        data = {"operations": operations}
        response = self.session.post(f"{self.base_url}/store/batch/",
                                    json=data)
        return response.json()


def main():
    """Demonstrate the REST API client."""
    print("=== KV Store REST API Client Demo ===\n")
    
    client = KVStoreClient()
    
    try:
        # Health check
        print("1. Health check:")
        health = client.health_check()
        print(f"   Status: {health['status']}")
        print(f"   Version: {health['version']}")
        
        # Initialize store
        print("\n2. Initialize store:")
        init_result = client.init_store()
        print(f"   Status: {init_result['status']}")
        print(f"   Session ID: {init_result['session_id']}")
        
        # Begin transaction
        print("\n3. Begin transaction:")
        tx_result = client.begin_transaction()
        print(f"   Transaction ID: {tx_result['transaction_id']}")
        print(f"   Status: {tx_result['status']}")
        
        # Set some keys
        print("\n4. Set key-value pairs:")
        client.set_key("name", "Alice")
        client.set_key("age", 30)
        client.set_key("preferences", {"theme": "dark", "lang": "en"})
        print("   Set name='Alice', age=30, preferences")
        
        # Get keys
        print("\n5. Get values:")
        name = client.get_key("name")
        age = client.get_key("age")
        prefs = client.get_key("preferences")
        print(f"   name: {name['value']}")
        print(f"   age: {age['value']}")
        print(f"   preferences: {prefs['value']}")
        
        # Demonstrate requirements example
        print("\n6. Requirements example (nested transactions):")
        
        # Set a = 50
        client.set_key("a", 50)
        print("   Set a=50")
        
        # Begin nested transaction
        nested_tx = client.begin_transaction()
        print(f"   Started nested transaction: {nested_tx['transaction_id']}")
        
        # Set a = 60
        client.set_key("a", 60)
        print("   Set a=60 in nested transaction")
        
        # Check value
        a_value = client.get_key("a")
        print(f"   Current value of a: {a_value['value']}")
        
        # Rollback nested transaction
        client.rollback_transaction()
        print("   Rolled back nested transaction")
        
        # Check value again
        a_value = client.get_key("a")
        print(f"   Value of a after rollback: {a_value['value']}")
        
        # Batch operations
        print("\n7. Batch operations:")
        batch_ops = [
            {"type": "set", "key": "batch1", "value": "value1"},
            {"type": "set", "key": "batch2", "value": "value2"},
            {"type": "get", "key": "batch1"},
            {"type": "delete", "key": "batch2"}
        ]
        
        batch_result = client.batch_operations(batch_ops)
        print(f"   Success count: {batch_result['success_count']}")
        print(f"   Error count: {batch_result['error_count']}")
        
        # Commit transaction
        print("\n8. Commit transaction:")
        commit_result = client.commit_transaction()
        print(f"   Status: {commit_result['status']}")
        
        # Check final transaction status
        print("\n9. Final transaction status:")
        status = client.get_transaction_status()
        print(f"   Has active transaction: {status['has_active_transaction']}")
        print(f"   Current transaction ID: {status['current_transaction_id']}")
        
        print("\n=== Demo completed successfully! ===")
        
    except requests.exceptions.ConnectionError:
        print("Error: Could not connect to the API server.")
        print("Make sure the Django server is running with: python manage.py runserver")
    
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
