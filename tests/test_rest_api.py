"""
Tests for the REST API layer.
"""
import json
import os
import sys
import tempfile
from django.test import TestCase, Client
from django.urls import reverse
from django.conf import settings

# Configure Django settings for testing
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'kvstore_api.settings')

import django
from django.test.utils import setup_test_environment, teardown_test_environment
from django.test.runner import DiscoverRunner

# Setup Django for testing
if not settings.configured:
    django.setup()

setup_test_environment()


class RestAPITestCase(TestCase):
    """Base test case for REST API tests."""
    
    def setUp(self):
        """Set up test client and session."""
        self.client = Client()
        # Create session
        session = self.client.session
        session.create()
        session.save()
    
    def tearDown(self):
        """Clean up after tests."""
        # Clean up any store instances
        from api.store_manager import store_manager
        store_manager.close_all_stores()


class TestStoreManagement(RestAPITestCase):
    """Test store management endpoints."""
    
    def test_store_init(self):
        """Test store initialization."""
        response = self.client.post('/api/store/init/')
        
        self.assertEqual(response.status_code, 201)
        data = response.json()
        self.assertEqual(data['status'], 'ready')
        self.assertIn('session_id', data)
        self.assertIn('message', data)
    
    def test_health_check(self):
        """Test health check endpoint."""
        response = self.client.get('/api/store/health/')
        
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data['status'], 'healthy')
        self.assertIn('timestamp', data)
        self.assertIn('version', data)
        self.assertEqual(data['store_status'], 'operational')


class TestTransactionManagement(RestAPITestCase):
    """Test transaction management endpoints."""
    
    def setUp(self):
        super().setUp()
        # Initialize store
        self.client.post('/api/store/init/')
    
    def test_begin_transaction(self):
        """Test beginning a transaction."""
        response = self.client.post('/api/store/begin/')
        
        self.assertEqual(response.status_code, 201)
        data = response.json()
        self.assertEqual(data['status'], 'active')
        self.assertIn('transaction_id', data)
        self.assertIn('message', data)
    
    def test_commit_transaction(self):
        """Test committing a transaction."""
        # Begin transaction first
        self.client.post('/api/store/begin/')
        
        # Commit transaction
        response = self.client.post('/api/store/commit/')
        
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data['status'], 'committed')
        self.assertIn('message', data)
    
    def test_rollback_transaction(self):
        """Test rolling back a transaction."""
        # Begin transaction first
        self.client.post('/api/store/begin/')
        
        # Rollback transaction
        response = self.client.post('/api/store/rollback/')
        
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data['status'], 'rolled_back')
        self.assertIn('message', data)
    
    def test_transaction_status(self):
        """Test getting transaction status."""
        # Check status without transaction
        response = self.client.get('/api/store/transaction/status/')
        
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertFalse(data['has_active_transaction'])
        self.assertIsNone(data['current_transaction_id'])
        self.assertEqual(data['status'], 'none')
        
        # Begin transaction and check status
        begin_response = self.client.post('/api/store/begin/')
        tx_id = begin_response.json()['transaction_id']
        
        response = self.client.get('/api/store/transaction/status/')
        
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data['has_active_transaction'])
        self.assertEqual(data['current_transaction_id'], tx_id)
        self.assertEqual(data['status'], 'active')
    
    def test_commit_without_transaction(self):
        """Test committing without active transaction."""
        response = self.client.post('/api/store/commit/')
        
        self.assertEqual(response.status_code, 400)
        data = response.json()
        self.assertEqual(data['error'], 'NoActiveTransactionError')
    
    def test_rollback_without_transaction(self):
        """Test rolling back without active transaction."""
        response = self.client.post('/api/store/rollback/')
        
        self.assertEqual(response.status_code, 400)
        data = response.json()
        self.assertEqual(data['error'], 'NoActiveTransactionError')


class TestKeyValueOperations(RestAPITestCase):
    """Test key-value operations."""
    
    def setUp(self):
        super().setUp()
        # Initialize store and begin transaction
        self.client.post('/api/store/init/')
        self.client.post('/api/store/begin/')
    
    def test_set_key(self):
        """Test setting a key-value pair."""
        data = {
            'key': 'test_key',
            'value': 'test_value'
        }
        
        response = self.client.put('/api/store/set/', 
                                 data=json.dumps(data),
                                 content_type='application/json')
        
        self.assertEqual(response.status_code, 200)
        response_data = response.json()
        self.assertEqual(response_data['key'], 'test_key')
        self.assertEqual(response_data['value'], 'test_value')
        self.assertIn('message', response_data)
    
    def test_set_complex_value(self):
        """Test setting complex data types."""
        data = {
            'key': 'complex_key',
            'value': {
                'nested': {'deep': 'value'},
                'list': [1, 2, 3],
                'number': 42,
                'boolean': True,
                'null': None
            }
        }
        
        response = self.client.put('/api/store/set/',
                                 data=json.dumps(data),
                                 content_type='application/json')
        
        self.assertEqual(response.status_code, 200)
        response_data = response.json()
        self.assertEqual(response_data['key'], 'complex_key')
        self.assertEqual(response_data['value'], data['value'])
    
    def test_get_key(self):
        """Test getting a value by key."""
        # Set a key first
        set_data = {'key': 'get_test', 'value': 'get_value'}
        self.client.put('/api/store/set/',
                       data=json.dumps(set_data),
                       content_type='application/json')
        
        # Get the key
        response = self.client.get('/api/store/get/get_test/')
        
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data['key'], 'get_test')
        self.assertEqual(data['value'], 'get_value')
    
    def test_get_nonexistent_key(self):
        """Test getting a nonexistent key."""
        response = self.client.get('/api/store/get/nonexistent/')
        
        self.assertEqual(response.status_code, 404)
        data = response.json()
        self.assertEqual(data['error'], 'KeyNotFoundError')
        self.assertIn('message', data)
    
    def test_delete_key(self):
        """Test deleting a key."""
        # Set a key first
        set_data = {'key': 'delete_test', 'value': 'delete_value'}
        self.client.put('/api/store/set/',
                       data=json.dumps(set_data),
                       content_type='application/json')
        
        # Delete the key
        response = self.client.delete('/api/store/delete/delete_test/')
        
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data['key'], 'delete_test')
        self.assertIn('message', data)
        
        # Verify key is deleted
        get_response = self.client.get('/api/store/get/delete_test/')
        self.assertEqual(get_response.status_code, 404)
    
    def test_delete_nonexistent_key(self):
        """Test deleting a nonexistent key."""
        response = self.client.delete('/api/store/delete/nonexistent/')
        
        self.assertEqual(response.status_code, 404)
        data = response.json()
        self.assertEqual(data['error'], 'KeyNotFoundError')
    
    def test_operations_without_transaction(self):
        """Test operations without active transaction."""
        # Rollback current transaction
        self.client.post('/api/store/rollback/')
        
        # Try to set key without transaction
        set_data = {'key': 'no_tx', 'value': 'no_tx_value'}
        response = self.client.put('/api/store/set/',
                                 data=json.dumps(set_data),
                                 content_type='application/json')
        
        self.assertEqual(response.status_code, 400)
        data = response.json()
        self.assertEqual(data['error'], 'NoActiveTransactionError')


class TestBatchOperations(RestAPITestCase):
    """Test batch operations."""
    
    def setUp(self):
        super().setUp()
        # Initialize store and begin transaction
        self.client.post('/api/store/init/')
        self.client.post('/api/store/begin/')
    
    def test_batch_operations_success(self):
        """Test successful batch operations."""
        data = {
            'operations': [
                {'type': 'set', 'key': 'batch1', 'value': 'value1'},
                {'type': 'set', 'key': 'batch2', 'value': 'value2'},
                {'type': 'get', 'key': 'batch1'},
                {'type': 'delete', 'key': 'batch2'},
            ]
        }
        
        response = self.client.post('/api/store/batch/',
                                  data=json.dumps(data),
                                  content_type='application/json')
        
        self.assertEqual(response.status_code, 200)
        response_data = response.json()
        
        self.assertEqual(response_data['success_count'], 4)
        self.assertEqual(response_data['error_count'], 0)
        self.assertEqual(len(response_data['results']), 4)
        
        # Check individual results
        results = response_data['results']
        self.assertEqual(results[0]['status'], 'success')
        self.assertEqual(results[1]['status'], 'success')
        self.assertEqual(results[2]['status'], 'success')
        self.assertEqual(results[2]['value'], 'value1')
        self.assertEqual(results[3]['status'], 'success')
    
    def test_batch_operations_with_errors(self):
        """Test batch operations with some errors."""
        data = {
            'operations': [
                {'type': 'set', 'key': 'batch1', 'value': 'value1'},
                {'type': 'get', 'key': 'nonexistent'},  # This will fail
                {'type': 'invalid', 'key': 'batch2'},   # Invalid operation
                {'type': 'get', 'key': 'batch1'},       # This will succeed
            ]
        }
        
        response = self.client.post('/api/store/batch/',
                                  data=json.dumps(data),
                                  content_type='application/json')
        
        self.assertEqual(response.status_code, 200)
        response_data = response.json()
        
        self.assertEqual(response_data['success_count'], 2)
        self.assertEqual(response_data['error_count'], 2)
        self.assertEqual(len(response_data['results']), 4)
        
        # Check results
        results = response_data['results']
        self.assertEqual(results[0]['status'], 'success')
        self.assertEqual(results[1]['status'], 'error')
        self.assertEqual(results[2]['status'], 'error')
        self.assertEqual(results[3]['status'], 'success')


class TestNestedTransactions(RestAPITestCase):
    """Test nested transactions via REST API."""
    
    def setUp(self):
        super().setUp()
        # Initialize store
        self.client.post('/api/store/init/')
    
    def test_requirements_example_via_api(self):
        """Test the requirements example via REST API."""
        # Begin outer transaction
        response1 = self.client.post('/api/store/begin/')
        self.assertEqual(response1.status_code, 201)
        tx1_id = response1.json()['transaction_id']
        
        # Set a = 50
        set_data = {'key': 'a', 'value': 50}
        response2 = self.client.put('/api/store/set/',
                                  data=json.dumps(set_data),
                                  content_type='application/json')
        self.assertEqual(response2.status_code, 200)
        
        # Begin inner transaction
        response3 = self.client.post('/api/store/begin/')
        self.assertEqual(response3.status_code, 201)
        tx2_id = response3.json()['transaction_id']
        self.assertNotEqual(tx1_id, tx2_id)
        
        # Set a = 60
        set_data = {'key': 'a', 'value': 60}
        response4 = self.client.put('/api/store/set/',
                                  data=json.dumps(set_data),
                                  content_type='application/json')
        self.assertEqual(response4.status_code, 200)
        
        # Get a (should be 60)
        response5 = self.client.get('/api/store/get/a/')
        self.assertEqual(response5.status_code, 200)
        self.assertEqual(response5.json()['value'], 60)
        
        # Rollback inner transaction
        response6 = self.client.post('/api/store/rollback/')
        self.assertEqual(response6.status_code, 200)
        
        # Get a (should be 50)
        response7 = self.client.get('/api/store/get/a/')
        self.assertEqual(response7.status_code, 200)
        self.assertEqual(response7.json()['value'], 50)
        
        # Commit outer transaction
        response8 = self.client.post('/api/store/commit/')
        self.assertEqual(response8.status_code, 200)
        
        # Check transaction status
        response9 = self.client.get('/api/store/transaction/status/')
        self.assertEqual(response9.status_code, 200)
        self.assertFalse(response9.json()['has_active_transaction'])


class TestErrorHandling(RestAPITestCase):
    """Test error handling in REST API."""
    
    def setUp(self):
        super().setUp()
        self.client.post('/api/store/init/')
    
    def test_invalid_json(self):
        """Test handling of invalid JSON."""
        response = self.client.put('/api/store/set/',
                                 data='invalid json',
                                 content_type='application/json')
        
        self.assertEqual(response.status_code, 400)
    
    def test_missing_required_fields(self):
        """Test handling of missing required fields."""
        # Missing 'value' field
        data = {'key': 'test_key'}
        response = self.client.put('/api/store/set/',
                                 data=json.dumps(data),
                                 content_type='application/json')
        
        self.assertEqual(response.status_code, 400)
    
    def test_empty_batch_operations(self):
        """Test handling of empty batch operations."""
        data = {'operations': []}
        response = self.client.post('/api/store/batch/',
                                  data=json.dumps(data),
                                  content_type='application/json')
        
        self.assertEqual(response.status_code, 400)


if __name__ == '__main__':
    import unittest
    unittest.main()
