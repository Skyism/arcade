"""
API views for the transactional key-value store.
"""
import json
from datetime import datetime
from typing import Any, Dict

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.http import JsonResponse
from django.shortcuts import render

from .store_manager import store_manager
from .serializers import (
    SetKeySerializer,
    BatchOperationSerializer,
    TransactionResponseSerializer,
    KeyValueResponseSerializer,
    ErrorResponseSerializer,
    HealthCheckResponseSerializer,
    BatchResponseSerializer,
)

# Import kvstore exceptions
import os
import sys
from django.conf import settings
sys.path.insert(0, os.path.join(settings.BASE_DIR, 'src'))

from kvstore.exceptions import (
    KeyNotFoundError,
    NoActiveTransactionError,
    TransactionError,
)


def test_gui(request):
    """Serve the test GUI."""
    return render(request, 'test_gui.html')


class BaseStoreView(APIView):
    """Base view with common functionality."""
    
    def get_session_key(self) -> str:
        """Get session key for store management."""
        if not self.request.session.session_key:
            self.request.session.create()
        return self.request.session.session_key
    
    def get_store(self):
        """Get store instance for current session."""
        session_key = self.get_session_key()
        return store_manager.get_store(session_key)
    
    def handle_store_error(self, error: Exception) -> Response:
        """Handle store-related errors."""
        if isinstance(error, KeyNotFoundError):
            return Response({
                'error': 'KeyNotFoundError',
                'message': str(error)
            }, status=status.HTTP_404_NOT_FOUND)
        
        elif isinstance(error, NoActiveTransactionError):
            return Response({
                'error': 'NoActiveTransactionError',
                'message': str(error)
            }, status=status.HTTP_400_BAD_REQUEST)
        
        elif isinstance(error, TransactionError):
            return Response({
                'error': 'TransactionError',
                'message': str(error)
            }, status=status.HTTP_400_BAD_REQUEST)
        
        else:
            return Response({
                'error': 'InternalError',
                'message': str(error)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class StoreInitView(BaseStoreView):
    """Initialize store for session."""
    
    def post(self, request) -> Response:
        """Initialize store instance."""
        try:
            store = self.get_store()
            session_key = self.get_session_key()
            
            return Response({
                'message': 'Store initialized successfully',
                'session_id': session_key,
                'status': 'ready'
            }, status=status.HTTP_201_CREATED)
        
        except Exception as e:
            return self.handle_store_error(e)


class HealthCheckView(APIView):
    """Health check endpoint."""
    
    def get(self, request) -> Response:
        """Get health status."""
        try:
            return Response({
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'version': '1.0.0',
                'store_status': 'operational'
            }, status=status.HTTP_200_OK)
        
        except Exception as e:
            return Response({
                'status': 'unhealthy',
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }, status=status.HTTP_503_SERVICE_UNAVAILABLE)


class BeginTransactionView(BaseStoreView):
    """Begin a new transaction."""
    
    def post(self, request) -> Response:
        """Begin transaction."""
        try:
            store = self.get_store()
            transaction_id = store.begin()
            
            return Response({
                'transaction_id': transaction_id,
                'status': 'active',
                'message': 'Transaction started successfully'
            }, status=status.HTTP_201_CREATED)
        
        except Exception as e:
            return self.handle_store_error(e)


class CommitTransactionView(BaseStoreView):
    """Commit current transaction."""
    
    def post(self, request) -> Response:
        """Commit transaction."""
        try:
            store = self.get_store()
            store.commit()
            
            return Response({
                'status': 'committed',
                'message': 'Transaction committed successfully'
            }, status=status.HTTP_200_OK)
        
        except Exception as e:
            return self.handle_store_error(e)


class RollbackTransactionView(BaseStoreView):
    """Rollback current transaction."""
    
    def post(self, request) -> Response:
        """Rollback transaction."""
        try:
            store = self.get_store()
            store.rollback()
            
            return Response({
                'status': 'rolled_back',
                'message': 'Transaction rolled back successfully'
            }, status=status.HTTP_200_OK)
        
        except Exception as e:
            return self.handle_store_error(e)


class TransactionStatusView(BaseStoreView):
    """Get transaction status."""
    
    def get(self, request) -> Response:
        """Get current transaction status."""
        try:
            store = self.get_store()
            has_active = store.has_active_transaction()
            current_id = store.get_current_transaction_id()
            
            return Response({
                'has_active_transaction': has_active,
                'current_transaction_id': current_id,
                'status': 'active' if has_active else 'none'
            }, status=status.HTTP_200_OK)
        
        except Exception as e:
            return self.handle_store_error(e)


class SetKeyView(BaseStoreView):
    """Set a key-value pair."""
    
    def put(self, request) -> Response:
        """Set key-value pair."""
        serializer = SetKeySerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        
        try:
            store = self.get_store()
            key = serializer.validated_data['key']
            value = serializer.validated_data['value']
            
            store.set(key, value)
            
            return Response({
                'key': key,
                'value': value,
                'message': 'Key set successfully'
            }, status=status.HTTP_200_OK)
        
        except Exception as e:
            return self.handle_store_error(e)


class GetKeyView(BaseStoreView):
    """Get a value by key."""
    
    def get(self, request, key: str) -> Response:
        """Get value for key."""
        try:
            store = self.get_store()
            value = store.get(key)
            
            return Response({
                'key': key,
                'value': value
            }, status=status.HTTP_200_OK)
        
        except Exception as e:
            return self.handle_store_error(e)


class DeleteKeyView(BaseStoreView):
    """Delete a key."""
    
    def delete(self, request, key: str) -> Response:
        """Delete key."""
        try:
            store = self.get_store()
            store.delete(key)
            
            return Response({
                'key': key,
                'message': 'Key deleted successfully'
            }, status=status.HTTP_200_OK)
        
        except Exception as e:
            return self.handle_store_error(e)


class BatchOperationView(BaseStoreView):
    """Batch operations."""
    
    def post(self, request) -> Response:
        """Execute batch operations."""
        serializer = BatchOperationSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        
        try:
            store = self.get_store()
            operations = serializer.validated_data['operations']
            results = []
            success_count = 0
            error_count = 0
            
            for op in operations:
                try:
                    op_type = op.get('type')
                    key = op.get('key')
                    
                    if op_type == 'set':
                        value = op.get('value')
                        store.set(key, value)
                        results.append({
                            'type': 'set',
                            'key': key,
                            'status': 'success'
                        })
                        success_count += 1
                    
                    elif op_type == 'get':
                        value = store.get(key)
                        results.append({
                            'type': 'get',
                            'key': key,
                            'value': value,
                            'status': 'success'
                        })
                        success_count += 1
                    
                    elif op_type == 'delete':
                        store.delete(key)
                        results.append({
                            'type': 'delete',
                            'key': key,
                            'status': 'success'
                        })
                        success_count += 1
                    
                    else:
                        results.append({
                            'type': op_type,
                            'key': key,
                            'status': 'error',
                            'error': 'Invalid operation type'
                        })
                        error_count += 1
                
                except Exception as e:
                    results.append({
                        'type': op.get('type'),
                        'key': key,
                        'status': 'error',
                        'error': str(e)
                    })
                    error_count += 1
            
            return Response({
                'results': results,
                'success_count': success_count,
                'error_count': error_count
            }, status=status.HTTP_200_OK)
        
        except Exception as e:
            return self.handle_store_error(e)
