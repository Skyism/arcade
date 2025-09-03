"""
Async transaction management for the key-value store.
"""

import asyncio
from enum import Enum
from typing import Dict, Any, Optional, List, TYPE_CHECKING
import uuid

if TYPE_CHECKING:
    from .async_storage import AsyncStorageBackend

from .transaction import TransactionState, Transaction


class AsyncTransactionManager:
    """Manages async transaction stack and provides transaction operations."""
    
    def __init__(self, storage_backend: Optional['AsyncStorageBackend'] = None) -> None:
        self.transaction_stack: List[Transaction] = []
        self.storage_backend = storage_backend
        self._committed_data: Optional[Dict[str, Any]] = None
        self._lock = asyncio.Lock()
        
    async def initialize(self) -> None:
        """Initialize the transaction manager."""
        if self.storage_backend:
            await self.storage_backend.initialize()
            self._committed_data = await self.storage_backend.get_committed_data()
        else:
            self._committed_data = {}
    
    async def get_committed_data(self) -> Dict[str, Any]:
        """Get committed data, loading from storage if needed."""
        if self._committed_data is None:
            if self.storage_backend:
                self._committed_data = await self.storage_backend.get_committed_data()
            else:
                self._committed_data = {}
        return self._committed_data
    
    async def begin(self) -> str:
        """Begin a new transaction and return its ID."""
        async with self._lock:
            parent = self.transaction_stack[-1] if self.transaction_stack else None
            transaction = Transaction(parent)
            self.transaction_stack.append(transaction)
            return transaction.id
    
    async def commit(self) -> None:
        """Commit the current transaction."""
        async with self._lock:
            if not self.transaction_stack:
                raise ValueError("No active transaction to commit")
            
            current_transaction = self.transaction_stack.pop()
            current_transaction.state = TransactionState.COMMITTED
            
            if self.transaction_stack:
                # Nested transaction: merge changes into parent
                parent_transaction = self.transaction_stack[-1]
                
                # Apply changes to parent
                for key, value in current_transaction.changes.items():
                    parent_transaction.set(key, value)
                
                # Apply deletions to parent
                for key in current_transaction.deleted_keys:
                    parent_transaction.delete(key)
            else:
                # Top-level transaction: commit to store
                if self.storage_backend:
                    # Commit to persistent storage
                    await self.storage_backend.commit_transaction(
                        current_transaction.changes,
                        current_transaction.deleted_keys
                    )
                    # Reload committed data from storage
                    self._committed_data = await self.storage_backend.get_committed_data()
                else:
                    # Commit to in-memory storage
                    committed_data = await self.get_committed_data()
                    for key, value in current_transaction.changes.items():
                        committed_data[key] = value
                    
                    for key in current_transaction.deleted_keys:
                        committed_data.pop(key, None)
    
    async def rollback(self) -> None:
        """Rollback the current transaction."""
        async with self._lock:
            if not self.transaction_stack:
                raise ValueError("No active transaction to rollback")
            
            current_transaction = self.transaction_stack.pop()
            current_transaction.state = TransactionState.ROLLED_BACK
            # Changes are simply discarded
    
    async def get(self, key: str) -> Any:
        """Get a value, considering transaction stack."""
        async with self._lock:
            # Check transactions from most recent to oldest
            for transaction in reversed(self.transaction_stack):
                if transaction.is_deleted(key):
                    raise KeyError(f"Key '{key}' not found")
                if transaction.has_key(key):
                    return transaction.get_value(key)
            
            # Check committed data
            if self._committed_data is None:
                if self.storage_backend:
                    self._committed_data = await self.storage_backend.get_committed_data()
                else:
                    self._committed_data = {}
            
            if key in self._committed_data:
                return self._committed_data[key]
            
            raise KeyError(f"Key '{key}' not found")
    
    async def set(self, key: str, value: Any) -> None:
        """Set a value in the current transaction."""
        async with self._lock:
            if not self.transaction_stack:
                raise ValueError("No active transaction")
            
            current_transaction = self.transaction_stack[-1]
            current_transaction.set(key, value)
    
    async def delete(self, key: str) -> None:
        """Delete a key in the current transaction."""
        async with self._lock:
            if not self.transaction_stack:
                raise ValueError("No active transaction")
            
            # Check if key exists (in any transaction or committed data)
            key_found = False
            
            # Check transactions from most recent to oldest
            for transaction in reversed(self.transaction_stack):
                if transaction.is_deleted(key):
                    break  # Already deleted
                if transaction.has_key(key):
                    key_found = True
                    break
            
            # Check committed data if not found in transactions
            if not key_found:
                if self._committed_data is None:
                    if self.storage_backend:
                        self._committed_data = await self.storage_backend.get_committed_data()
                    else:
                        self._committed_data = {}
                
                if key in self._committed_data:
                    key_found = True
            
            if not key_found:
                raise KeyError(f"Key '{key}' not found")
            
            current_transaction = self.transaction_stack[-1]
            current_transaction.delete(key)
    
    def has_active_transaction(self) -> bool:
        """Check if there's an active transaction."""
        return len(self.transaction_stack) > 0
    
    def get_current_transaction_id(self) -> Optional[str]:
        """Get the ID of the current transaction."""
        if self.transaction_stack:
            return self.transaction_stack[-1].id
        return None
    
    async def close(self) -> None:
        """Close the transaction manager and storage backend."""
        if self.storage_backend:
            await self.storage_backend.close()
