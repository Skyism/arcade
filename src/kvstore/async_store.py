"""
Async Store class implementation for the transactional key-value store.
"""

from typing import Any, Optional, TYPE_CHECKING
from .async_transaction import AsyncTransactionManager
from .exceptions import (
    KeyNotFoundError,
    NoActiveTransactionError,
    TransactionError,
)

if TYPE_CHECKING:
    from .async_storage import AsyncStorageBackend


class AsyncStore:
    """
    An async transactional key-value store.
    
    Supports basic operations (set, get, delete) within transactions
    with nested transaction support and optional persistence.
    All operations are async and thread-safe.
    
    Example usage:
        # In-memory store
        store = AsyncStore()
        await store.initialize()
        
        # Persistent store with SQLite
        from kvstore.async_storage import AsyncSQLiteStorage
        storage = AsyncSQLiteStorage("mystore.db")
        store = AsyncStore(storage_backend=storage)
        await store.initialize()
        
        await store.begin()
        await store.set("key", "value")
        await store.commit()
        
        # Nested transactions
        await store.begin()
        await store.set("a", 50)
        await store.begin()
        await store.set("a", 60)
        # Inner transaction sees a=60, outer sees a=50 after rollback
    """
    
    def __init__(self, storage_backend: Optional['AsyncStorageBackend'] = None) -> None:
        """
        Initialize the async store.
        
        Args:
            storage_backend: Optional async storage backend for persistence.
                           If None, uses in-memory storage.
        """
        self._transaction_manager = AsyncTransactionManager(storage_backend)
        self._storage_backend = storage_backend
        self._initialized = False
        
    async def initialize(self) -> None:
        """Initialize the async store."""
        if not self._initialized:
            await self._transaction_manager.initialize()
            self._initialized = True
    
    async def set(self, key: str, value: Any) -> None:
        """
        Set a key-value pair in the current transaction.
        
        Args:
            key: The key to set
            value: The value to associate with the key
            
        Raises:
            NoActiveTransactionError: If no transaction is active
        """
        if not self._initialized:
            await self.initialize()
            
        if not self._transaction_manager.has_active_transaction():
            raise NoActiveTransactionError("No active transaction. Call begin() first.")
        
        try:
            await self._transaction_manager.set(key, value)
        except ValueError as e:
            raise TransactionError(str(e))
    
    async def get(self, key: str) -> Any:
        """
        Get the value for a key.
        
        Args:
            key: The key to retrieve
            
        Returns:
            The value associated with the key
            
        Raises:
            KeyNotFoundError: If the key is not found
            NoActiveTransactionError: If no transaction is active
        """
        if not self._initialized:
            await self.initialize()
            
        if not self._transaction_manager.has_active_transaction():
            raise NoActiveTransactionError("No active transaction. Call begin() first.")
        
        try:
            return await self._transaction_manager.get(key)
        except KeyError:
            raise KeyNotFoundError(f"Key '{key}' not found")
    
    async def delete(self, key: str) -> None:
        """
        Delete a key from the store.
        
        Args:
            key: The key to delete
            
        Raises:
            KeyNotFoundError: If the key is not found
            NoActiveTransactionError: If no transaction is active
        """
        if not self._initialized:
            await self.initialize()
            
        if not self._transaction_manager.has_active_transaction():
            raise NoActiveTransactionError("No active transaction. Call begin() first.")
        
        try:
            await self._transaction_manager.delete(key)
        except KeyError:
            raise KeyNotFoundError(f"Key '{key}' not found")
        except ValueError as e:
            raise TransactionError(str(e))
    
    async def begin(self) -> str:
        """
        Begin a new transaction.
        
        Returns:
            The transaction ID
        """
        if not self._initialized:
            await self.initialize()
            
        return await self._transaction_manager.begin()
    
    async def commit(self) -> None:
        """
        Commit the current transaction.
        
        For nested transactions, changes are merged into the parent transaction.
        For top-level transactions, changes are committed to the store and
        persisted if a storage backend is configured.
        
        Raises:
            NoActiveTransactionError: If no transaction is active
        """
        if not self._initialized:
            await self.initialize()
            
        if not self._transaction_manager.has_active_transaction():
            raise NoActiveTransactionError("No active transaction to commit")
        
        try:
            await self._transaction_manager.commit()
        except ValueError as e:
            raise TransactionError(str(e))
    
    async def rollback(self) -> None:
        """
        Rollback the current transaction.
        
        All changes made in the current transaction are discarded.
        
        Raises:
            NoActiveTransactionError: If no transaction is active
        """
        if not self._initialized:
            await self.initialize()
            
        if not self._transaction_manager.has_active_transaction():
            raise NoActiveTransactionError("No active transaction to rollback")
        
        try:
            await self._transaction_manager.rollback()
        except ValueError as e:
            raise TransactionError(str(e))
    
    # Additional utility methods
    
    def has_active_transaction(self) -> bool:
        """
        Check if there's an active transaction.
        
        Returns:
            True if there's an active transaction, False otherwise
        """
        return self._transaction_manager.has_active_transaction()
    
    def get_current_transaction_id(self) -> Optional[str]:
        """
        Get the ID of the current transaction.
        
        Returns:
            The current transaction ID, or None if no transaction is active
        """
        return self._transaction_manager.get_current_transaction_id()
    
    async def _get_committed_data(self) -> dict[str, Any]:
        """
        Get the committed data (for testing purposes).
        
        Returns:
            A copy of the committed data
        """
        if not self._initialized:
            await self.initialize()
            
        committed_data = await self._transaction_manager.get_committed_data()
        return committed_data.copy()
    
    async def close(self) -> None:
        """
        Close the store and its storage backend.
        
        This should be called when the store is no longer needed to
        properly close database connections and clean up resources.
        """
        await self._transaction_manager.close()
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
