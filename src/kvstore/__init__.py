"""
Transactional Key-Value Store

A Python implementation of a transactional key-value store with support for
nested transactions and persistence.
"""

from .store import Store
from .storage import StorageBackend, SQLiteStorage, InMemoryStorage
from .async_store import AsyncStore
from .async_storage import AsyncStorageBackend, AsyncSQLiteStorage, AsyncInMemoryStorage
from .exceptions import (
    StoreError,
    TransactionError,
    KeyNotFoundError,
    NoActiveTransactionError,
)

__version__ = "0.1.0"
__all__ = [
    "Store",
    "StorageBackend",
    "SQLiteStorage", 
    "InMemoryStorage",
    "AsyncStore",
    "AsyncStorageBackend",
    "AsyncSQLiteStorage",
    "AsyncInMemoryStorage",
    "StoreError", 
    "TransactionError",
    "KeyNotFoundError",
    "NoActiveTransactionError",
]
