"""
Store manager for handling KV store instances and sessions.
"""
import os
import sys
from typing import Dict, Optional
from django.conf import settings

# Add src to path for kvstore imports
sys.path.insert(0, os.path.join(settings.BASE_DIR, 'src'))

from kvstore import Store, SQLiteStorage
from kvstore.exceptions import (
    KeyNotFoundError,
    NoActiveTransactionError,
    TransactionError,
)


class StoreManager:
    """Manages KV store instances per session."""
    
    _stores: Dict[str, Store] = {}
    
    @classmethod
    def get_store(cls, session_key: str) -> Store:
        """Get or create a store instance for the session."""
        if session_key not in cls._stores:
            # Create new store with SQLite persistence
            storage = SQLiteStorage(settings.KVSTORE_DATABASE_PATH)
            store = Store(storage)
            cls._stores[session_key] = store
        
        return cls._stores[session_key]
    
    @classmethod
    def close_store(cls, session_key: str) -> None:
        """Close and remove a store instance."""
        if session_key in cls._stores:
            store = cls._stores[session_key]
            store.close()
            del cls._stores[session_key]
    
    @classmethod
    def close_all_stores(cls) -> None:
        """Close all store instances."""
        for session_key in list(cls._stores.keys()):
            cls.close_store(session_key)


# Global store manager instance
store_manager = StoreManager()
