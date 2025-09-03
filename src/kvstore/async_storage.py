"""
Async storage backends for the transactional key-value store.
"""

import aiosqlite
import json
import os
import asyncio
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, List, Tuple
from contextlib import asynccontextmanager


class AsyncStorageBackend(ABC):
    """Abstract base class for async storage backends."""
    
    @abstractmethod
    async def initialize(self) -> None:
        """Initialize the storage backend."""
        pass
    
    @abstractmethod
    async def get_committed_data(self) -> Dict[str, Any]:
        """Get all committed key-value pairs."""
        pass
    
    @abstractmethod
    async def commit_transaction(self, changes: Dict[str, Any], deletions: set[str]) -> None:
        """Commit transaction changes to storage."""
        pass
    
    @abstractmethod
    async def close(self) -> None:
        """Close the storage backend."""
        pass


class AsyncSQLiteStorage(AsyncStorageBackend):
    """Async SQLite-based storage backend."""
    
    def __init__(self, db_path: str = "kvstore_async.db"):
        self.db_path = db_path
        self.connection: Optional[aiosqlite.Connection] = None
        self._lock = asyncio.Lock()
        
    async def initialize(self) -> None:
        """Initialize SQLite database with required tables."""
        async with self._lock:
            if self.connection is None:
                self.connection = await aiosqlite.connect(self.db_path)
                await self.connection.execute("PRAGMA journal_mode=WAL")
                
                # Create tables
                await self.connection.execute("""
                    CREATE TABLE IF NOT EXISTS kv_data (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                await self.connection.execute("""
                    CREATE TABLE IF NOT EXISTS transaction_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        transaction_id TEXT NOT NULL,
                        operation TEXT NOT NULL,
                        key TEXT NOT NULL,
                        value TEXT,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                await self.connection.commit()
    
    async def get_committed_data(self) -> Dict[str, Any]:
        """Get all committed key-value pairs from database."""
        if not self.connection:
            await self.initialize()
            
        async with self._lock:
            cursor = await self.connection.execute("SELECT key, value FROM kv_data")
            rows = await cursor.fetchall()
            
            data = {}
            for key, value_json in rows:
                try:
                    data[key] = json.loads(value_json)
                except json.JSONDecodeError:
                    # Fallback for non-JSON values
                    data[key] = value_json
                    
            return data
    
    async def commit_transaction(self, changes: Dict[str, Any], deletions: set[str]) -> None:
        """Commit transaction changes to SQLite database."""
        if not self.connection:
            await self.initialize()
            
        async with self._lock:
            try:
                # Apply changes
                for key, value in changes.items():
                    value_json = json.dumps(value)
                    await self.connection.execute("""
                        INSERT OR REPLACE INTO kv_data (key, value, updated_at)
                        VALUES (?, ?, CURRENT_TIMESTAMP)
                    """, (key, value_json))
                
                # Apply deletions
                for key in deletions:
                    await self.connection.execute("DELETE FROM kv_data WHERE key = ?", (key,))
                
                await self.connection.commit()
                
            except Exception as e:
                await self.connection.rollback()
                raise RuntimeError(f"Failed to commit transaction: {e}")
    
    async def close(self) -> None:
        """Close the database connection."""
        async with self._lock:
            if self.connection:
                await self.connection.close()
                self.connection = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()


class AsyncInMemoryStorage(AsyncStorageBackend):
    """Async in-memory storage backend for testing."""
    
    def __init__(self):
        self.data: Dict[str, Any] = {}
        self._lock = asyncio.Lock()
    
    async def initialize(self) -> None:
        """Initialize in-memory storage."""
        pass  # Nothing to initialize
    
    async def get_committed_data(self) -> Dict[str, Any]:
        """Get all committed key-value pairs."""
        async with self._lock:
            return self.data.copy()
    
    async def commit_transaction(self, changes: Dict[str, Any], deletions: set[str]) -> None:
        """Commit transaction changes to memory."""
        async with self._lock:
            # Apply changes
            for key, value in changes.items():
                self.data[key] = value
            
            # Apply deletions
            for key in deletions:
                self.data.pop(key, None)
    
    async def close(self) -> None:
        """Close the storage backend."""
        pass  # Nothing to close
