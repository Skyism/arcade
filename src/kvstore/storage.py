"""
Storage backends for the transactional key-value store.
"""

import sqlite3
import json
import os
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, List, Tuple
from contextlib import contextmanager


class StorageBackend(ABC):
    """Abstract base class for storage backends."""
    
    @abstractmethod
    def initialize(self) -> None:
        """Initialize the storage backend."""
        pass
    
    @abstractmethod
    def get_committed_data(self) -> Dict[str, Any]:
        """Get all committed key-value pairs."""
        pass
    
    @abstractmethod
    def commit_transaction(self, changes: Dict[str, Any], deletions: set[str]) -> None:
        """Commit transaction changes to storage."""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close the storage backend."""
        pass


class SQLiteStorage(StorageBackend):
    """SQLite-based storage backend."""
    
    def __init__(self, db_path: str = "kvstore.db"):
        self.db_path = db_path
        self.connection: Optional[sqlite3.Connection] = None
        
    def initialize(self) -> None:
        """Initialize SQLite database with required tables."""
        self.connection = sqlite3.connect(self.db_path)
        self.connection.execute("PRAGMA journal_mode=WAL")  # Enable WAL mode
        
        # Create tables
        self.connection.execute("""
            CREATE TABLE IF NOT EXISTS kv_data (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        self.connection.execute("""
            CREATE TABLE IF NOT EXISTS transaction_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                transaction_id TEXT NOT NULL,
                operation TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        self.connection.commit()
    
    def get_committed_data(self) -> Dict[str, Any]:
        """Get all committed key-value pairs from database."""
        if not self.connection:
            self.initialize()
            
        cursor = self.connection.cursor()
        cursor.execute("SELECT key, value FROM kv_data")
        
        data = {}
        for key, value_json in cursor.fetchall():
            try:
                data[key] = json.loads(value_json)
            except json.JSONDecodeError:
                # Fallback for non-JSON values
                data[key] = value_json
                
        return data
    
    def commit_transaction(self, changes: Dict[str, Any], deletions: set[str]) -> None:
        """Commit transaction changes to SQLite database."""
        if not self.connection:
            self.initialize()
            
        cursor = self.connection.cursor()
        
        try:
            # Apply changes
            for key, value in changes.items():
                value_json = json.dumps(value)
                cursor.execute("""
                    INSERT OR REPLACE INTO kv_data (key, value, updated_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                """, (key, value_json))
            
            # Apply deletions
            for key in deletions:
                cursor.execute("DELETE FROM kv_data WHERE key = ?", (key,))
            
            self.connection.commit()
            
        except Exception as e:
            self.connection.rollback()
            raise RuntimeError(f"Failed to commit transaction: {e}")
    
    def log_transaction_operation(self, transaction_id: str, operation: str, 
                                key: str, value: Any = None) -> None:
        """Log transaction operation for recovery purposes."""
        if not self.connection:
            self.initialize()
            
        cursor = self.connection.cursor()
        value_json = json.dumps(value) if value is not None else None
        
        cursor.execute("""
            INSERT INTO transaction_log (transaction_id, operation, key, value)
            VALUES (?, ?, ?, ?)
        """, (transaction_id, operation, key, value_json))
        
        self.connection.commit()
    
    def get_transaction_log(self, transaction_id: Optional[str] = None) -> List[Tuple]:
        """Get transaction log entries."""
        if not self.connection:
            self.initialize()
            
        cursor = self.connection.cursor()
        
        if transaction_id:
            cursor.execute("""
                SELECT transaction_id, operation, key, value, timestamp
                FROM transaction_log
                WHERE transaction_id = ?
                ORDER BY id
            """, (transaction_id,))
        else:
            cursor.execute("""
                SELECT transaction_id, operation, key, value, timestamp
                FROM transaction_log
                ORDER BY id
            """)
        
        return cursor.fetchall()
    
    def clear_transaction_log(self, before_timestamp: Optional[str] = None) -> None:
        """Clear old transaction log entries."""
        if not self.connection:
            self.initialize()
            
        cursor = self.connection.cursor()
        
        if before_timestamp:
            cursor.execute("""
                DELETE FROM transaction_log
                WHERE timestamp < ?
            """, (before_timestamp,))
        else:
            cursor.execute("DELETE FROM transaction_log")
        
        self.connection.commit()
    
    def backup_data(self, backup_path: str) -> None:
        """Create a backup of the database."""
        if not self.connection:
            self.initialize()
            
        backup_conn = sqlite3.connect(backup_path)
        self.connection.backup(backup_conn)
        backup_conn.close()
    
    def restore_data(self, backup_path: str) -> None:
        """Restore data from a backup."""
        if not os.path.exists(backup_path):
            raise FileNotFoundError(f"Backup file not found: {backup_path}")
        
        if self.connection:
            self.connection.close()
        
        # Replace current database with backup
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        
        backup_conn = sqlite3.connect(backup_path)
        new_conn = sqlite3.connect(self.db_path)
        backup_conn.backup(new_conn)
        backup_conn.close()
        new_conn.close()
        
        # Reinitialize connection
        self.initialize()
    
    def close(self) -> None:
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def __enter__(self):
        """Context manager entry."""
        self.initialize()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


class InMemoryStorage(StorageBackend):
    """In-memory storage backend for testing."""
    
    def __init__(self):
        self.data: Dict[str, Any] = {}
        self.transaction_log: List[Tuple] = []
    
    def initialize(self) -> None:
        """Initialize in-memory storage."""
        pass  # Nothing to initialize
    
    def get_committed_data(self) -> Dict[str, Any]:
        """Get all committed key-value pairs."""
        return self.data.copy()
    
    def commit_transaction(self, changes: Dict[str, Any], deletions: set[str]) -> None:
        """Commit transaction changes to memory."""
        # Apply changes
        for key, value in changes.items():
            self.data[key] = value
        
        # Apply deletions
        for key in deletions:
            self.data.pop(key, None)
    
    def close(self) -> None:
        """Close the storage backend."""
        pass  # Nothing to close
