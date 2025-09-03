"""
Transaction management for the key-value store.
"""

from enum import Enum
from typing import Dict, Any, Optional, List, TYPE_CHECKING
import uuid
import copy

if TYPE_CHECKING:
    from .storage import StorageBackend


class TransactionState(Enum):
    """Transaction state enumeration."""
    ACTIVE = "active"
    COMMITTED = "committed"
    ROLLED_BACK = "rolled_back"


class Transaction:
    """Represents a single transaction with its state and operations."""
    
    def __init__(self, parent: Optional['Transaction'] = None) -> None:
        self.id = str(uuid.uuid4())
        self.state = TransactionState.ACTIVE
        self.parent = parent
        self.changes: Dict[str, Any] = {}  # Key -> Value mapping for this transaction
        self.deleted_keys: set[str] = set()  # Keys deleted in this transaction
        
    def set(self, key: str, value: Any) -> None:
        """Set a key-value pair in this transaction."""
        if self.state != TransactionState.ACTIVE:
            raise ValueError(f"Cannot modify transaction in state: {self.state}")
        
        self.changes[key] = value
        # Remove from deleted keys if it was previously deleted
        self.deleted_keys.discard(key)
    
    def delete(self, key: str) -> None:
        """Delete a key in this transaction."""
        if self.state != TransactionState.ACTIVE:
            raise ValueError(f"Cannot modify transaction in state: {self.state}")
        
        self.deleted_keys.add(key)
        # Remove from changes if it was previously set
        self.changes.pop(key, None)
    
    def has_key(self, key: str) -> bool:
        """Check if this transaction has a value for the given key."""
        return key in self.changes
    
    def is_deleted(self, key: str) -> bool:
        """Check if the key is deleted in this transaction."""
        return key in self.deleted_keys
    
    def get_value(self, key: str) -> Any:
        """Get the value for a key in this transaction."""
        if key in self.deleted_keys:
            raise KeyError(f"Key '{key}' was deleted in this transaction")
        return self.changes[key]


class TransactionManager:
    """Manages the transaction stack and provides transaction operations."""
    
    def __init__(self, storage_backend: Optional['StorageBackend'] = None) -> None:
        self.transaction_stack: List[Transaction] = []
        self.storage_backend = storage_backend
        self._committed_data: Optional[Dict[str, Any]] = None
        
        # Load committed data from storage if available
        if self.storage_backend:
            self.storage_backend.initialize()
            self._committed_data = self.storage_backend.get_committed_data()
        else:
            self._committed_data = {}
    
    @property
    def committed_data(self) -> Dict[str, Any]:
        """Get committed data, loading from storage if needed."""
        if self._committed_data is None:
            if self.storage_backend:
                self._committed_data = self.storage_backend.get_committed_data()
            else:
                self._committed_data = {}
        return self._committed_data
    
    def begin(self) -> str:
        """Begin a new transaction and return its ID."""
        parent = self.transaction_stack[-1] if self.transaction_stack else None
        transaction = Transaction(parent)
        self.transaction_stack.append(transaction)
        return transaction.id
    
    def commit(self) -> None:
        """Commit the current transaction."""
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
                self.storage_backend.commit_transaction(
                    current_transaction.changes,
                    current_transaction.deleted_keys
                )
                # Reload committed data from storage
                self._committed_data = self.storage_backend.get_committed_data()
            else:
                # Commit to in-memory storage
                for key, value in current_transaction.changes.items():
                    self.committed_data[key] = value
                
                for key in current_transaction.deleted_keys:
                    self.committed_data.pop(key, None)
    
    def rollback(self) -> None:
        """Rollback the current transaction."""
        if not self.transaction_stack:
            raise ValueError("No active transaction to rollback")
        
        current_transaction = self.transaction_stack.pop()
        current_transaction.state = TransactionState.ROLLED_BACK
        # Changes are simply discarded
    
    def get(self, key: str) -> Any:
        """Get a value, considering transaction stack."""
        # Check transactions from most recent to oldest
        for transaction in reversed(self.transaction_stack):
            if transaction.is_deleted(key):
                raise KeyError(f"Key '{key}' not found")
            if transaction.has_key(key):
                return transaction.get_value(key)
        
        # Check committed data
        if key in self.committed_data:
            return self.committed_data[key]
        
        raise KeyError(f"Key '{key}' not found")
    
    def set(self, key: str, value: Any) -> None:
        """Set a value in the current transaction."""
        if not self.transaction_stack:
            raise ValueError("No active transaction")
        
        current_transaction = self.transaction_stack[-1]
        current_transaction.set(key, value)
    
    def delete(self, key: str) -> None:
        """Delete a key in the current transaction."""
        if not self.transaction_stack:
            raise ValueError("No active transaction")
        
        # Check if key exists (in any transaction or committed data)
        try:
            self.get(key)  # This will raise KeyError if not found
        except KeyError:
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
    
    def close(self) -> None:
        """Close the transaction manager and storage backend."""
        if self.storage_backend:
            self.storage_backend.close()
