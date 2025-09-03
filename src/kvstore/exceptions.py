"""
Custom exceptions for the transactional key-value store.
"""


class StoreError(Exception):
    """Base exception for all store-related errors."""
    pass


class TransactionError(StoreError):
    """Exception raised for transaction-related errors."""
    pass


class KeyNotFoundError(StoreError):
    """Exception raised when a key is not found in the store."""
    pass


class NoActiveTransactionError(TransactionError):
    """Exception raised when trying to commit/rollback without an active transaction."""
    pass


class InvalidTransactionStateError(TransactionError):
    """Exception raised when transaction is in an invalid state for the operation."""
    pass
