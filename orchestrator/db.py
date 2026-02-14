import sqlite3
import os
import threading

# SQLite doesn't support connection pooling across threads
# Use thread-local storage instead
_thread_local = threading.local()

def get_db_connection():
    """Gets a thread-local connection to prevent SQLite thread errors."""
    # Check if we have a connection and if it's still valid
    if hasattr(_thread_local, 'connection') and _thread_local.connection is not None:
        try:
            # Test if connection is still alive
            _thread_local.connection.execute('SELECT 1')
            return _thread_local.connection
        except (sqlite3.ProgrammingError, sqlite3.DatabaseError):
            # Connection is closed or invalid, recreate it
            pass
    
    # Create new connection
    _thread_local.connection = sqlite3.connect('/tmp/orchestrator.db', timeout=10.0, check_same_thread=True)
    return _thread_local.connection

def return_db_connection(conn):
    """No-op for SQLite thread-local connections (kept for API compatibility)."""
    # Don't close - connection is reused per thread
    pass

def close_thread_connection():
    """Explicitly close thread-local connection (call on thread shutdown if needed)."""
    if hasattr(_thread_local, 'connection') and _thread_local.connection:
        try:
            _thread_local.connection.close()
        except:
            pass
        _thread_local.connection = None
