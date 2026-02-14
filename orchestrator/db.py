import sqlite3
import os
import queue
import threading

# Thread-safe connection pool
_conn_pool = queue.Queue(maxsize=5)
_pool_lock = threading.Lock()

def get_db_connection():
    """Gets a connection from the pool or creates a new one."""
    try:
        return _conn_pool.get_nowait()
    except queue.Empty:
        # Pool exhausted, create new connection
        return sqlite3.connect('/tmp/orchestrator.db', timeout=10.0)

def return_db_connection(conn):
    """Returns a connection to the pool or closes it ifpool is full."""
    if conn is None:
        return
    try:
        _conn_pool.put_nowait(conn)
    except queue.Full:
        # Pool full, close connection
        conn.close()
