from typing import Dict, Any, Optional
from queue import Queue
import threading
from contextlib import contextmanager
import psycopg2
import sqlite3
from pymongo import MongoClient
# import mysql.connector
# from mysql.connector import pooling

class ConnectionPool:
    """
    A thread-safe connection pool implementation that supports multiple database types.
    """
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.pools: Dict[str, Queue] = {}
            self.pool_sizes: Dict[str, int] = {}
            self.db_configs: Dict[str, Dict[str, Any]] = {}
            self.initialized = True

    def initialize_pool(self, dbms: str, config: Dict[str, Any], pool_size: int = 5):
        """
        Initialize a connection pool for a specific database.
        
        @param dbms: Database management system ('mysql', 'postgresql', 'sqlite', 'mongodb')
        @param config: Database configuration
        @param pool_size: Size of the connection pool
        """
        pool_key = self._get_pool_key(dbms, config)
        
        if pool_key in self.pools:
            return
        
        self.pool_sizes[pool_key] = pool_size
        self.db_configs[pool_key] = config
        self.pools[pool_key] = Queue(maxsize=pool_size)
        
        # Create initial connections
        for _ in range(pool_size):
            conn = self._create_connection(dbms, config)
            if conn:
                self.pools[pool_key].put(conn)

    def _get_pool_key(self, dbms: str, config: Dict[str, Any]) -> str:
        """Generate a unique key for the pool based on database configuration."""
        if dbms == 'sqlite':
            return f"{dbms}:{config.get('database', '')}"
        return f"{dbms}:{config.get('host', '')}:{config.get('port', '')}:{config.get('database', '')}"

    def _create_connection(self, dbms: str, config: Dict[str, Any]) -> Any:
        """Create a new database connection based on the DBMS type."""
        try:
            if dbms == 'postgresql':
                return psycopg2.connect(
                    host=config.get('host', 'localhost'),
                    port=config.get('port', 5432),
                    database=config.get('database', ''),
                    user=config.get('username', ''),
                    password=config.get('password', '')
                )
            # elif dbms == 'mysql':
            #     return mysql.connector.connect(
            #         host=config.get('host', 'localhost'),
            #         port=config.get('port', 3306),
            #         database=config.get('database', ''),
            #         user=config.get('username', ''),
            #         password=config.get('password', '')
            #     )
            elif dbms == 'sqlite':
                return sqlite3.connect(config.get('database', ':memory:'))
            elif dbms == 'mongodb':
                return MongoClient(
                    host=config.get('host', 'localhost'),
                    port=config.get('port', 27017)
                )
        except Exception as e:
            print(f"Error creating connection: {str(e)}")
            return None

    @contextmanager
    def get_connection(self, dbms: str, config: Dict[str, Any]) -> Any:
        """
        Get a connection from the pool. Use with context manager.
        
        Usage:
            with pool.get_connection(dbms, config) as conn:
                # use connection
        """
        pool_key = self._get_pool_key(dbms, config)
        
        if pool_key not in self.pools:
            self.initialize_pool(dbms, config)
        
        connection = None
        try:
            connection = self.pools[pool_key].get(timeout=5)
            yield connection
        finally:
            if connection:
                try:
                    # Check if connection is still valid
                    if dbms in ['postgresql', 'mysql']:
                        connection.ping(reconnect=True)
                    elif dbms == 'sqlite':
                        connection.cursor().execute('SELECT 1')
                    self.pools[pool_key].put(connection)
                except Exception:
                    # Connection is dead, create a new one
                    connection = self._create_connection(dbms, config)
                    if connection:
                        self.pools[pool_key].put(connection)

    def close_all(self):
        """Close all connections in all pools."""
        for pool_key, pool in self.pools.items():
            while not pool.empty():
                conn = pool.get_nowait()
                try:
                    conn.close()
                except Exception:
                    pass
            self.pools[pool_key] = Queue(maxsize=self.pool_sizes[pool_key]) 