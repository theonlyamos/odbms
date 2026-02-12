from typing import Dict, Any, Optional
from queue import Queue, Empty
import threading
from contextlib import contextmanager
import time
import logging
import psycopg2
import sqlite3
from pymongo import MongoClient

logger = logging.getLogger(__name__)

class ConnectionPool:
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
            self._retry_attempts = 3
            self._retry_delay = 0.5

    def initialize_pool(self, dbms: str, config: Dict[str, Any], pool_size: int = 5):
        pool_key = self._get_pool_key(dbms, config)
        
        if pool_key in self.pools:
            return
        
        self.pool_sizes[pool_key] = pool_size
        self.db_configs[pool_key] = config
        self.pools[pool_key] = Queue(maxsize=pool_size)
        
        for _ in range(pool_size):
            conn = self._create_connection_with_retry(dbms, config)
            if conn:
                self.pools[pool_key].put(conn)

    def _get_pool_key(self, dbms: str, config: Dict[str, Any]) -> str:
        if dbms == 'sqlite':
            return f"{dbms}:{config.get('database', '')}"
        return f"{dbms}:{config.get('host', '')}:{config.get('port', '')}:{config.get('database', '')}"

    def _create_connection(self, dbms: str, config: Dict[str, Any]) -> Any:
        try:
            if dbms == 'postgresql':
                return psycopg2.connect(
                    host=config.get('host', 'localhost'),
                    port=config.get('port', 5432),
                    database=config.get('database', ''),
                    user=config.get('username', ''),
                    password=config.get('password'),
                    connect_timeout=5
                )
            elif dbms == 'sqlite':
                conn = sqlite3.connect(config.get('database', ':memory:'), check_same_thread=False)
                conn.row_factory = sqlite3.Row
                return conn
            elif dbms == 'mongodb':
                return MongoClient(
                    host=config.get('host', 'localhost'),
                    port=config.get('port', 27017),
                    serverSelectionTimeoutMS=5000
                )
        except Exception as e:
            logger.error(f"Error creating connection: {str(e)}")
            return None
    
    def _create_connection_with_retry(self, dbms: str, config: Dict[str, Any]) -> Any:
        last_error = None
        for attempt in range(self._retry_attempts):
            conn = self._create_connection(dbms, config)
            if conn and self._validate_connection(conn, dbms):
                return conn
            last_error = "Connection validation failed" if conn else "Connection creation failed"
            if attempt < self._retry_attempts - 1:
                time.sleep(self._retry_delay * (attempt + 1))
        logger.error(f"Failed to create connection after {self._retry_attempts} attempts: {last_error}")
        return None

    def _validate_connection(self, conn: Any, dbms: str) -> bool:
        try:
            if dbms == 'postgresql':
                with conn.cursor() as cur:
                    cur.execute('SELECT 1')
            elif dbms == 'sqlite':
                conn.execute('SELECT 1')
            elif dbms == 'mongodb':
                conn.admin.command('ping')
            return True
        except Exception as e:
            logger.warning(f"Connection validation failed: {str(e)}")
            try:
                conn.close()
            except Exception:
                pass
            return False

    @contextmanager
    def get_connection(self, dbms: str, config: Dict[str, Any]) -> Any:
        pool_key = self._get_pool_key(dbms, config)
        
        if pool_key not in self.pools:
            self.initialize_pool(dbms, config)
        
        connection = None
        try:
            connection = self._get_validated_connection(pool_key, dbms, config)
            yield connection
        finally:
            if connection:
                self._return_connection(pool_key, connection, dbms, config)
    
    def _get_validated_connection(self, pool_key: str, dbms: str, config: Dict[str, Any]) -> Any:
        for _ in range(self._retry_attempts):
            try:
                connection = self.pools[pool_key].get(timeout=5)
                if self._validate_connection(connection, dbms):
                    return connection
                self._replace_connection(pool_key, connection, dbms, config)
            except Empty:
                connection = self._create_connection_with_retry(dbms, config)
                if connection:
                    return connection
        raise RuntimeError(f"Could not get valid connection for {pool_key}")
    
    def _replace_connection(self, pool_key: str, old_conn: Any, dbms: str, config: Dict[str, Any]) -> None:
        try:
            old_conn.close()
        except Exception:
            pass
        new_conn = self._create_connection_with_retry(dbms, config)
        if new_conn:
            self.pools[pool_key].put(new_conn)
    
    def _return_connection(self, pool_key: str, connection: Any, dbms: str, config: Dict[str, Any]) -> None:
        if self._validate_connection(connection, dbms):
            try:
                self.pools[pool_key].put_nowait(connection)
            except Exception:
                self._replace_connection(pool_key, connection, dbms, config)
        else:
            self._replace_connection(pool_key, connection, dbms, config)

    def close_all(self):
        for pool_key, pool in self.pools.items():
            while not pool.empty():
                conn = pool.get_nowait()
                try:
                    conn.close()
                except Exception:
                    pass
            self.pools[pool_key] = Queue(maxsize=self.pool_sizes.get(pool_key, 5))