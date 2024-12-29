#!python3
# -*- coding: utf-8 -*-
# @Date    : 2022-10-23 10:02:39
# @Author  : Amos Amissah (theonlyamos@gmai.com)
# @Link    : link
# @Version : 1.0.0

from datetime import datetime
import sqlite3
from typing import Optional, Dict, Any, List
import asyncio
from concurrent.futures import ThreadPoolExecutor
from ..dbms import Database

class SQLiteDB(Database):
    """SQLite database implementation."""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.dbms = 'sqlite'
        self._connection = None
        self._cursor = None
        self._executor = ThreadPoolExecutor(max_workers=4)  # Pool for async operations
        self._loop = None
        # Use a shared in-memory database
        self._uri = 'file::memory:?cache=shared'
    
    def connect(self):
        """Connect to SQLite."""
        if self._connection is None:
            self._connection = sqlite3.connect(
                self._uri if self.config.get('database', ':memory:') == ':memory:' else self.config['database'],
                uri=True,
                check_same_thread=False  # Allow access from other threads
            )
            self._connection.row_factory = sqlite3.Row
            self._cursor = self._connection.cursor()
    
    def disconnect(self):
        """Disconnect from SQLite."""
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()
            self._connection = None
            self._cursor = None
        if self._executor:
            self._executor.shutdown(wait=False)
            self._executor = None
        self._loop = None
    
    async def _ensure_loop(self):
        """Ensure we have a valid event loop."""
        if self._loop is None or self._loop.is_closed():
            self._loop = asyncio.get_running_loop()
        return self._loop
    
    async def _run_in_executor(self, func, *args, **kwargs):
        """Run a function in the thread pool executor."""
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=4)
        loop = await self._ensure_loop()
        return await loop.run_in_executor(self._executor, func, *args, **kwargs)
    
    def _get_connection(self):
        """Get a new connection for thread-safe operations."""
        conn = sqlite3.connect(
            self._uri if self.config.get('database', ':memory:') == ':memory:' else self.config['database'],
            uri=True,
            check_same_thread=False
        )
        conn.row_factory = sqlite3.Row
        return conn
    
    def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a query."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or {})
            conn.commit()
            return cursor
    
    def find(self, table: str, params: Optional[Dict[str, Any]] = None) -> List[Dict]:
        """Find records matching params."""
        query = f"SELECT * FROM {table}"
        if params:
            conditions = " AND ".join(f"{k} = :{k}" for k in params.keys())
            query += f" WHERE {conditions}"
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or {})
            return [dict(row) for row in cursor.fetchall()]
    
    def find_one(self, table: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict]:
        """Find one record matching params."""
        query = f"SELECT * FROM {table}"
        if params:
            conditions = " AND ".join(f"{k} = :{k}" for k in params.keys())
            query += f" WHERE {conditions} LIMIT 1"
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or {})
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def insert(self, table: str, data: dict) -> Any:
        """Insert a record."""
        
        # Remove id if it's a string (MongoDB style) since SQLite uses auto-increment

        if 'id' in data and isinstance(data['id'], str):
            del data['id']
        
        # Convert datetime strings to proper SQLite timestamp format

        for key, value in data.items():

            if isinstance(value, str) and ('_at' in key or key.endswith('date')):
                try:
                    # Try to parse and format as SQLite timestamp
                    dt = datetime.fromisoformat(value)
                    data[key] = dt.strftime('%Y-%m-%d %H:%M:%S')

                except ValueError:
                    pass  # Keep original value if parsing fails
        
        columns = ", ".join(data.keys())
        placeholders = ", ".join(f":{k}" for k in data.keys())
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, data)
            conn.commit()
            return cursor.lastrowid
    
    def insert_many(self, table: str, data: List[dict]) -> Any:
        """Insert multiple records."""
        if not data:
            return None
        
        columns = ", ".join(data[0].keys())
        placeholders = ", ".join(f":{k}" for k in data[0].keys())
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(query, data)
            conn.commit()
            return cursor.rowcount
    
    def update(self, table: str, params: dict, data: dict) -> Any:
        """Update records matching params."""
        set_values = ", ".join(f"{k} = :{k}" for k in data.keys())
        conditions = " AND ".join(f"{k} = :where_{k}" for k in params.keys())
        query = f"UPDATE {table} SET {set_values} WHERE {conditions}"
        
        # Prefix param keys with 'where_' to avoid conflicts
        params_with_prefix = {f"where_{k}": v for k, v in params.items()}
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, {**data, **params_with_prefix})
            conn.commit()
            return cursor.rowcount
    
    def remove(self, table: str, params: dict) -> Any:
        """Remove records matching params."""
        conditions = " AND ".join(f"{k} = :{k}" for k in params.keys())
        query = f"DELETE FROM {table} WHERE {conditions}"
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            return cursor.rowcount
    
    async def find_async(self, table: str, params: Optional[Dict[str, Any]] = None) -> List[Dict]:
        """Find records matching params asynchronously."""
        return await self._run_in_executor(self.find, table, params)
    
    async def find_one_async(self, table: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict]:
        """Find one record matching params asynchronously."""
        return await self._run_in_executor(self.find_one, table, params)
    
    async def insert_async(self, table: str, data: dict) -> Any:
        """Insert a record asynchronously."""
        return await self._run_in_executor(self.insert, table, data)
    
    async def insert_many_async(self, table: str, data: List[dict]) -> Any:
        """Insert multiple records asynchronously."""
        return await self._run_in_executor(self.insert_many, table, data)
    
    async def update_async(self, table: str, params: dict, data: dict) -> Any:
        """Update records matching params asynchronously."""
        return await self._run_in_executor(self.update, table, params, data)
    
    async def remove_async(self, table: str, params: dict) -> Any:
        """Remove records matching params asynchronously."""
        return await self._run_in_executor(self.remove, table, params)