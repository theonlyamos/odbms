from datetime import datetime
import sqlite3
from typing import Optional, Dict, Any, List, Union
import asyncio
from concurrent.futures import ThreadPoolExecutor
from ..dbms import Database
from ..query_utils import SQLIdentifier, QueryBuilder

class SQLiteDB(Database):
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.dbms = 'sqlite'
        self._connection = None
        self._cursor = None
        self._executor = ThreadPoolExecutor(max_workers=4)
        self._loop = None
        self._uri = 'file::memory:?cache=shared'
        self._transaction_conn = None
    
    def connect(self):
        if self._connection is None:
            self._connection = sqlite3.connect(
                self._uri if self.config.get('database', ':memory:') == ':memory:' else self.config['database'],
                uri=True,
                check_same_thread=False
            )
            self._connection.row_factory = sqlite3.Row
            self._cursor = self._connection.cursor()
    
    def disconnect(self):
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
    
    def begin_transaction(self):
        if self._connection:
            self._transaction_conn = self._connection
            self._transaction_conn.execute("BEGIN")
        return self._transaction_conn
    
    def commit(self):
        if self._transaction_conn:
            self._transaction_conn.commit()
            self._transaction_conn = None
    
    def rollback(self):
        if self._transaction_conn:
            self._transaction_conn.rollback()
            self._transaction_conn = None
    
    async def _ensure_loop(self):
        if self._loop is None or self._loop.is_closed():
            self._loop = asyncio.get_running_loop()
        return self._loop
    
    async def _run_in_executor(self, func, *args, **kwargs):
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=4)
        loop = await self._ensure_loop()
        return await loop.run_in_executor(self._executor, func, *args, **kwargs)
    
    def _get_connection(self):
        conn = sqlite3.connect(
            self._uri if self.config.get('database', ':memory:') == ':memory:' else self.config['database'],
            uri=True,
            check_same_thread=False
        )
        conn.row_factory = sqlite3.Row
        return conn
    
    
    async def query(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or {})
            conn.commit()
            return cursor
    
    async def find(self, table: str, params: Optional[Dict[str, Any]] = None) -> List[Dict]:
        quoted_table = SQLIdentifier.quote_identifier(table, 'sqlite')
        query = f"SELECT * FROM {quoted_table}"
        sql_params = {}
        
        if params:
            conditions = " AND ".join(
                f"{SQLIdentifier.quote_identifier(k, 'sqlite')} = :{k}" 
                for k in params.keys()
            )
            query += f" WHERE {conditions}"
            sql_params = params
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, sql_params)
            return [dict(row) for row in cursor.fetchall()]
    
    async def find_one(self, table: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict]:
        quoted_table = SQLIdentifier.quote_identifier(table, 'sqlite')
        query = f"SELECT * FROM {quoted_table}"
        sql_params = {}
        
        if params:
            conditions = " AND ".join(
                f"{SQLIdentifier.quote_identifier(k, 'sqlite')} = :{k}" 
                for k in params.keys()
            )
            query += f" WHERE {conditions} LIMIT 1"
            sql_params = params
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, sql_params)
            row = cursor.fetchone()
            return dict(row) if row else None
    
    async def insert_one(self, table: str, data: dict) -> Any:
        if 'id' in data and isinstance(data['id'], str):
            del data['id']
        
        for key, value in data.items():
            if isinstance(value, str) and ('_at' in key or key.endswith('date')):
                try:
                    dt = datetime.fromisoformat(value)
                    data[key] = dt.strftime('%Y-%m-%d %H:%M:%S')
                except ValueError:
                    pass
        
        quoted_table = SQLIdentifier.quote_identifier(table, 'sqlite')
        columns = SQLIdentifier.quote_identifiers(list(data.keys()), 'sqlite')
        placeholders = ", ".join(f":{k}" for k in data.keys())
        query = f"INSERT INTO {quoted_table} ({columns}) VALUES ({placeholders})"
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, data)
            conn.commit()
            return cursor.lastrowid
    
    def insert_many(self, table: str, data: List[dict]) -> Any:
        if not data:
            return None
        
        quoted_table = SQLIdentifier.quote_identifier(table, 'sqlite')
        columns = SQLIdentifier.quote_identifiers(list(data[0].keys()), 'sqlite')
        placeholders = ", ".join(f":{k}" for k in data[0].keys())
        query = f"INSERT INTO {quoted_table} ({columns}) VALUES ({placeholders})"
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(query, data)
            conn.commit()
            return cursor.rowcount
    
    async def update_many(self, table: str, params: dict, data: dict) -> Any:
        quoted_table = SQLIdentifier.quote_identifier(table, 'sqlite')
        set_values = ", ".join(
            f"{SQLIdentifier.quote_identifier(k, 'sqlite')} = :{k}" 
            for k in data.keys()
        )
        conditions = " AND ".join(
            f"{SQLIdentifier.quote_identifier(k, 'sqlite')} = :where_{k}" 
            for k in params.keys()
        )
        query = f"UPDATE {quoted_table} SET {set_values} WHERE {conditions}"
        
        params_with_prefix = {f"where_{k}": v for k, v in params.items()}
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, {**data, **params_with_prefix})
            conn.commit()
            return cursor.rowcount
    
    async def update_one(self, table: str, params: dict, data: dict) -> Any:
        quoted_table = SQLIdentifier.quote_identifier(table, 'sqlite')
        set_values = ", ".join(
            f"{SQLIdentifier.quote_identifier(k, 'sqlite')} = :{k}" 
            for k in data.keys()
        )
        conditions = " AND ".join(
            f"{SQLIdentifier.quote_identifier(k, 'sqlite')} = :where_{k}" 
            for k in params.keys()
        )
        query = f"UPDATE {quoted_table} SET {set_values} WHERE rowid IN (SELECT rowid FROM {quoted_table} WHERE {conditions} LIMIT 1)"

        params_with_prefix = {f"where_{k}": v for k, v in params.items()}

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, {**data, **params_with_prefix})
            conn.commit()
            return cursor.rowcount
        
    async def delete_one(self, table: str, params: dict) -> Any:
        quoted_table = SQLIdentifier.quote_identifier(table, 'sqlite')
        conditions = " AND ".join(
            f"{SQLIdentifier.quote_identifier(k, 'sqlite')} = :{k}" 
            for k in params.keys()
        )
        query = f"DELETE FROM {quoted_table} WHERE {conditions} LIMIT 1"
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            return cursor.rowcount
    
    async def delete_many(self, table: str, params: dict) -> Any:
        quoted_table = SQLIdentifier.quote_identifier(table, 'sqlite')
        conditions = " AND ".join(
            f"{SQLIdentifier.quote_identifier(k, 'sqlite')} = :{k}" 
            for k in params.keys()
        )
        query = f"DELETE FROM {quoted_table} WHERE {conditions}"
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            return cursor.rowcount

    async def sum(self, table: str, column: str, params: dict = {}) -> Union[int, float]:
        quoted_table = SQLIdentifier.quote_identifier(table, 'sqlite')
        quoted_column = SQLIdentifier.quote_identifier(column, 'sqlite')
        query = f"SELECT SUM({quoted_column}) as total FROM {quoted_table}"
        
        if params:
            conditions = " AND ".join(
                f"{SQLIdentifier.quote_identifier(k, 'sqlite')} = :{k}" 
                for k in params.keys()
            )
            query += f" WHERE {conditions}"
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            return float(row[0]) if row and row[0] is not None else 0