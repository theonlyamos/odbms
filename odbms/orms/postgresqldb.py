from sys import exit
from typing import Dict, List, Any, Optional, Union, Type, cast
import asyncio
import aiopg
from aiopg import Pool, Connection, Cursor

from .base import ORM
from ..query_utils import SQLIdentifier, QueryBuilder

class PostgresqlDB(ORM):
    _db: Optional[Connection] = None
    _dbms: str = 'postgresql'
    _pool: Optional[Pool] = None
    _loop: Optional[asyncio.AbstractEventLoop] = None
    _transaction_conn: Optional[Connection] = None

    @classmethod
    async def connect(cls, dbsettings: dict) -> None:
        try:
            dsn = (
                f"dbname={dbsettings.get('database')} "
                f"user={dbsettings['user']} "
                f"password={dbsettings['password']} "
                f"host={dbsettings.get('host', 'localhost')} "
                f"port={dbsettings.get('port', 5432)}"
            )
            cls._pool = await aiopg.create_pool(dsn, minsize=1, maxsize=10)
        except Exception as e:
            if 'database' in str(e):
                dbsettings = dbsettings.copy()
                dsn = (
                    f"user={dbsettings['user']} "
                    f"password={dbsettings['password']} "
                    f"host={dbsettings.get('host', 'localhost')} "
                    f"port={dbsettings.get('port', 5432)}"
                )
                cls._pool = await aiopg.create_pool(dsn, minsize=1, maxsize=10)
            else:
                print(str(e))
                exit(1)
    
    @classmethod
    async def disconnect(cls) -> None:
        if cls._pool:
            cls._pool.close()
            if cls._loop:
                await cls._pool.wait_closed()
        if cls._loop:
            cls._loop.close()
            cls._loop = None
    
    @classmethod
    async def begin_transaction(cls) -> Connection:
        if cls._pool is None:
            raise RuntimeError("Database not connected")
        cls._transaction_conn = await cls._pool.acquire()
        async with cls._transaction_conn.cursor() as cur:
            await cur.execute("BEGIN")
        return cls._transaction_conn
    
    @classmethod
    async def commit(cls) -> None:
        if cls._transaction_conn:
            async with cls._transaction_conn.cursor() as cur:
                await cur.execute("COMMIT")
            cls._pool.release(cls._transaction_conn)
            cls._transaction_conn = None
    
    @classmethod
    async def rollback(cls) -> None:
        if cls._transaction_conn:
            async with cls._transaction_conn.cursor() as cur:
                await cur.execute("ROLLBACK")
            cls._pool.release(cls._transaction_conn)
            cls._transaction_conn = None

    @classmethod
    async def query(cls, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        if cls._pool is None:
            raise RuntimeError("Database not connected")
        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params or {})
                return cur

    @classmethod
    async def insert_one(cls, table: str, data: dict) -> Union[str, int]:
        if cls._pool is None:
            raise RuntimeError("Database not connected")

        quoted_table = SQLIdentifier.quote_identifier(table, 'postgresql')
        columns = SQLIdentifier.quote_identifiers(list(data.keys()), 'postgresql')
        placeholders = ', '.join(['%s'] * len(data))
        query = f'INSERT INTO {quoted_table} ({columns}) VALUES ({placeholders}) RETURNING id'

        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, tuple(data.values()))
                result = await cur.fetchone()
                return result[0] if result else 0
    
    @classmethod
    async def insert_many(cls, table: str, data: List[dict]):
        if cls._pool is None:
            raise RuntimeError("Database not connected")
        
        if not data:
            return 0
        
        columns = list(data[0].keys())
        quoted_table = SQLIdentifier.quote_identifier(table, 'postgresql')
        quoted_columns = SQLIdentifier.quote_identifiers(columns, 'postgresql')
        placeholders = ', '.join(['%s'] * len(columns))
        query = f'INSERT INTO {quoted_table} ({quoted_columns}) VALUES ({placeholders})'
        params = [tuple(item.get(c) for c in columns) for item in data]

        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(query, params)
                return cur.rowcount

    @classmethod
    async def find(cls, table: str, filter: Optional[Dict[str, Any]] = None, columns: list = ['*']) -> List[Dict[str, Any]]:
        if cls._pool is None:
            raise RuntimeError("Database not connected")

        quoted_table = SQLIdentifier.quote_identifier(table, 'postgresql')
        col_str = SQLIdentifier.quote_identifiers(columns, 'postgresql') if columns != ['*'] else '*'
        query = f'SELECT {col_str} FROM {quoted_table}'
        params = ()
        
        if filter:
            conditions = ' AND '.join([f'{SQLIdentifier.quote_identifier(k, "postgresql")} = %s' for k in filter.keys()])
            query += f' WHERE {conditions}'
            params = tuple(filter.values())

        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params)
                results = await cur.fetchall()
                if not results or cur.description is None:
                    return []
                column_names = [desc[0] for desc in cur.description]
                return [dict(zip(column_names, row)) for row in results]

    @classmethod
    async def find_one(cls, table: str, filter: dict = {}, columns: list = ['*']) -> Optional[Dict[str, Any]]:
        if cls._pool is None:
            raise RuntimeError("Database not connected")

        quoted_table = SQLIdentifier.quote_identifier(table, 'postgresql')
        col_str = SQLIdentifier.quote_identifiers(columns, 'postgresql') if columns != ['*'] else '*'
        query = f'SELECT {col_str} FROM {quoted_table}'
        
        if filter:
            conditions = ' AND '.join([f'{SQLIdentifier.quote_identifier(k, "postgresql")} = %s' for k in filter.keys()])
            query += f' WHERE {conditions}'
        query += ' LIMIT 1'

        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, tuple(filter.values()))
                result = await cur.fetchone()
                if not result or cur.description is None:
                    return None
                column_names = [desc[0] for desc in cur.description]
                return dict(zip(column_names, result))

    @classmethod
    async def update_many(cls, table: str, filter: dict, data: dict) -> int:
        if cls._pool is None:
            raise RuntimeError("Database not connected")

        quoted_table = SQLIdentifier.quote_identifier(table, 'postgresql')
        set_clause = ', '.join([f"{SQLIdentifier.quote_identifier(k, 'postgresql')} = %s" for k in data.keys()])
        query = f'UPDATE {quoted_table} SET {set_clause}'
        
        params = list(data.values())
        if filter:
            conditions = ' AND '.join([f"{SQLIdentifier.quote_identifier(k, 'postgresql')} = %s" for k in filter.keys()])
            query += f' WHERE {conditions}'
            params.extend(filter.values())

        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, tuple(params))
                return cur.rowcount

    @classmethod
    async def update_one(cls, table: str, filter: dict, data: dict) -> int:
        if cls._pool is None:
            raise RuntimeError("Database not connected")

        quoted_table = SQLIdentifier.quote_identifier(table, 'postgresql')
        set_clause = ', '.join([f"{SQLIdentifier.quote_identifier(k, 'postgresql')} = %s" for k in data.keys()])
        query = f'UPDATE {quoted_table} SET {set_clause}'
        
        params = list(data.values())
        if filter:
            conditions = ' AND '.join([f"{SQLIdentifier.quote_identifier(k, 'postgresql')} = %s" for k in filter.keys()])
            query += f' WHERE {conditions}'
            params.extend(filter.values())
        query += ' LIMIT 1'

        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, tuple(params))
                return cur.rowcount

    @classmethod
    async def delete_many(cls, table: str, filter: dict) -> int:
        if cls._pool is None:
            raise RuntimeError("Database not connected")

        quoted_table = SQLIdentifier.quote_identifier(table, 'postgresql')
        query = f'DELETE FROM {quoted_table}'
        if filter:
            conditions = ' AND '.join([f"{SQLIdentifier.quote_identifier(k, 'postgresql')} = %s" for k in filter.keys()])
            query += f' WHERE {conditions}'

        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, tuple(filter.values()))
                return cur.rowcount

    @classmethod
    async def delete_one(cls, table: str, filter: dict) -> int:
        if cls._pool is None:
            raise RuntimeError("Database not connected")

        quoted_table = SQLIdentifier.quote_identifier(table, 'postgresql')
        query = f'DELETE FROM {quoted_table}'
        if filter:
            conditions = ' AND '.join([f"{SQLIdentifier.quote_identifier(k, 'postgresql')} = %s" for k in filter.keys()])
            query += f' WHERE {conditions}'
        query += ' LIMIT 1'

        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, tuple(filter.values()))
                return cur.rowcount

    @classmethod
    async def sum(cls, table: str, column: str, filter: dict = {}) -> Union[int, float]:
        if cls._pool is None:
            raise RuntimeError("Database not connected")

        quoted_table = SQLIdentifier.quote_identifier(table, 'postgresql')
        quoted_column = SQLIdentifier.quote_identifier(column, 'postgresql')
        query = f'SELECT SUM({quoted_column}) as total FROM {quoted_table}'
        if filter:
            conditions = ' AND '.join([f'{SQLIdentifier.quote_identifier(k, "postgresql")} = %s' for k in filter.keys()])
            query += f' WHERE {conditions}'

        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, tuple(filter.values()))
                result = await cur.fetchone()
                return float(result[0]) if result and result[0] is not None else 0
    
    @classmethod
    async def count(cls, table: str, filter: dict = {}) -> int:
        if cls._pool is None:
            raise RuntimeError("Database not connected")
        
        quoted_table = SQLIdentifier.quote_identifier(table, 'postgresql')
        query = f'SELECT COUNT(*) FROM {quoted_table}'
        if filter:
            conditions = ' AND '.join([f'{SQLIdentifier.quote_identifier(k, "postgresql")} = %s' for k in filter.keys()])
            query += f' WHERE {conditions}'

        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, tuple(filter.values()))
                result = await cur.fetchone()
                return result[0] if result else 0