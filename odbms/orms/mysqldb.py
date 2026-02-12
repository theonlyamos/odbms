import logging
import os
from sys import exit
from typing import Dict, List, Any, Optional, Union, Type, cast
import asyncio
import aiomysql
from aiomysql import Pool, Connection, DictCursor

from .base import ORM
from ..query_utils import SQLIdentifier, QueryBuilder

class MysqlDB(ORM):
    _db: Optional[Connection] = None
    _dbms: str = 'mysql'
    _pool: Optional[Pool] = None
    _loop: Optional[asyncio.AbstractEventLoop] = None
    _transaction_conn: Optional[Connection] = None

    @classmethod
    def connect(cls, dbsettings: dict) -> None:
        cls._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(cls._loop)
        
        try:
            cls._pool = cls._loop.run_until_complete(aiomysql.create_pool(
                host=dbsettings.get('host', 'localhost'),
                port=dbsettings.get('port', 3306),
                user=dbsettings['user'],
                password=dbsettings['password'],
                db=dbsettings.get('database'),
                autocommit=True,
                minsize=1,
                maxsize=10
            ))
        except Exception as e:
            if 'Unknown database' in str(e):
                dbsettings = dbsettings.copy()
                del dbsettings['database']
                cls._pool = cls._loop.run_until_complete(aiomysql.create_pool(
                    host=dbsettings.get('host', 'localhost'),
                    port=dbsettings.get('port', 3306),
                    user=dbsettings['user'],
                    password=dbsettings['password'],
                    autocommit=True,
                    minsize=1,
                    maxsize=10
                ))
            else:
                print(str(e))
                exit(1)
    
    @classmethod
    def disconnect(cls) -> None:
        if cls._pool:
            cls._pool.close()
            if cls._loop:
                cls._loop.run_until_complete(cls._pool.wait_closed())
        if cls._loop:
            cls._loop.close()
            cls._loop = None
    
    @classmethod
    async def begin_transaction(cls) -> Connection:
        if cls._pool is None:
            raise RuntimeError("Database not connected")
        cls._transaction_conn = await cls._pool.acquire()
        async with cls._transaction_conn.cursor() as cur:
            await cur.execute("START TRANSACTION")
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
    def _run_sync(cls, coro):
        if cls._loop is None or cls._loop.is_closed():
            cls._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(cls._loop)
        return cls._loop.run_until_complete(coro)
    
    @classmethod
    def update(cls, table: str, filter: dict, data: dict) -> int:
        return cls._run_sync(cls.update_async(table, filter, data))
            
    @classmethod
    def remove(cls, table: str, filter: dict) -> int:
        return cls._run_sync(cls.remove_async(table, filter))

    @classmethod
    def sum(cls, table: str, column: str, filter: dict = {}) -> Union[int, float]:
        return cls._run_sync(cls.sum_async(table, column, filter))

    @classmethod
    async def insert_one(cls, table: str, data: dict) -> Union[str, int]:
        if cls._pool is None:
            raise RuntimeError("Database not connected")

        quoted_table = SQLIdentifier.quote_identifier(table, 'mysql')
        columns = SQLIdentifier.quote_identifiers(list(data.keys()), 'mysql')
        placeholders = ', '.join(['%s'] * len(data))
        query = f'INSERT INTO {quoted_table} ({columns}) VALUES ({placeholders})'

        async with cls._pool.acquire() as conn:
            async with conn.cursor(DictCursor) as cur:
                await cur.execute(query, tuple(data.values()))
                return cur.lastrowid or 0

    @classmethod
    async def find(cls, table: str, filter: dict = {}, columns: list = ['*']) -> List[Dict[str, Any]]:
        if cls._pool is None:
            raise RuntimeError("Database not connected")

        quoted_table = SQLIdentifier.quote_identifier(table, 'mysql')
        col_str = SQLIdentifier.quote_identifiers(columns, 'mysql') if columns != ['*'] else '*'
        query = f'SELECT {col_str} FROM {quoted_table}'
        
        if filter:
            conditions = ' AND '.join([f'{SQLIdentifier.quote_identifier(k, "mysql")} = %s' for k in filter.keys()])
            query += f' WHERE {conditions}'

        async with cls._pool.acquire() as conn:
            async with conn.cursor(DictCursor) as cur:
                await cur.execute(query, tuple(filter.values()))
                results = await cur.fetchall()
                return [dict(row) for row in results]

    @classmethod
    async def find_one(cls, table: str, filter: dict = {}, columns: list = ['*']) -> Optional[Dict[str, Any]]:
        if cls._pool is None:
            raise RuntimeError("Database not connected")

        quoted_table = SQLIdentifier.quote_identifier(table, 'mysql')
        col_str = SQLIdentifier.quote_identifiers(columns, 'mysql') if columns != ['*'] else '*'
        query = f'SELECT {col_str} FROM {quoted_table}'
        
        if filter:
            conditions = ' AND '.join([f'{SQLIdentifier.quote_identifier(k, "mysql")} = %s' for k in filter.keys()])
            query += f' WHERE {conditions}'
        query += ' LIMIT 1'

        async with cls._pool.acquire() as conn:
            async with conn.cursor(DictCursor) as cur:
                await cur.execute(query, tuple(filter.values()))
                result = await cur.fetchone()
                return dict(result) if result else None

    @classmethod
    async def update_async(cls, table: str, filter: dict, data: dict) -> int:
        if cls._pool is None:
            raise RuntimeError("Database not connected")

        quoted_table = SQLIdentifier.quote_identifier(table, 'mysql')
        set_clause = ', '.join([f"{SQLIdentifier.quote_identifier(k, 'mysql')} = %s" for k in data.keys()])
        query = f'UPDATE {quoted_table} SET {set_clause}'
        
        params = list(data.values())
        if filter:
            conditions = ' AND '.join([f"{SQLIdentifier.quote_identifier(k, 'mysql')} = %s" for k in filter.keys()])
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

        quoted_table = SQLIdentifier.quote_identifier(table, 'mysql')
        set_clause = ', '.join([f"{SQLIdentifier.quote_identifier(k, 'mysql')} = %s" for k in data.keys()])
        query = f'UPDATE {quoted_table} SET {set_clause}'
        
        params = list(data.values())
        if filter:
            conditions = ' AND '.join([f"{SQLIdentifier.quote_identifier(k, 'mysql')} = %s" for k in filter.keys()])
            query += f' WHERE {conditions}'
            params.extend(filter.values())
        query += ' LIMIT 1'

        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, tuple(params))
                return cur.rowcount

    @classmethod
    async def update_many(cls, table: str, filter: dict, data: dict) -> int:
        return await cls.update_async(table, filter, data)

    @classmethod
    async def remove_async(cls, table: str, filter: dict) -> int:
        if cls._pool is None:
            raise RuntimeError("Database not connected")

        quoted_table = SQLIdentifier.quote_identifier(table, 'mysql')
        query = f'DELETE FROM {quoted_table}'
        if filter:
            conditions = ' AND '.join([f"{SQLIdentifier.quote_identifier(k, 'mysql')} = %s" for k in filter.keys()])
            query += f' WHERE {conditions}'

        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, tuple(filter.values()))
                return cur.rowcount

    @classmethod
    async def delete_one(cls, table: str, filter: dict) -> int:
        if cls._pool is None:
            raise RuntimeError("Database not connected")

        quoted_table = SQLIdentifier.quote_identifier(table, 'mysql')
        query = f'DELETE FROM {quoted_table}'
        if filter:
            conditions = ' AND '.join([f"{SQLIdentifier.quote_identifier(k, 'mysql')} = %s" for k in filter.keys()])
            query += f' WHERE {conditions}'
        query += ' LIMIT 1'

        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, tuple(filter.values()))
                return cur.rowcount

    @classmethod
    async def delete_many(cls, table: str, filter: dict) -> int:
        return await cls.remove_async(table, filter)

    @classmethod
    async def sum_async(cls, table: str, column: str, filter: dict = {}) -> Union[int, float]:
        if cls._pool is None:
            raise RuntimeError("Database not connected")

        quoted_table = SQLIdentifier.quote_identifier(table, 'mysql')
        quoted_column = SQLIdentifier.quote_identifier(column, 'mysql')
        query = f'SELECT SUM({quoted_column}) as total FROM {quoted_table}'
        if filter:
            conditions = ' AND '.join([f'{SQLIdentifier.quote_identifier(k, "mysql")} = %s' for k in filter.keys()])
            query += f' WHERE {conditions}'

        async with cls._pool.acquire() as conn:
            async with conn.cursor(DictCursor) as cur:
                await cur.execute(query, tuple(filter.values()))
                result = await cur.fetchone()
                if result:
                    total = result['total']
                    return float(total) if total is not None else 0
                return 0

    @classmethod
    async def query(cls, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        if cls._pool is None:
            raise RuntimeError("Database not connected")
        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params or {})
                return cur