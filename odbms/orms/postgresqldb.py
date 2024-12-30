import logging
import os
from sys import exit
from typing import Dict, List, Any, Optional, Union, Type, cast
import asyncio
import aiopg
from aiopg import Pool, Connection, Cursor

from .base import ORM

class PostgresqlDB(ORM):
    _db: Optional[Connection] = None
    _dbms: str = 'postgresql'
    _pool: Optional[Pool] = None
    _loop: Optional[asyncio.AbstractEventLoop] = None

    @classmethod
    def connect(cls, dbsettings: dict) -> None:
        '''Connection method'''
        cls._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(cls._loop)
        
        try:
            dsn = (
                f"dbname={dbsettings.get('database')} "
                f"user={dbsettings['user']} "
                f"password={dbsettings['password']} "
                f"host={dbsettings.get('host', 'localhost')} "
                f"port={dbsettings.get('port', 5432)}"
            )
            cls._pool = cls._loop.run_until_complete(aiopg.create_pool(dsn))
        except Exception as e:
            if 'database' in str(e):
                # Try connecting without database to create it
                dbsettings = dbsettings.copy()
                dsn = (
                    f"user={dbsettings['user']} "
                    f"password={dbsettings['password']} "
                    f"host={dbsettings.get('host', 'localhost')} "
                    f"port={dbsettings.get('port', 5432)}"
                )
                cls._pool = cls._loop.run_until_complete(aiopg.create_pool(dsn))
            else:
                print(str(e))
                exit(1)
    
    @classmethod
    def disconnect(cls) -> None:
        """Disconnect from PostgreSQL."""
        if cls._pool:
            cls._pool.close()
            if cls._loop:
                cls._loop.run_until_complete(cls._pool.wait_closed())
        if cls._loop:
            cls._loop.close()
            cls._loop = None
    
    @classmethod
    def _run_sync(cls, coro):
        """Run coroutine synchronously."""
        if cls._loop is None or cls._loop.is_closed():
            cls._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(cls._loop)
        return cls._loop.run_until_complete(coro)

    @classmethod
    def insert(cls, table: str, data: dict) -> Union[str, int]:
        """Insert a record."""
        return cls._run_sync(cls.insert_async(table, data))
            
    @classmethod
    def find(cls, table: str, filter: Optional[Dict[str, Any]] = None, columns: list = ['*']) -> List[Dict[str, Any]]:
        """Find records matching filter."""
        return cls._run_sync(cls.find_async(table, filter or {}, columns))
            
    @classmethod
    def find_one(cls, table: str, filter: dict = {}, columns: list = ['*']) -> Optional[Dict[str, Any]]:
        """Find one record matching filter."""
        return cls._run_sync(cls.find_one_async(table, filter, columns))
            
    @classmethod
    def update(cls, table: str, filter: dict, data: dict) -> int:
        """Update records matching filter."""
        return cls._run_sync(cls.update_async(table, filter, data))
            
    @classmethod
    def remove(cls, table: str, filter: dict) -> int:
        """Remove records matching filter."""
        return cls._run_sync(cls.remove_async(table, filter))

    @classmethod
    def sum(cls, table: str, column: str, filter: dict = {}) -> Union[int, float]:
        """Sum values in a column."""
        return cls._run_sync(cls.sum_async(table, column, filter))

    @classmethod
    async def insert_async(cls, table: str, data: dict) -> Union[str, int]:
        """Insert a record asynchronously."""
        if cls._pool is None:
            raise RuntimeError("Database not connected")

        columns = ', '.join(data.keys())
        placeholders = ', '.join([f'%s'] * len(data))
        query = f'INSERT INTO {table} ({columns}) VALUES ({placeholders}) RETURNING id'

        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, tuple(data.values()))
                result = await cur.fetchone()
                return result[0] if result else 0

    @classmethod
    async def find_async(cls, table: str, filter: Optional[Dict[str, Any]] = None, columns: list = ['*']) -> List[Dict[str, Any]]:
        """Find records matching filter asynchronously."""
        if cls._pool is None:
            raise RuntimeError("Database not connected")

        query = f'SELECT {", ".join(columns)} FROM {table}'
        if filter:
            conditions = ' AND '.join([f'{k} = %s' for k in filter.keys()])
            query += f' WHERE {conditions}'
            params = tuple(filter.values())
        else:
            params = ()

        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params)
                results = await cur.fetchall()
                if not results:
                    return []
                
                # Convert results to dictionaries
                if cur.description is None:
                    return []
                column_names = [desc[0] for desc in cur.description]
                return [dict(zip(column_names, row)) for row in results]

    @classmethod
    async def find_one_async(cls, table: str, filter: dict = {}, columns: list = ['*']) -> Optional[Dict[str, Any]]:
        """Find one record matching filter asynchronously."""
        if cls._pool is None:
            raise RuntimeError("Database not connected")

        query = f'SELECT {", ".join(columns)} FROM {table}'
        if filter:
            conditions = ' AND '.join([f'{k} = %s' for k in filter.keys()])
            query += f' WHERE {conditions}'
        query += ' LIMIT 1'

        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, tuple(filter.values()))
                result = await cur.fetchone()
                if not result:
                    return None
                
                # Convert result to dictionary
                if cur.description is None:
                    return None
                column_names = [desc[0] for desc in cur.description]
                return dict(zip(column_names, result))

    @classmethod
    async def update_async(cls, table: str, filter: dict, data: dict) -> int:
        """Update records matching filter asynchronously."""
        if cls._pool is None:
            raise RuntimeError("Database not connected")

        set_clause = ', '.join([f"{k} = %s" for k in data.keys()])
        query = f'UPDATE {table} SET {set_clause}'
        
        params = list(data.values())
        if filter:
            conditions = ' AND '.join([f"{k} = %s" for k in filter.keys()])
            query += f' WHERE {conditions}'
            params.extend(filter.values())

        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, tuple(params))
                return cur.rowcount

    @classmethod
    async def remove_async(cls, table: str, filter: dict) -> int:
        """Remove records matching filter asynchronously."""
        if cls._pool is None:
            raise RuntimeError("Database not connected")

        query = f'DELETE FROM {table}'
        if filter:
            conditions = ' AND '.join([f"{k} = %s" for k in filter.keys()])
            query += f' WHERE {conditions}'

        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, tuple(filter.values()))
                return cur.rowcount

    @classmethod
    async def sum_async(cls, table: str, column: str, filter: dict = {}) -> Union[int, float]:
        """Sum values in a column asynchronously."""
        if cls._pool is None:
            raise RuntimeError("Database not connected")

        query = f'SELECT SUM({column}) as total FROM {table}'
        if filter:
            conditions = ' AND '.join([f'{k} = %s' for k in filter.keys()])
            query += f' WHERE {conditions}'

        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, tuple(filter.values()))
                result = await cur.fetchone()
                return float(result[0]) if result and result[0] is not None else 0