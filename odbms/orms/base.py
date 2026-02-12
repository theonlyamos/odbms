from typing import Any, List, Dict, Union, Optional, Tuple
from contextlib import contextmanager
from ..query_utils import SQLIdentifier, QueryBuilder
from ..connection_pool import ConnectionPool

class ORM:
    db: Any
    dbms: str
    _pool: Optional[ConnectionPool] = None
    _config: Dict[str, Any] = {}

    def initialize(self, *args, **kwargs):
        self._config = kwargs
        if self.dbms != 'mongodb':
            self._pool = ConnectionPool()
            self._pool.initialize_pool(self.dbms, kwargs)

    @contextmanager
    def get_connection(self):
        if self.dbms == 'mongodb':
            yield self.db
        elif self._pool:
            with self._pool.get_connection(self.dbms, self._config) as conn:
                yield conn
        else:
            raise RuntimeError("Connection pool not initialized")

    def _execute_sql(self, conn, sql: str, params: tuple = (), fetch: bool = True):
        cursor = conn.cursor()
        cursor.execute(sql, params)
        
        if fetch and sql.lower().strip().startswith(('select', 'show')):
            return cursor.fetchall()
        
        conn.commit()
        return cursor.lastrowid if sql.lower().strip().startswith('insert') else None

    def insert(self, table: str, data: dict):
        if self.dbms == 'mongodb':
            return self.db[table].insert_one(data)
        
        sql, params = QueryBuilder.build_insert(table, data, self.dbms)
        with self.get_connection() as conn:
            return self._execute_sql(conn, sql, params)

    def insert_many(self, table: str, data: List[dict]):
        if self.dbms == 'mongodb':
            return self.db[table].insert_many(data)
        
        if not data:
            return None
        
        columns = list(data[0].keys())
        quoted_columns = SQLIdentifier.quote_identifiers(columns, self.dbms)
        quoted_table = SQLIdentifier.quote_identifier(table, self.dbms)
        placeholders = ', '.join(['%s'] * len(columns))
        sql = f"INSERT INTO {quoted_table} ({quoted_columns}) VALUES ({placeholders})"
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(sql, [tuple(item.get(c) for c in columns) for item in data])
            conn.commit()
            return cursor.lastrowid

    def find(self, table: str, filter: dict = {}, projection: Union[List, Dict] = []):
        if self.dbms == 'mongodb':
            return self.db[table].find(filter, projection)
        
        builder = QueryBuilder(table, self.dbms)
        if projection and isinstance(projection, list):
            builder.select(*projection)
        for key, value in filter.items():
            builder.where(key, '=', value)
        
        sql, params = builder.build_select()
        with self.get_connection() as conn:
            return self._execute_sql(conn, sql, params)

    def find_one(self, table: str, filter: dict = {}, projection: Union[List, Dict] = []):
        if self.dbms == 'mongodb':
            return self.db[table].find_one(filter, projection)
        
        builder = QueryBuilder(table, self.dbms)
        if projection and isinstance(projection, list):
            builder.select(*projection)
        for key, value in filter.items():
            builder.where(key, '=', value)
        builder.limit(1)
        
        sql, params = builder.build_select()
        with self.get_connection() as conn:
            results = self._execute_sql(conn, sql, params)
            return results[0] if results else None

    def remove(self, table: str, filter: dict):
        if self.dbms == 'mongodb':
            return self.db[table].delete_many(filter)
        
        sql, params = QueryBuilder.build_delete(table, filter, self.dbms)
        with self.get_connection() as conn:
            return self._execute_sql(conn, sql, params, fetch=False)

    def delete(self, table: str, filter: dict):
        return self.remove(table, filter)

    def update(self, table: str, filter: dict, data: dict):
        if self.dbms == 'mongodb':
            return self.db[table].update_many(filter, {'$set': data})
        
        sql, params = QueryBuilder.build_update(table, data, filter, self.dbms)
        with self.get_connection() as conn:
            return self._execute_sql(conn, sql, params, fetch=False)

    def update_many(self, table: str, filter: dict, data: dict):
        return self.update(table, filter, data)

    def count(self, table: str, filter: dict = {}):
        if self.dbms == 'mongodb':
            return self.db[table].count_documents(filter)
        
        quoted_table = SQLIdentifier.quote_identifier(table, self.dbms)
        where_parts = []
        params = []
        for key, value in filter.items():
            quoted_key = SQLIdentifier.quote_identifier(key, self.dbms)
            where_parts.append(f"{quoted_key} = %s")
            params.append(value)
        
        sql = f"SELECT COUNT(*) as count FROM {quoted_table}"
        if where_parts:
            sql += f" WHERE {' AND '.join(where_parts)}"
        
        with self.get_connection() as conn:
            results = self._execute_sql(conn, sql, tuple(params))
            return results[0]['count'] if results else 0

    def sum(self, table: str, column: str, params: dict = {}):
        if self.dbms == 'mongodb':
            pipeline = [
                {'$match': params},
                {'$group': {'_id': None, 'total': {'$sum': f'${column}'}}}
            ]
            result = list(self.db[table].aggregate(pipeline))
            return result[0]['total'] if result else 0
        
        quoted_table = SQLIdentifier.quote_identifier(table, self.dbms)
        quoted_column = SQLIdentifier.quote_identifier(column, self.dbms)
        where_parts = []
        sql_params = []
        for key, value in params.items():
            quoted_key = SQLIdentifier.quote_identifier(key, self.dbms)
            where_parts.append(f"{quoted_key} = %s")
            sql_params.append(value)
        
        sql = f"SELECT SUM({quoted_column}) as total FROM {quoted_table}"
        if where_parts:
            sql += f" WHERE {' AND '.join(where_parts)}"
        
        with self.get_connection() as conn:
            results = self._execute_sql(conn, sql, tuple(sql_params))
            return results[0]['total'] if results else 0

    def query(self, query: str, params: tuple = ()):
        if self.dbms == 'mongodb':
            return None
        
        with self.get_connection() as conn:
            return self._execute_sql(conn, query, params)

    def import_from_file(self, filename: str):
        pass
    
    def command(self, command: str, table: str):
        if self.dbms == 'mongodb':
            return self.db.command(command, table)
        return None
    
    def begin_transaction(self):
        pass
    
    def commit(self):
        pass
    
    def rollback(self):
        pass