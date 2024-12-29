from typing import Any, List, Dict, Union
from contextlib import contextmanager

class ORM:
    db: Any
    dbms: str

    def initialize(self, *args, **kwargs):
        pass

    @contextmanager
    def get_connection(self):
        """Get a connection from the pool or return MongoDB connection."""
        from ..dbms import DBMS
        
        if self.dbms != 'mongodb':
            with DBMS.get_connection() as conn:
                yield conn
        else:
            # MongoDB already manages its own connections
            yield self.db

    def _execute_sql(self, conn, sql: str, params: tuple = (), fetch: bool = True):
        """Execute SQL with proper cursor management."""
        cursor = conn.cursor()
        cursor.execute(sql, params)
        
        if fetch and sql.lower().strip().startswith(('select', 'show')):
            return cursor.fetchall()
        
        conn.commit()
        return cursor.lastrowid if sql.lower().strip().startswith('insert') else None

    def insert(self, table: str, data: dict):
        """Insert a single record."""
        if self.dbms == 'mongodb':
            return self.db[table].insert_one(data)
        
        with self.get_connection() as conn:
            placeholders = ', '.join(['%s'] * len(data))
            columns = ', '.join(data.keys())
            sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
            return self._execute_sql(conn, sql, tuple(data.values()))

    def insert_many(self, table: str, data: List[dict]):
        """Insert multiple records."""
        if self.dbms == 'mongodb':
            return self.db[table].insert_many(data)
        
        if not data:
            return None
            
        with self.get_connection() as conn:
            placeholders = ', '.join(['%s'] * len(data[0]))
            columns = ', '.join(data[0].keys())
            sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
            cursor = conn.cursor()
            cursor.executemany(sql, [tuple(item.values()) for item in data])
            conn.commit()
            return cursor.lastrowid

    def find(self, table: str, filter: dict = {}, projection: Union[List, Dict] = []):
        """Find all matching records."""
        if self.dbms == 'mongodb':
            return self.db[table].find(filter, projection)
        
        with self.get_connection() as conn:
            where_clause = ' AND '.join([f"{k} = %s" for k in filter.keys()]) if filter else '1=1'
            proj_str = ', '.join(projection) if isinstance(projection, list) and projection else '*'
            sql = f"SELECT {proj_str} FROM {table} WHERE {where_clause}"
            return self._execute_sql(conn, sql, tuple(filter.values()))

    def find_one(self, table: str, filter: dict = {}, projection: Union[List, Dict] = []):
        """Find a single record."""
        if self.dbms == 'mongodb':
            return self.db[table].find_one(filter, projection)
        
        with self.get_connection() as conn:
            where_clause = ' AND '.join([f"{k} = %s" for k in filter.keys()]) if filter else '1=1'
            proj_str = ', '.join(projection) if isinstance(projection, list) and projection else '*'
            sql = f"SELECT {proj_str} FROM {table} WHERE {where_clause} LIMIT 1"
            results = self._execute_sql(conn, sql, tuple(filter.values()))
            return results[0] if results else None

    def remove(self, table: str, filter: dict):
        """Remove matching records."""
        if self.dbms == 'mongodb':
            return self.db[table].delete_many(filter)
        
        with self.get_connection() as conn:
            where_clause = ' AND '.join([f"{k} = %s" for k in filter.keys()])
            sql = f"DELETE FROM {table} WHERE {where_clause}"
            return self._execute_sql(conn, sql, tuple(filter.values()), fetch=False)

    def delete(self, table: str, filter: dict):
        """Alias for remove."""
        return self.remove(table, filter)

    def update(self, table: str, filter: dict, data: dict):
        """Update matching records."""
        if self.dbms == 'mongodb':
            return self.db[table].update_many(filter, {'$set': data})
        
        with self.get_connection() as conn:
            set_clause = ', '.join([f"{k} = %s" for k in data.keys()])
            where_clause = ' AND '.join([f"{k} = %s" for k in filter.keys()])
            sql = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
            return self._execute_sql(conn, sql, tuple(data.values()) + tuple(filter.values()), fetch=False)

    def update_many(self, table: str, filter: dict, data: dict):
        """Alias for update as it handles multiple records by default."""
        return self.update(table, filter, data)

    def count(self, table: str, filter: dict = {}):
        """Count matching records."""
        if self.dbms == 'mongodb':
            return self.db[table].count_documents(filter)
        
        with self.get_connection() as conn:
            where_clause = ' AND '.join([f"{k} = %s" for k in filter.keys()]) if filter else '1=1'
            sql = f"SELECT COUNT(*) as count FROM {table} WHERE {where_clause}"
            results = self._execute_sql(conn, sql, tuple(filter.values()))
            return results[0]['count'] if results else 0

    def sum(self, table: str, column: str, params: dict = {}):
        """Sum values in a column."""
        if self.dbms == 'mongodb':
            pipeline = [
                {'$match': params},
                {'$group': {'_id': None, 'total': {'$sum': f'${column}'}}}
            ]
            result = list(self.db[table].aggregate(pipeline))
            return result[0]['total'] if result else 0
        
        with self.get_connection() as conn:
            where_clause = ' AND '.join([f"{k} = %s" for k in params.keys()]) if params else '1=1'
            sql = f"SELECT SUM({column}) as total FROM {table} WHERE {where_clause}"
            results = self._execute_sql(conn, sql, tuple(params.values()))
            return results[0]['total'] if results else 0

    def execute(self, query: str, params: tuple = ()):
        """Execute raw SQL query."""
        if self.dbms == 'mongodb':
            return None  # MongoDB doesn't support SQL
        
        with self.get_connection() as conn:
            return self._execute_sql(conn, query, params)

    def import_from_file(self, filename: str):
        """Import data from a file."""
        pass  # Implement based on specific needs
    
    def command(self, command: str, table: str):
        """Execute a database-specific command."""
        if self.dbms == 'mongodb':
            return self.db.command(command, table)
        return None  # SQL databases handle commands through execute()