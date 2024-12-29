from typing import Optional, Dict, Any, List
import psycopg2
import psycopg2.extras
from ..dbms import Database

class PostgresqlDB(Database):
    """PostgreSQL database implementation."""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.dbms = 'postgresql'
        self.cursor = None
    
    def connect(self):
        """Connect to PostgreSQL."""
        self.connection = psycopg2.connect(
            host=self.config.get('host', 'localhost'),
            port=self.config.get('port', 5432),
            database=self.config['database'],
            user=self.config.get('user', 'postgres'),
            password=self.config.get('password', '')
        )
        self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    def disconnect(self):
        """Disconnect from PostgreSQL."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
    
    def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a query."""
        if self.cursor is None:
            raise RuntimeError("Database not connected")
        self.cursor.execute(query, params or {})
        return self.cursor
    
    def find(self, table: str, params: Optional[Dict[str, Any]] = None) -> List[Dict]:
        """Find records matching params."""
        if self.cursor is None:
            raise RuntimeError("Database not connected")
        
        query = f"SELECT * FROM {table}"
        if params:
            conditions = " AND ".join(f"{k} = %({k})s" for k in params.keys())
            query += f" WHERE {conditions}"
        
        self.cursor.execute(query, params or {})
        return [dict(row) for row in self.cursor.fetchall()]
    
    def find_one(self, table: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict]:
        """Find one record matching params."""
        if self.cursor is None:
            raise RuntimeError("Database not connected")
        
        query = f"SELECT * FROM {table}"
        if params:
            conditions = " AND ".join(f"{k} = %({k})s" for k in params.keys())
            query += f" WHERE {conditions} LIMIT 1"
        
        self.cursor.execute(query, params or {})
        # RealDictCursor returns a dict or None
        return self.cursor.fetchone()
    
    def insert(self, table: str, data: dict) -> Any:
        """Insert a record."""
        if self.cursor is None:
            raise RuntimeError("Database not connected")
        
        columns = ", ".join(data.keys())
        placeholders = ", ".join(f"%({k})s" for k in data.keys())
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) RETURNING id"
        
        self.cursor.execute(query, data)
        self.connection.commit()
        result = self.cursor.fetchone()
        return result['id'] if result else None
    
    def insert_many(self, table: str, data: List[dict]) -> Any:
        """Insert multiple records."""
        if self.cursor is None:
            raise RuntimeError("Database not connected")
        
        if not data:
            return None
        
        columns = ", ".join(data[0].keys())
        placeholders = ", ".join(f"%({k})s" for k in data[0].keys())
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        
        self.cursor.executemany(query, data)
        self.connection.commit()
        return self.cursor.rowcount
    
    def update(self, table: str, params: dict, data: dict) -> Any:
        """Update records matching params."""
        if self.cursor is None:
            raise RuntimeError("Database not connected")
        
        set_values = ", ".join(f"{k} = %({k})s" for k in data.keys())
        conditions = " AND ".join(f"{k} = %(where_{k})s" for k in params.keys())
        query = f"UPDATE {table} SET {set_values} WHERE {conditions}"
        
        # Prefix param keys with 'where_' to avoid conflicts
        params_with_prefix = {f"where_{k}": v for k, v in params.items()}
        self.cursor.execute(query, {**data, **params_with_prefix})
        self.connection.commit()
        return self.cursor.rowcount
    
    def remove(self, table: str, params: dict) -> Any:
        """Remove records matching params."""
        if self.cursor is None:
            raise RuntimeError("Database not connected")
        
        conditions = " AND ".join(f"{k} = %({k})s" for k in params.keys())
        query = f"DELETE FROM {table} WHERE {conditions}"
        
        self.cursor.execute(query, params)
        self.connection.commit()
        return self.cursor.rowcount