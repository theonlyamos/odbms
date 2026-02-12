"""
SQL utilities for safe identifier handling and query building.
"""
import re
from typing import List, Union, Optional, Dict, Any, Tuple


class SQLIdentifier:
    """Safe SQL identifier handling with proper escaping."""
    
    @staticmethod
    def quote_identifier(identifier: str, dbms: str = 'postgresql') -> str:
        """
        Safely quote a SQL identifier (table name, column name).
        
        Args:
            identifier: The identifier to quote
            dbms: Database type ('postgresql', 'mysql', 'sqlite')
            
        Returns:
            Quoted identifier safe for SQL interpolation
        """
        if not identifier:
            raise ValueError("Identifier cannot be empty")
        
        identifier = str(identifier).strip()
        
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
            raise ValueError(f"Invalid identifier format: {identifier}")
        
        if dbms == 'postgresql':
            return f'"{identifier.replace(chr(34), chr(34)+chr(34))}"'
        elif dbms == 'mysql':
            return f'`{identifier.replace(chr(96), chr(96)+chr(96))}`'
        elif dbms == 'sqlite':
            return f'"{identifier.replace(chr(34), chr(34)+chr(34))}"'
        else:
            return f'"{identifier}"'
    
    @staticmethod
    def quote_identifiers(identifiers: List[str], dbms: str = 'postgresql') -> str:
        """Quote multiple identifiers and join with comma."""
        return ', '.join(SQLIdentifier.quote_identifier(i, dbms) for i in identifiers)


class QueryBuilder:
    """Fluent query builder for safe SQL construction."""
    
    def __init__(self, table: str, dbms: str = 'postgresql'):
        self._table = table
        self._dbms = dbms
        self._columns: List[str] = ['*']
        self._where: List[Tuple[str, str]] = []
        self._params: List[Any] = []
        self._order_by: Optional[str] = None
        self._limit: Optional[int] = None
        self._offset: Optional[int] = None
    
    def select(self, *columns: str) -> 'QueryBuilder':
        """Set columns to select."""
        if columns:
            self._columns = list(columns)
        return self
    
    def where(self, column: str, operator: str = '=', value: Any = None) -> 'QueryBuilder':
        """Add a WHERE condition."""
        quoted_col = SQLIdentifier.quote_identifier(column, self._dbms)
        self._where.append((quoted_col, operator))
        if value is not None:
            self._params.append(value)
        return self
    
    def where_in(self, column: str, values: List[Any]) -> 'QueryBuilder':
        """Add WHERE IN condition."""
        if not values:
            return self
        quoted_col = SQLIdentifier.quote_identifier(column, self._dbms)
        placeholders = ', '.join(['%s'] * len(values))
        self._where.append((quoted_col, f'IN ({placeholders})'))
        self._params.extend(values)
        return self
    
    def order_by(self, column: str, direction: str = 'ASC') -> 'QueryBuilder':
        """Set ORDER BY clause."""
        direction = direction.upper()
        if direction not in ('ASC', 'DESC'):
            raise ValueError("Direction must be ASC or DESC")
        quoted_col = SQLIdentifier.quote_identifier(column, self._dbms)
        self._order_by = f"{quoted_col} {direction}"
        return self
    
    def limit(self, limit: int) -> 'QueryBuilder':
        """Set LIMIT."""
        self._limit = limit
        return self
    
    def offset(self, offset: int) -> 'QueryBuilder':
        """Set OFFSET."""
        self._offset = offset
        return self
    
    def build_select(self) -> Tuple[str, tuple]:
        """Build SELECT query."""
        table = SQLIdentifier.quote_identifier(self._table, self._dbms)
        columns = SQLIdentifier.quote_identifiers(self._columns, self._dbms) if self._columns != ['*'] else '*'
        
        sql = f"SELECT {columns} FROM {table}"
        
        if self._where:
            conditions = ' AND '.join(f"{col} {op}" for col, op in self._where)
            sql += f" WHERE {conditions}"
        
        if self._order_by:
            sql += f" ORDER BY {self._order_by}"
        
        if self._limit is not None:
            sql += f" LIMIT {self._limit}"
        
        if self._offset is not None:
            sql += f" OFFSET {self._offset}"
        
        return sql, tuple(self._params)
    
    @staticmethod
    def build_insert(table: str, data: Dict[str, Any], dbms: str = 'postgresql') -> Tuple[str, tuple]:
        """Build INSERT query."""
        if not data:
            raise ValueError("No data to insert")
        
        quoted_table = SQLIdentifier.quote_identifier(table, dbms)
        columns = SQLIdentifier.quote_identifiers(list(data.keys()), dbms)
        placeholders = ', '.join(['%s'] * len(data))
        
        sql = f"INSERT INTO {quoted_table} ({columns}) VALUES ({placeholders})"
        return sql, tuple(data.values())
    
    @staticmethod
    def build_update(table: str, data: Dict[str, Any], conditions: Dict[str, Any], 
                     dbms: str = 'postgresql') -> Tuple[str, tuple]:
        """Build UPDATE query."""
        if not data:
            raise ValueError("No data to update")
        
        quoted_table = SQLIdentifier.quote_identifier(table, dbms)
        set_clause = ', '.join(
            f"{SQLIdentifier.quote_identifier(k, dbms)} = %s" 
            for k in data.keys()
        )
        
        params = list(data.values())
        
        where_parts = []
        for key, value in conditions.items():
            quoted_key = SQLIdentifier.quote_identifier(key, dbms)
            where_parts.append(f"{quoted_key} = %s")
            params.append(value)
        
        sql = f"UPDATE {quoted_table} SET {set_clause}"
        if where_parts:
            sql += f" WHERE {' AND '.join(where_parts)}"
        
        return sql, tuple(params)
    
    @staticmethod
    def build_delete(table: str, conditions: Dict[str, Any], 
                     dbms: str = 'postgresql') -> Tuple[str, tuple]:
        """Build DELETE query."""
        quoted_table = SQLIdentifier.quote_identifier(table, dbms)
        
        where_parts = []
        params = []
        for key, value in conditions.items():
            quoted_key = SQLIdentifier.quote_identifier(key, dbms)
            where_parts.append(f"{quoted_key} = %s")
            params.append(value)
        
        sql = f"DELETE FROM {quoted_table}"
        if where_parts:
            sql += f" WHERE {' AND '.join(where_parts)}"
        
        return sql, tuple(params)
