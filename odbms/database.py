from typing import Dict, List, Any, Optional, Union

class Database:
    """Base class for database implementations."""
    
    def __init__(self, **kwargs):
        """Initialize database connection."""
        self.dbms = kwargs.get('dbms', None)
        self.connection = None
        self.config = kwargs
    
    async def connect(self) -> None:
        """Connect to the database."""
        raise NotImplementedError
    
    async def disconnect(self) -> None:
        """Disconnect from the database."""
        raise NotImplementedError
    
    async def query(self, query: str, params: Optional[Dict[str, Any]] = None, **kwargs: Dict[str, Any]) -> Any:
        """Execute a raw query."""
        raise NotImplementedError

    async def find_one(self, table: str, conditions: Dict[str, Any], **kwargs: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Find a single record matching conditions."""
        raise NotImplementedError

    async def find(self, table: str, conditions: Optional[Dict[str, Any]] = None,
                  skip: int = 0, limit: int = 100, sort: Optional[List[tuple]] = None, **kwargs: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Find multiple records matching conditions."""
        raise NotImplementedError

    async def insert_one(self, table: str, data: Dict[str, Any], **kwargs: Dict[str, Any]) -> Any:
        """Insert a record."""
        raise NotImplementedError
    
    async def insert_many(self, table: str, data: List[Dict[str, Any]], **kwargs: Dict[str, Any]) -> int:
        """Insert multiple records."""
        raise NotImplementedError
    
    async def update_one(self, table: str, conditions: Dict[str, Any], data: Dict[str, Any], upsert: bool = False, **kwargs: Dict[str, Any]) -> int:
        """Update a single record matching conditions."""
        raise NotImplementedError
    
    async def update_many(self, table: str, conditions: Dict[str, Any], data: Dict[str, Any], upsert: bool = False, **kwargs: Dict[str, Any]) -> int:
        """Update multiple records matching conditions."""
        raise NotImplementedError
    
    async def delete_one(self, table: str, conditions: Dict[str, Any], **kwargs: Dict[str, Any]) -> int:
        """Delete a single record matching conditions."""
        raise NotImplementedError

    async def delete_many(self, table: str, conditions: Dict[str, Any], **kwargs: Dict[str, Any]) -> int:
        """Delete multiple records matching conditions."""
        raise NotImplementedError
    
    async def sum(self, table: str, column: str, conditions: Optional[Dict[str, Any]] = None, **kwargs: Dict[str, Any]) -> Union[int, float]:
        """Sum values in a column."""
        raise NotImplementedError
    
    async def count(self, table: str, conditions: Optional[Dict[str, Any]] = None, **kwargs: Dict[str, Any]) -> int:
        """Count documents."""
        raise NotImplementedError
    
    async def aggregate(self, table: str, pipeline: List[Dict[str, Any]], **kwargs: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Perform aggregation."""
        raise NotImplementedError
