from typing import Dict, List, Any, Optional
import pymongo

from ..database import Database

class MongoDB(Database):
    """MongoDB database implementation."""
    
    def __init__(self, host: str = 'localhost', port: int = 27017, database: str = 'test'):
        """Initialize MongoDB connection."""
        super().__init__(host=host, port=port, database=database)
        self.client = None
        self.db = None
    
    def connect(self) -> None:
        """Connect to MongoDB."""
        self.client = pymongo.MongoClient(
            host=self.config['host'],
            port=self.config['port']
        )
        self.db = self.client[self.config['database']]
    
    def disconnect(self) -> None:
        """Disconnect from MongoDB."""
        if self.client:
            self.client.close()
            self.client = None
            self.db = None
    
    def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a MongoDB command."""
        if self.db is None:
            raise RuntimeError("Database not connected")
        return self.db.command(query, params)
    
    def find(self, table: str, conditions: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Find records matching conditions."""
        if self.db is None:
            raise RuntimeError("Database not connected")
        return list(self.db[table].find(conditions or {}))
    
    def find_one(self, table: str, conditions: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Find a single record matching conditions."""
        if self.db is None:
            raise RuntimeError("Database not connected")
        return self.db[table].find_one(conditions)
    
    def insert(self, table: str, data: Dict[str, Any]) -> Any:
        """Insert a record."""
        if self.db is None:
            raise RuntimeError("Database not connected")
        result = self.db[table].insert_one(data)
        return str(result.inserted_id)
    
    def insert_many(self, table: str, data: List[Dict[str, Any]]) -> int:
        """Insert multiple records."""
        if self.db is None:
            raise RuntimeError("Database not connected")
        result = self.db[table].insert_many(data)
        return len(result.inserted_ids)
    
    def update(self, table: str, conditions: Dict[str, Any], data: Dict[str, Any]) -> int:
        """Update records matching conditions."""
        if self.db is None:
            raise RuntimeError("Database not connected")
        result = self.db[table].update_many(conditions, {'$set': data})
        return result.modified_count
    
    def remove(self, table: str, conditions: Dict[str, Any]) -> int:
        """Remove records matching conditions."""
        if self.db is None:
            raise RuntimeError("Database not connected")
        result = self.db[table].delete_many(conditions)
        return result.deleted_count

