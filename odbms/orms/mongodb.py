from typing import Dict, List, Any, Optional, Union
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from bson import ObjectId
from pydantic import BaseModel

from ..database import Database

class MongoDB(Database):
    """MongoDB database implementation."""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.dbms = 'mongodb'
        self.client: Optional[AsyncIOMotorClient] = None
        self.db: Optional[AsyncIOMotorDatabase] = None
        self._loop = None
    
    def _convert_id(self, conditions: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Convert string _id to ObjectId."""
        if not conditions:
            return {}
        
        conditions = conditions.copy()
        if '_id' in conditions:
            if not isinstance(conditions['_id'], ObjectId):
                try:
                    conditions['_id'] = ObjectId(conditions['_id'])
                except (TypeError, ValueError):
                    # If conversion fails, keep original value
                    pass
        return conditions

    def connect(self):
        """Connect to MongoDB."""
        host = self.config.get('host', 'localhost')
        port = self.config.get('port', 27017)
        database = self.config['database']
        
        self.client = AsyncIOMotorClient(host=host, port=port)
        self.db = self.client[database]
    
    def disconnect(self):
        """Disconnect from MongoDB."""
        if self.client is not None:
            self.client.close()
            self.client = None
            self.db = None
    
    async def find(self, table: str, conditions: Optional[Dict[str, Any]] = None,
                  skip: int = 0, limit: int = 100, sort: Optional[List[tuple]] = None) -> List[Dict[str, Any]]:
        """Find records matching conditions with pagination and sorting."""
        if self.db is None:
            raise RuntimeError("Database not connected")
        conditions = self._convert_id(conditions)
        cursor = self.db[table].find(conditions)
        if sort:
            cursor = cursor.sort(sort)
        cursor = cursor.skip(skip).limit(limit)
        return await cursor.to_list(length=None)
    
    async def find_one(self, table: str, conditions: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Find a single record matching conditions."""
        if self.db is None:
            raise RuntimeError("Database not connected")
        conditions = self._convert_id(conditions)
        return await self.db[table].find_one(conditions)
    
    async def insert_one(self, table: str, data: Dict[str, Any]) -> Any:
        """Insert a record."""
        if self.db is None:
            raise RuntimeError("Database not connected")
        result = await self.db[table].insert_one(data)
        return result.inserted_id
    
    async def insert_many(self, table: str, data: List[Dict[str, Any]]) -> int:
        """Insert multiple records."""
        if self.db is None:
            raise RuntimeError("Database not connected")
        result = await self.db[table].insert_many(data)
        return len(result.inserted_ids)
    
    async def update_one(self, table: str, conditions: Dict[str, Any], data: Dict[str, Any], upsert: bool = False, **kwargs: Dict[str, Any]) -> int:
        """Update a single record matching conditions."""
        if self.db is None:
            raise RuntimeError("Database not connected")
        conditions = self._convert_id(conditions)
        result = await self.db[table].update_one(conditions, {'$set': data}, upsert=upsert)
        return result.modified_count
    
    async def update_many(self, table: str, conditions: Dict[str, Any], data: Dict[str, Any], upsert: bool = False, **kwargs: Dict[str, Any]) -> int:
        """Update multiple records matching conditions."""
        if self.db is None:
            raise RuntimeError("Database not connected")
        conditions = self._convert_id(conditions)
        result = await self.db[table].update_many(conditions, {'$set': data}, upsert=upsert)
        return result.modified_count
    

    async def delete_one(self, table: str, conditions: Dict[str, Any]) -> int:
        """Delete a single record matching conditions."""
        if self.db is None:
            raise RuntimeError("Database not connected")
        conditions = self._convert_id(conditions)
        result = await self.db[table].delete_one(conditions)
        return result.deleted_count
    
    async def delete_many(self, table: str, conditions: Dict[str, Any]) -> int:
        """Delete multiple records matching conditions."""
        if self.db is None:
            raise RuntimeError("Database not connected")
        conditions = self._convert_id(conditions)
        result = await self.db[table].delete_many(conditions)
        return result.deleted_count
    
    async def sum(self, table: str, column: str, conditions: Optional[Dict[str, Any]] = None) -> Union[int, float]:
        """Sum values in a column."""
        if self.db is None:
            raise RuntimeError("Database not connected")
        conditions = self._convert_id(conditions)
        pipeline = [
            {'$match': conditions or {}},
            {'$group': {'_id': None, 'total': {'$sum': f'${column}'}}}
        ]
        
        result = await self.db[table].aggregate(pipeline).to_list(length=1)
        return float(result[0]['total']) if result else 0

    async def count(self, table: str, conditions: Optional[Dict[str, Any]] = None) -> int:
        """Count documents."""
        if self.db is None:
            raise RuntimeError("Database not connected")
        conditions = self._convert_id(conditions)
        return await self.db[table].count_documents(conditions or {})

    async def aggregate(self, table: str, pipeline: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Perform aggregation."""
        if self.db is None:
            raise RuntimeError("Database not connected")
        return await self.db[table].aggregate(pipeline).to_list(None)