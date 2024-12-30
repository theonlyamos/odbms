from typing import Dict, List, Any, Optional, Union
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from bson import ObjectId

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
        if '_id' in conditions and not isinstance(conditions['_id'], ObjectId):
            conditions['_id'] = ObjectId(conditions['_id'])
        return conditions

    def connect(self):
        """Connect to MongoDB."""
        host = self.config.get('host', 'localhost')
        port = self.config.get('port', 27017)
        database = self.config['database']
        
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self.client = AsyncIOMotorClient(host=host, port=port)
        self.db = self.client[database]
    
    def disconnect(self):
        """Disconnect from MongoDB."""
        if self.client is not None:
            self.client.close()
            self.client = None
            self.db = None
        if self._loop is not None:
            self._loop.close()
            self._loop = None
    
    def _run_sync(self, coro):
        """Run coroutine synchronously."""
        if self._loop is None or self._loop.is_closed():
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
        try:
            return self._loop.run_until_complete(coro)
        except RuntimeError as e:
            if "Event loop is closed" in str(e):
                # Create new event loop and retry
                self._loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self._loop)
                return self._loop.run_until_complete(coro)
            raise
    
    def find(self, table: str, conditions: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Find records matching conditions."""
        conditions = self._convert_id(conditions)
        return self._run_sync(self.find_async(table, conditions))
    
    def find_one(self, table: str, conditions: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Find a single record matching conditions."""
        conditions = self._convert_id(conditions)
        return self._run_sync(self.find_one_async(table, conditions))
    
    def insert(self, table: str, data: Dict[str, Any]) -> Any:
        """Insert a record."""
        return self._run_sync(self.insert_async(table, data))
    
    def insert_many(self, table: str, data: List[Dict[str, Any]]) -> int:
        """Insert multiple records."""
        return self._run_sync(self.insert_many_async(table, data))
    
    def update(self, table: str, conditions: Dict[str, Any], data: Dict[str, Any]) -> int:
        """Update records matching conditions."""
        return self._run_sync(self.update_async(table, conditions, data))
    
    def remove(self, table: str, conditions: Dict[str, Any]) -> int:
        """Remove records matching conditions."""
        return self._run_sync(self.remove_async(table, conditions))
    
    def sum(self, table: str, column: str, conditions: Optional[Dict[str, Any]] = None) -> Union[int, float]:
        """Sum values in a column."""
        return self._run_sync(self.sum_async(table, column, conditions))

    async def find_async(self, table: str, conditions: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Find records matching conditions asynchronously."""
        if self.db is None:
            raise RuntimeError("Database not connected")
        conditions = self._convert_id(conditions)
        cursor = self.db[table].find(conditions)
        return await cursor.to_list(length=None)
    
    async def find_one_async(self, table: str, conditions: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Find a single record matching conditions asynchronously."""
        if self.db is None:
            raise RuntimeError("Database not connected")
        conditions = self._convert_id(conditions)
        return await self.db[table].find_one(conditions)
    
    async def insert_async(self, table: str, data: Dict[str, Any]) -> Any:
        """Insert a record asynchronously."""
        if self.db is None:
            raise RuntimeError("Database not connected")
        result = await self.db[table].insert_one(data)
        return str(result.inserted_id)
    
    async def insert_many_async(self, table: str, data: List[Dict[str, Any]]) -> int:
        """Insert multiple records asynchronously."""
        if self.db is None:
            raise RuntimeError("Database not connected")
        result = await self.db[table].insert_many(data)
        return len(result.inserted_ids)
    
    async def update_async(self, table: str, conditions: Dict[str, Any], data: Dict[str, Any]) -> int:
        """Update records matching conditions asynchronously."""
        if self.db is None:
            raise RuntimeError("Database not connected")
        conditions = self._convert_id(conditions)
        result = await self.db[table].update_many(conditions, {'$set': data})
        return result.modified_count
    
    async def remove_async(self, table: str, conditions: Dict[str, Any]) -> int:
        """Remove records matching conditions asynchronously."""
        if self.db is None:
            raise RuntimeError("Database not connected")
        conditions = self._convert_id(conditions)
        result = await self.db[table].delete_many(conditions)
        return result.deleted_count
    
    async def sum_async(self, table: str, column: str, conditions: Optional[Dict[str, Any]] = None) -> Union[int, float]:
        """Sum values in a column asynchronously."""
        if self.db is None:
            raise RuntimeError("Database not connected")
        conditions = self._convert_id(conditions)
        pipeline = [
            {'$match': conditions or {}},
            {'$group': {'_id': None, 'total': {'$sum': f'${column}'}}}
        ]
        
        result = await self.db[table].aggregate(pipeline).to_list(length=1)
        return float(result[0]['total']) if result else 0

