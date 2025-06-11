from typing import TypeVar, Generic, Type, List, Optional, Any, Dict
from motor.motor_asyncio import AsyncIOMotorCollection
from pydantic import BaseModel
from bson import ObjectId
from app.core.database import get_collection

ModelType = TypeVar("ModelType", bound=BaseModel)

class BaseRepository(Generic[ModelType]):
    def __init__(self, collection_name: str, model_class: Type[ModelType]):
        self.collection: AsyncIOMotorCollection = get_collection(collection_name)
        self.model_class = model_class

    async def find_one(self, filter_dict: dict) -> Optional[ModelType]:
        """Find a single document and return as a Pydantic model"""
        result = await self.collection.find_one(filter_dict)
        if result:
            return self.model_class(**result)
        return None

    async def find_many(self, 
                       filter_dict: Optional[dict] = None, 
                       skip: int = 0, 
                       limit: int = 100,
                       sort: Optional[List[tuple]] = None) -> List[ModelType]:
        """Find multiple documents and return as Pydantic models"""
        cursor = self.collection.find(filter_dict or {})
        
        if sort:
            cursor = cursor.sort(sort)
        
        cursor = cursor.skip(skip).limit(limit)
        return [self.model_class(**doc) async for doc in cursor]

    async def insert_one(self, model: ModelType) -> str:
        """Insert a single document"""
        doc = model.model_dump(by_alias=True, exclude={"id"} if model.id is None else set())
        result = await self.collection.insert_one(doc)
        return str(result.inserted_id)

    async def insert_many(self, models: List[ModelType]) -> List[str]:
        """Insert multiple documents"""
        docs = [
            model.model_dump(by_alias=True, exclude={"id"} if model.id is None else set())
            for model in models
        ]
        result = await self.collection.insert_many(docs)
        return [str(id) for id in result.inserted_ids]

    async def update_one(self, 
                        filter_dict: dict, 
                        update_dict: dict,
                        upsert: bool = False) -> bool:
        """Update a single document"""
        result = await self.collection.update_one(
            filter_dict,
            {"$set": update_dict},
            upsert=upsert
        )
        return result.modified_count > 0

    async def update_many(self, 
                         filter_dict: dict, 
                         update_dict: dict,
                         upsert: bool = False) -> int:
        """Update multiple documents"""
        result = await self.collection.update_many(
            filter_dict,
            {"$set": update_dict},
            upsert=upsert
        )
        return result.modified_count

    async def delete_one(self, filter_dict: dict) -> bool:
        """Delete a single document"""
        result = await self.collection.delete_one(filter_dict)
        return result.deleted_count > 0

    async def delete_many(self, filter_dict: dict) -> int:
        """Delete multiple documents"""
        result = await self.collection.delete_many(filter_dict)
        return result.deleted_count

    async def count(self, filter_dict: Optional[dict] = None) -> int:
        """Count documents matching the filter"""
        return await self.collection.count_documents(filter_dict or {})

    async def aggregate(self, pipeline: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Perform an aggregation pipeline"""
        return await self.collection.aggregate(pipeline).to_list(None)

    async def find_by_id(self, id: str) -> Optional[ModelType]:
        """Find a document by its ID"""
        if not ObjectId.is_valid(id):
            return None
        return await self.find_one({"_id": ObjectId(id)}) 
