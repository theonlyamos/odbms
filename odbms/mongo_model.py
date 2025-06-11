from typing import Any, List, Optional, Dict, TypeVar, Type, ClassVar
from datetime import datetime
from pydantic import Field, BaseModel
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorCollection
from app.core.database import get_collection

T = TypeVar('T', bound='MongoModel')

class PyObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid ObjectId")
        return ObjectId(v)

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(type="string")

class MongoModel(BaseModel):
    id: Optional[PyObjectId] = Field(alias="_id", default=None)
    
    # Class variable to store collection name
    collection_name: ClassVar[str]
    
    class Config:
        json_encoders = {ObjectId: str}
        populate_by_name = True
        arbitrary_types_allowed = True

    @classmethod
    def _get_collection(cls) -> AsyncIOMotorCollection:
        return get_collection(cls.collection_name)

    @classmethod
    async def find_one(cls: Type[T], filter_dict: dict) -> Optional[T]:
        """Find a single document"""
        result = await cls._get_collection().find_one(filter_dict)
        if result:
            return cls(**result)
        return None

    @classmethod
    async def find_many(cls: Type[T], 
                       filter_dict: Optional[dict] = None, 
                       skip: int = 0, 
                       limit: int = 100,
                       sort: Optional[List[tuple]] = None) -> List[T]:
        """Find multiple documents"""
        cursor = cls._get_collection().find(filter_dict or {})
        if sort:
            cursor = cursor.sort(sort)
        cursor = cursor.skip(skip).limit(limit)
        return [cls(**doc) async for doc in cursor]

    @classmethod
    async def find_by_id(cls: Type[T], id: str) -> Optional[T]:
        """Find document by ID"""
        if not ObjectId.is_valid(id):
            return None
        return await cls.find_one({"_id": ObjectId(id)})

    async def insert(self) -> str:
        """Insert this model instance"""
        doc = self.model_dump(by_alias=True, exclude={"id"} if self.id is None else set())
        result = await self._get_collection().insert_one(doc)
        self.id = result.inserted_id
        return str(result.inserted_id)

    @classmethod
    async def insert_many(cls: Type[T], models: List[T]) -> List[str]:
        """Insert multiple models"""
        docs = [
            model.model_dump(by_alias=True, exclude={"id"} if model.id is None else set())
            for model in models
        ]
        result = await cls._get_collection().insert_many(docs)
        return [str(id) for id in result.inserted_ids]

    async def update(self, update_dict: dict, upsert: bool = False) -> bool:
        """Update this model instance"""
        if self.id is None:
            raise ValueError("Cannot update a model with no ID")
        result = await self._get_collection().update_one(
            {"_id": self.id},
            {"$set": update_dict},
            upsert=upsert
        )
        return result.modified_count > 0

    async def delete(self) -> bool:
        """Delete this model instance"""
        if self.id is None:
            raise ValueError("Cannot delete a model with no ID")
        result = await self._get_collection().delete_one({"_id": self.id})
        return result.deleted_count > 0

    @classmethod
    async def delete_many(cls, filter_dict: dict) -> int:
        """Delete multiple documents"""
        result = await cls._get_collection().delete_many(filter_dict)
        return result.deleted_count

    @classmethod
    async def count(cls, filter_dict: Optional[dict] = None) -> int:
        """Count documents"""
        return await cls._get_collection().count_documents(filter_dict or {})

    @classmethod
    async def aggregate(cls, pipeline: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Perform aggregation"""
        return await cls._get_collection().aggregate(pipeline).to_list(None)

class InstagramPost(MongoModel):
    collection_name = "instagram_posts"
    
    postId: str
    userId: str
    username: str
    full_name: Optional[str]
    is_private: bool
    is_verified: bool
    
    # Post info
    mediaId: str
    shortcode: str
    taken_at: datetime
    media_type: str|int
    product_type: Optional[str]
    thumbnail_url: Optional[str]
    display_url: Optional[str]
    
    # Collaboration info
    is_collaboration: bool = False
    tagged_accounts: List[str] = []
    tagged_verified_accounts: int = 0
    
    # Engagement metrics
    like_count: int = 0
    comment_count: int = 0
    play_count: Optional[int] = 0
    view_count: Optional[int] = 0
    reshare_count: Optional[int] = 0
    fb_aggregated_like_count: Optional[int] = 0
    fb_aggregated_comment_count: Optional[int] = 0
    engagement_rate: float = 0.0
    total_engagement: Optional[int] = 0
    
    # Content
    caption: Optional[Dict] = None
    
    # Video specific
    video_duration: Optional[float] = None
    video_url: Optional[str] = None
    
    # Meta
    comments_disabled: bool = False
    is_paid_partnership: bool = False
    permalink: str = Field(default_factory=lambda: "")
    
    product_tags: Optional[List[Dict[str, Any]]] = None
    top_likers: Optional[List[str]] = []
    shop_routing_userId: Optional[str] = None
    
    ai_generated_caption: Optional[str] = None
    ai_analysis: Optional[Dict] = None

    @classmethod
    async def find_by_username(cls, username: str, limit: int = 50):
        """Find posts by username"""
        return await cls.find_many(
            {"username": username}, 
            limit=limit,
            sort=[("taken_at", -1)]
        )
    
    @classmethod
    async def find_top_performing(cls, username: str, limit: int = 10):
        """Find top performing posts"""
        return await cls.find_many(
            {"username": username},
            limit=limit,
            sort=[("engagement_rate", -1)]
        )

    @staticmethod
    def calculate_engagement_rate(post_data: dict, follower_count: int) -> dict:
        if not follower_count:
            post_data['engagement_rate'] = 0.0
            return post_data
        
        total_engagement = post_data.get('like_count', 0) + post_data.get('comment_count', 0)
        
        if post_data.get('media_type') == 2:
            video_views = post_data.get('play_count', 0)
            total_engagement += video_views
        
        engagement_rate = (total_engagement / follower_count) * 100
        post_data['engagement_rate'] = engagement_rate
        post_data['total_engagement'] = total_engagement
        
        return post_data

class InstagramProfile(MongoModel):
    collection_name = "instagram_profiles"
    
    userId: str
    username: str
    profile_pic_url: Optional[str]
    account_type: Optional[int|str]
    is_verified: bool = False
    full_name: Optional[str]
    biography: Optional[str]
    is_business: bool = False
    follower_count: int = 0
    following_count: int = 0
    media_count: int = 0
    city_name: Optional[str]
    category: Optional[str]
    public_email: Optional[str]
    contact_phone_number: Optional[str]
    
    location: Dict = Field(default_factory=lambda: {
        "latitude": None,
        "longitude": None,
        "city_id": None,
        "address_street": None,
        "zip": None
    })
    
    business_info: Dict = Field(default_factory=lambda: {
        "business_contact_method": None,
        "instagram_location_id": None
    })
    
    posts: List[InstagramPost] = []

    @classmethod
    async def find_by_username(cls, username: str) -> Optional['InstagramProfile']:
        """Find profile by username"""
        return await cls.find_one({"username": username})

    @classmethod
    async def find_business_accounts(cls):
        """Find all business accounts"""
        return await cls.find_many({"is_business": True})

class InstagramFollower(MongoModel):
    collection_name = "instagram_followers"
    
    userId: str
    username: str
    full_name: Optional[str]
    is_private: bool = False
    is_verified: bool = False
    profile_pic_url: Optional[str]
    followed_at: datetime = Field(default_factory=datetime.now)

class InstagramAnalytics(MongoModel):
    collection_name = "instagram_analytics"
    
    best_posting_times: List[Dict[str, Any]] = Field(
        description="List of best posting times with engagement metrics",
        default=[]
    )

class CaptionAnalysis(MongoModel):
    collection_name = "caption_analysis"
    
    main_topics: List[str]
    tone: str
    style: str
    keywords: List[str]
    sentiment: str
    engagement_potential: float
    call_to_action: Optional[str]
    hashtag_effectiveness: str
    suggestions: List[str]

class UGMAnalysis(MongoModel):
    collection_name = "ugm_analysis"
    
    username: str
    content_analysis: Dict[str, Any] = Field(
        description="Analysis of content patterns and quality",
        default_factory=lambda: {
            "dominant_themes": [],
            "visual_styles": [],
            "quality_assessment": {
                "average_score": 0,
                "strengths": [],
                "weaknesses": []
            },
            "platform_optimization": []
        }
    )
    audience_insights: Dict[str, Any] = Field(
        description="Analysis of audience behavior and engagement",
        default_factory=lambda: {
            "engagement_patterns": {
                "high_performing_elements": [],
                "low_performing_elements": [],
                "optimal_content_types": []
            },
            "sentiment_trends": {
                "overall_sentiment": "",
                "positive_triggers": [],
                "negative_triggers": []
            },
            "community_behavior": {
                "interaction_types": [],
                "response_patterns": []
            }
        }
    )
    brand_representation: Dict[str, Any] = Field(
        description="Analysis of brand presence and consistency",
        default_factory=lambda: {
            "consistency_score": 0,
            "message_alignment": {
                "strengths": [],
                "gaps": []
            },
            "visual_cohesion": {
                "score": 0,
                "observations": []
            }
        }
    )
    authenticity_assessment: Dict[str, Any] = Field(
        description="Assessment of content authenticity and trust",
        default_factory=lambda: {
            "overall_score": 0,
            "trust_indicators": [],
            "concern_areas": [],
            "community_trust_level": "medium"
        }
    )
    strategic_recommendations: Dict[str, Any] = Field(
        description="Strategic insights and recommendations",
        default_factory=lambda: {
            "content_opportunities": [],
            "engagement_tactics": [],
            "collaboration_suggestions": [],
            "improvement_areas": [],
            "priority_actions": []
        }
    )
    trend_analysis: Dict[str, Any] = Field(
        description="Analysis of emerging trends and patterns",
        default_factory=lambda: {
            "emerging_patterns": [],
            "declining_elements": [],
            "future_opportunities": []
        }
    )

class AnalyticsReport(MongoModel):
    client_username: str
    competitor_username: Optional[str]
    report_status: str = "pending"  # pending, processing, completed, failed
    report_url: Optional[str] = None
    error_message: Optional[str] = None
    
    # Analytics data
    engagement_rate: Optional[float] = None
    top_performing_posts: Optional[List[InstagramPost]] = None
    top_performing_offers: Optional[List[InstagramPost]] = None
    
    # Analysis data
    client_analysis: Optional[Dict] = None
    competitor_analysis: Optional[Dict] = None
    ai_insights: Optional[Dict] = None 

