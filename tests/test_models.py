import pytest
from datetime import datetime
from typing import Optional, List, Union

from odbms.dbms import DBMS
from odbms.model import Model
from pydantic import Field
import asyncio

class Address(Model):
    street: str = Field(default=...)
    city: str = Field(default=...)
    country: str = Field(default=...)
    postal_code: Optional[str] = Field(default=None)

class Post(Model):
    title: str = Field(default=...)
    content: str = Field(default=...)
    published: bool = Field(default=False)

class User(Model):
    name: str = Field(default=...)
    email: str = Field(default=..., pattern=r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
    age: Optional[int] = Field(default=None, ge=0)
    address_id: Optional[Union[str, int]] = Field(default=None)
    posts_ids: List[Union[str, int]] = Field(default_factory=list)
    
    def __init__(self, **data):
        super().__init__(**data)
        self._address: Optional[Address] = None
        self._posts: List[Post] = []
    
    @property
    async def address(self) -> Optional[Address]:
        if self._address is None and self.address_id is not None:
            self._address = await Address.get(self.address_id)
        return self._address
    
    @address.setter
    def address(self, value: Optional[Address]) -> None:
        self._address = value
        if value is not None:
            setattr(self, 'address_id', value.id)
        else:
            setattr(self, 'address_id', None)
    
    @property
    async def posts(self) -> List[Post]:
        if not self._posts and self.posts_ids:
            posts = [await Post.get(post_id) for post_id in self.posts_ids]
            self._posts = [post for post in posts if post is not None]
        return self._posts
    
    @posts.setter
    def posts(self, value: List[Post]) -> None:
        self._posts = value
        setattr(self, 'posts_ids', [post.id for post in value if post is not None])

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for each test case."""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope="session")
def database():
    """Initialize database for testing."""
    DBMS.initialize(
        dbms='mongodb',
        database='test_db',
        host='localhost',
        port=27017
    )
    
    # Create tables
    if DBMS.Database is not None and DBMS.Database.dbms != 'mongodb':
        # Create addresses table first
        DBMS.Database.query("""
            CREATE TABLE IF NOT EXISTS addresses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                street TEXT NOT NULL,
                city TEXT NOT NULL,
                country TEXT NOT NULL,
                postal_code TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
        """)
        
        # Create users table second (since posts will reference it)
        DBMS.Database.query("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                age INTEGER,
                address_id INTEGER,
                posts_ids TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                FOREIGN KEY (address_id) REFERENCES addresses (id)
            )
        """)
        
        # Create posts table last
        DBMS.Database.query("""
            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                published BOOLEAN DEFAULT 0,
                user_id INTEGER,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (id)
            )
        """)
    
    yield DBMS.Database
    
    # Cleanup
    if DBMS.Database is not None:
        if DBMS.Database.dbms != 'mongodb':
            DBMS.Database.query("DROP TABLE IF EXISTS posts")
            DBMS.Database.query("DROP TABLE IF EXISTS users")
            DBMS.Database.query("DROP TABLE IF EXISTS addresses")
        DBMS.Database.disconnect()

@pytest.fixture
def test_data():
    """Test data fixture."""
    return {
        'address_data': {
            'street': '123 Main St',
            'city': 'Test City',
            'country': 'Test Country',
            'postal_code': '12345'
        },
        'user_data': {
            'name': 'John Doe',
            'email': 'john@example.com',
            'age': 30
        },
        'post_data': {
            'title': 'Test Post',
            'content': 'Test Content',
            'published': True
        }
    }

@pytest.fixture(autouse=True)
async def cleanup(database):
    """Clean up after each test."""
    print('\n======Starting test cleanup========')
    try:
        yield
    finally:
        print('======Cleaning up test data========')
        try:
            r = await Address.delete_many({})
            print('======Deleted Addresses========', r)
            r = await Post.delete_many({})
            print('======Deleted Posts========', r)
            r = await User.delete_many({})
            print('======Deleted Users========', r)
            print('======Cleanup complete========\n')
        except Exception as e:
            print('======Cleanup error========', str(e))
        finally:
            await database.disconnect()  # Ensure database is disconnected

def test_field_validation(test_data):
    """Test field validation."""
    # Test required field
    with pytest.raises(ValueError):
        User(email='test@example.com', age=25)  # Missing required name
    
    # Test email validation
    with pytest.raises(ValueError):
        User(name='Test', email='invalid-email', age=25)
    
    # Test min value validation
    with pytest.raises(ValueError):
        User(name='Test', email='test@example.com', age=-1)
    
    # Test valid data
    user = User(**test_data['user_data'])
    assert user.name == 'John Doe'
    assert user.email == 'john@example.com'
    assert user.age == 30

@pytest.mark.asyncio
async def test_model_methods(test_data):
    """Test model utility methods."""
    # Test table name generation

    assert User.table_name() == 'users'
    assert Address.table_name() == 'addresses'
    
    # Test JSON serialization
    user = User(**test_data['user_data'])
    await user.save()
    
    json_data = user.json()
    assert 'id' in json_data
    assert json_data['name'] == 'John Doe'
    assert json_data['email'] == 'john@example.com'
    
    # Test model validation
    # user.age = -1  # Invalid age
    # with pytest.raises(ValueError):
    #     user.validate_fields()
    
    await User.delete_many({})
    await Address.delete_many({})

@pytest.mark.asyncio
async def test_timestamps(test_data):
    """Test automatic timestamps."""
    
    await User.delete_many({})
    user = User(**test_data['user_data'])
    # assert user.created_at is None
    # assert user.updated_at is None
    assert isinstance(user.created_at, datetime)
    assert isinstance(user.updated_at, datetime)
    
    # Save user and verify timestamps are set
    user = User(**test_data['user_data'])
    await user.save()
    
    # Store original timestamps
    created_at = user.created_at
    updated_at = user.updated_at
    
    # Update user and verify only updated_at changes
    user.name = 'Jane Doe'
    await user.save()
    assert user.created_at == created_at
    assert user.updated_at != updated_at
    
    await User.delete_many({})

@pytest.mark.asyncio
async def test_relationships(test_data):
    """Test model relationships."""
    # Create address
    address = Address(**test_data['address_data'])
    await address.save()
    
    # Create user with address
    user = User(**test_data['user_data'])
    user.address = address
    await user.save()
    
    # Create posts
    post1 = Post(**test_data['post_data'])
    await post1.save()
    post2 = Post(title='Another Post', content='More Content', published=False)
    await post2.save()
    
    # Add posts to user
    user.posts = [post1, post2]
    await user.save()
    
    # Retrieve user and verify relationships
    retrieved_user = await User.get(user.id)
    assert retrieved_user is not None
    
    # Verify address relationship
    assert await retrieved_user.address is not None
    assert (await retrieved_user.address).street == '123 Main St'
    
    # Verify posts relationship
    assert len(await retrieved_user.posts) == 2
    assert (await retrieved_user.posts)[0].title == 'Test Post'
    assert (await retrieved_user.posts)[1].title == 'Another Post'
    
    await User.delete_many({})
    await Address.delete_many({})
    await Post.delete_many({})

@pytest.mark.asyncio
async def test_find_and_all(test_data):
    """Test find and all methods."""
    # Create multiple users
    users = [
        User(name='John Doe', email='john@example.com', age=30),
        User(name='Jane Doe', email='jane@example.com', age=25),
        User(name='Bob Smith', email='bob@example.com', age=35)
    ]
    for user in users:
        await user.save()
    
    # Test find with conditions
    found_users = await User.find({'age': 30})
    assert len(found_users) == 1
    assert found_users[0].name == 'John Doe'
    
    # Test find with multiple results
    found_users = await User.find({'age': {'$gte': 30}})
    assert len(found_users) == 2
    assert {user.name for user in found_users} == {'John Doe', 'Bob Smith'}
    
    # Test all method
    all_users = await User.all()
    assert len(all_users) == 3
    assert {user.name for user in all_users} == {'John Doe', 'Jane Doe', 'Bob Smith'}

    await User.delete_many({})

@pytest.mark.asyncio
async def test_update_and_remove_class_methods(test_data):
    """Test update_async and remove_async class methods."""
    # Create multiple users
    users = [
        User(name='John Doe', email='john@example.com', age=30),
        User(name='Jane Doe', email='jane@example.com', age=25),
        User(name='Bob Smith', email='bob@example.com', age=35)
    ]
    for user in users:
        await user.save()
    
    # Test update_async method
    updated = await User.update_many({'age': {'$gte': 30}}, {'name': 'Senior User'})
    assert updated == 2  # Should update 2 records
    
    # Verify updates
    senior_users = await User.find({'name': 'Senior User'})
    assert len(senior_users) == 2
    assert all(user.age is not None and user.age >= 30 for user in senior_users)
    
    # Test remove_async method
    removed = await User.delete_many({'age': 25})
    assert removed == 1  # Should remove 1 record
    
    # Verify removal
    remaining_users = await User.all()
    assert len(remaining_users) == 2
    assert all(user.age is not None and user.age >= 30 for user in remaining_users) 
    
    await User.delete_many({})
