import pytest
from datetime import datetime
from typing import Optional, List, Union

from odbms.dbms import DBMS
from odbms.model import Model
from pydantic import Field

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
    def address(self) -> Optional[Address]:
        if self._address is None and self.address_id is not None:
            self._address = Address.get(self.address_id)
        return self._address
    
    @address.setter
    def address(self, value: Optional[Address]) -> None:
        self._address = value
        if value is not None:
            setattr(self, 'address_id', value.id)
        else:
            setattr(self, 'address_id', None)
    
    @property
    def posts(self) -> List[Post]:
        if not self._posts and self.posts_ids:
            posts = [Post.get(post_id) for post_id in self.posts_ids]
            self._posts = [post for post in posts if post is not None]
        return self._posts
    
    @posts.setter
    def posts(self, value: List[Post]) -> None:
        self._posts = value
        setattr(self, 'posts_ids', [post.id for post in value if post is not None])

@pytest.fixture(scope="module")
def database():
    """Initialize database for testing."""
    DBMS.initialize(
        dbms='sqlite',
        database=':memory:'
    )
    
    # Create tables
    if DBMS.Database is not None and DBMS.Database.dbms != 'mongodb':
        # Create addresses table first
        DBMS.Database.execute("""
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
        DBMS.Database.execute("""
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
        DBMS.Database.execute("""
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
            DBMS.Database.execute("DROP TABLE IF EXISTS posts")
            DBMS.Database.execute("DROP TABLE IF EXISTS users")
            DBMS.Database.execute("DROP TABLE IF EXISTS addresses")
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
def cleanup(database):
    """Clean up after each test."""
    yield
    if DBMS.Database is not None:
        if DBMS.Database.dbms != 'mongodb':
            DBMS.Database.execute("DELETE FROM addresses")
            DBMS.Database.execute("DELETE FROM posts")
            DBMS.Database.execute("DELETE FROM users")
        else:
            DBMS.Database.remove('addresses', {})
            DBMS.Database.remove('posts', {})
            DBMS.Database.remove('users', {})

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

def test_relationships(test_data):
    """Test model relationships."""
    # Create address
    address = Address(**test_data['address_data'])
    address.save()
    
    # Create user with address
    user = User(**test_data['user_data'])
    user.address = address
    user.save()
    
    # Create posts
    post1 = Post(**test_data['post_data'])
    post1.save()
    post2 = Post(title='Another Post', content='More Content', published=False)
    post2.save()
    
    # Add posts to user
    user.posts = [post1, post2]
    user.save()
    
    # Retrieve user and verify relationships
    retrieved_user = User.get(user.id)
    assert retrieved_user is not None
    
    # Verify address relationship
    assert retrieved_user.address is not None
    assert retrieved_user.address.street == '123 Main St'
    
    # Verify posts relationship
    assert len(retrieved_user.posts) == 2
    assert retrieved_user.posts[0].title == 'Test Post'
    assert retrieved_user.posts[1].title == 'Another Post'

def test_timestamps(test_data):
    """Test automatic timestamps."""
    user = User(**test_data['user_data'])
    assert user.created_at is None
    assert user.updated_at is None
    
    # Save user and verify timestamps are set
    user.save()
    assert user.created_at is not None
    assert user.updated_at is not None
    assert isinstance(user.created_at, datetime)
    assert isinstance(user.updated_at, datetime)
    
    # Store original timestamps
    created_at = user.created_at
    updated_at = user.updated_at
    
    # Update user and verify only updated_at changes
    user.name = 'Jane Doe'
    user.save()
    assert user.created_at == created_at
    assert user.updated_at != updated_at

def test_model_methods(test_data):
    """Test model utility methods."""
    # Test table name generation
    assert User.table_name() == 'users'
    assert Address.table_name() == 'addresses'
    
    # Test JSON serialization
    user = User(**test_data['user_data'])
    user.save()
    
    json_data = user.json()
    assert 'id' in json_data
    assert json_data['name'] == 'John Doe'
    assert json_data['email'] == 'john@example.com'
    
    # Test model validation
    user.age = -1  # Invalid age
    with pytest.raises(ValueError):
        user.validate_fields()

@pytest.mark.asyncio
async def test_async_relationships(test_data):
    """Test async relationship operations."""
    # Create address
    address = Address(**test_data['address_data'])
    await address.save_async()
    
    # Create user with address
    user = User(**test_data['user_data'])
    user.address = address
    await user.save_async()
    
    # Create posts
    post1 = Post(**test_data['post_data'])
    await post1.save_async()
    post2 = Post(title='Another Post', content='More Content', published=False)
    await post2.save_async()
    
    # Add posts to user
    user.posts = [post1, post2]
    await user.save_async()
    
    # Retrieve user and verify relationships
    retrieved_user = await User.get_async(user.id)
    assert retrieved_user is not None
    
    # Verify address relationship
    assert retrieved_user.address is not None
    assert retrieved_user.address.street == '123 Main St'
    
    # Verify posts relationship
    assert len(retrieved_user.posts) == 2
    assert retrieved_user.posts[0].title == 'Test Post'
    assert retrieved_user.posts[1].title == 'Another Post'
    
    # Test cascade delete
    await user.delete_async(cascade=True)
    
    # Verify everything was deleted
    assert await User.get_async(user.id) is None
    assert await Address.get_async(address.id) is None
    assert await Post.get_async(post1.id) is None
    assert await Post.get_async(post2.id) is None 

def test_find_and_all(test_data):
    """Test find and all methods."""
    # Create multiple users
    users = [
        User(name='John Doe', email='john@example.com', age=30),
        User(name='Jane Doe', email='jane@example.com', age=25),
        User(name='Bob Smith', email='bob@example.com', age=35)
    ]
    for user in users:
        user.save()
    
    # Test find with conditions
    found_users = User.find({'age': 30})
    assert len(found_users) == 1
    assert found_users[0].name == 'John Doe'
    
    # Test find with multiple results
    found_users = User.find({'age': {'$gte': 30}})
    assert len(found_users) == 2
    assert {user.name for user in found_users} == {'John Doe', 'Bob Smith'}
    
    # Test all method
    all_users = User.all()
    assert len(all_users) == 3
    assert {user.name for user in all_users} == {'John Doe', 'Jane Doe', 'Bob Smith'}

@pytest.mark.asyncio
async def test_find_async_and_all_async(test_data):
    """Test find_async and all_async methods."""
    # Create multiple users
    users = [
        User(name='John Doe', email='john@example.com', age=30),
        User(name='Jane Doe', email='jane@example.com', age=25),
        User(name='Bob Smith', email='bob@example.com', age=35)
    ]
    for user in users:
        await user.save_async()
    
    # Test find_async with conditions
    found_users = await User.find_async({'age': 30})
    assert len(found_users) == 1
    assert found_users[0].name == 'John Doe'
    
    # Test find_async with multiple results
    found_users = await User.find_async({'age': {'$gte': 30}})
    assert len(found_users) == 2
    assert {user.name for user in found_users} == {'John Doe', 'Bob Smith'}
    
    # Test all_async method
    all_users = await User.all_async()
    assert len(all_users) == 3
    assert {user.name for user in all_users} == {'John Doe', 'Jane Doe', 'Bob Smith'} 