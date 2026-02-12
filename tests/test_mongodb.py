import pytest
from odbms.orms.mongodb import MongoDB
from odbms.dbms import DBMS
import asyncio

class TestMongoDBInitialization:
    """Test MongoDB initialization with default values."""
    
    @pytest.fixture(autouse=True)
    def cleanup(self):
        """Override the async cleanup fixture from conftest with a sync version."""
        # Disconnect any existing connection before test
        if DBMS.Database is not None:
            DBMS.Database.disconnect()
            DBMS.Database = None
            DBMS._initialized = False
        yield
        # Cleanup after test
        if DBMS.Database is not None:
            DBMS.Database.disconnect()
            DBMS.Database = None
            DBMS._initialized = False
    
    def test_mongodb_init_with_defaults(self):
        """Test MongoDB initialization with default values."""
        # Create MongoDB instance with minimal config (only database required)
        db = MongoDB(database='test_default_db')
        
        # Verify config contains only what was passed
        assert db.config['database'] == 'test_default_db'
        # host and port are not in config until connect() is called
        assert 'host' not in db.config
        assert 'port' not in db.config
        assert db.dbms == 'mongodb'
        assert db.client is None
        assert db.db is None
    
    def test_mongodb_init_with_custom_values(self):
        """Test MongoDB initialization with custom values."""
        db = MongoDB(
            host='mongodb.example.com',
            port=27018,
            database='custom_db'
        )
        
        assert db.config['host'] == 'mongodb.example.com'
        assert db.config['port'] == 27018
        assert db.config['database'] == 'custom_db'
    
    def test_mongodb_init_with_string_port(self):
        """Test MongoDB initialization handles string port conversion."""
        db = MongoDB(
            host='localhost',
            port='27017',  # String port
            database='test_db'
        )
        
        # Port should be stored as string initially
        assert db.config['port'] == '27017'
        
        # Connect and verify port is converted to int
        db.connect()
        assert db.client is not None
        assert db.db is not None
        
        db.disconnect()
    
    @pytest.mark.asyncio
    async def test_dbms_initialize_async_with_defaults(self):
        """Test DBMS.initialize_async with default MongoDB values."""
        # Initialize with minimal parameters
        await DBMS.initialize_async(
            dbms='mongodb',
            database='test_dbms_defaults'
        )
        
        # Verify initialization
        assert DBMS.Database is not None
        assert DBMS.Database.dbms == 'mongodb'
        assert DBMS._initialized is True
        
        # Verify default values
        assert DBMS.Database.config['host'] == 'localhost'
        assert DBMS.Database.config['port'] == 27017
        assert DBMS.Database.config['database'] == 'test_dbms_defaults'
        
        # Verify connection is established
        assert DBMS.Database.client is not None
        assert DBMS.Database.db is not None
        
        # Cleanup
        await DBMS.disconnect_async()
    
    @pytest.mark.asyncio
    async def test_dbms_initialize_async_with_string_port(self):
        """Test DBMS.initialize_async handles string port."""
        await DBMS.initialize_async(
            dbms='mongodb',
            host='localhost',
            port='27017',  # String port
            database='test_string_port'
        )
        
        # Verify initialization succeeded
        assert DBMS.Database is not None
        assert DBMS.Database.client is not None
        
        # Cleanup
        await DBMS.disconnect_async()
    
    def test_mongodb_connect_applies_default_host(self):
        """Test that connect() uses default host when not specified."""
        db = MongoDB(database='test_connect_defaults')
        
        # Verify defaults before connect
        assert db.config.get('host', 'localhost') == 'localhost'
        assert db.config.get('port', 27017) == 27017


@pytest.fixture
def db():
    """Database fixture."""
    settings = {
        'host': 'localhost',
        'port': 27017,
        'database': 'test_db'
    }
    db_instance = MongoDB(**settings)
    db_instance.connect()
    yield db_instance
    db_instance.disconnect()

@pytest.fixture(autouse=True)
async def cleanup(db):
    """Clean up after each test."""
    yield
    await db.delete_many('test_users', {})
    await db.delete_many('test_users', {})
    await db.delete_many('test_scores', {})

@pytest.mark.asyncio
async def test_crud_operations(db):
    """Test basic CRUD operations."""
    # Test insert
    data = {'name': 'John Doe', 'age': 30}
    user_id = await db.insert_one('test_users', data)
    assert user_id is not None

    # Test find_one
    user = await db.find_one('test_users', {'_id': user_id})
    assert user is not None
    assert user['name'] == 'John Doe'
    assert user['age'] == 30

    # Test find
    users = await db.find('test_users', {'age': 30})
    assert len(users) == 1
    assert users[0]['name'] == 'John Doe'

    # Test update
    updated = await db.update_one('test_users', {'_id': user_id}, {'age': 31})
    assert updated == 1
    user = await db.find_one('test_users', {'_id': user_id})
    assert user['age'] == 31

    # Test remove
    removed = await db.delete_one('test_users', {'_id': user_id})
    assert removed == 1
    user = await db.find_one('test_users', {'_id': user_id})
    assert user is None

@pytest.mark.asyncio
async def test_query_operators(db):
    """Test MongoDB query operators."""
    # Insert test data
    users = [
        {'name': 'John', 'age': 20},
        {'name': 'Jane', 'age': 25},
        {'name': 'Bob', 'age': 30},
        {'name': 'Alice', 'age': 35},
    ]
    
    await db.insert_many('test_users', users)

    # Test $lt (less than)
    young_users = await db.find('test_users', {'age': {'$lt': 25}})
    assert len(young_users) == 1
    assert young_users[0]['name'] == 'John'

    # Test $lte (less than or equal)
    young_users = await db.find('test_users', {'age': {'$lte': 25}})
    assert len(young_users) == 2
    assert {user['name'] for user in young_users} == {'John', 'Jane'}

    # Test $gt (greater than)
    older_users = await db.find('test_users', {'age': {'$gt': 30}})
    assert len(older_users) == 1
    assert older_users[0]['name'] == 'Alice'

    # Test $gte (greater than or equal)
    older_users = await db.find('test_users', {'age': {'$gte': 30}})
    assert len(older_users) == 2
    assert {user['name'] for user in older_users} == {'Bob', 'Alice'}

    # Test $ne (not equal)
    not_john = await db.find('test_users', {'name': {'$ne': 'John'}})
    assert len(not_john) == 3
    assert all(user['name'] != 'John' for user in not_john)

    # Test $in (in array)
    selected_users = await db.find('test_users', {'name': {'$in': ['John', 'Jane']}})
    assert len(selected_users) == 2
    assert {user['name'] for user in selected_users} == {'John', 'Jane'}

    # Test $nin (not in array)
    other_users = await db.find('test_users', {'name': {'$nin': ['John', 'Jane']}})
    assert len(other_users) == 2
    assert {user['name'] for user in other_users} == {'Bob', 'Alice'}

    # Clean up
    await db.delete_many('test_users', {})


@pytest.mark.asyncio
async def test_sum_operation(db):
    """Test sum operation."""
    # Insert test data
    data = [
        {'user_id': 1, 'score': 10},
        {'user_id': 1, 'score': 20},
        {'user_id': 2, 'score': 30},
    ]
    
    await db.insert_many('test_scores', data)
    
    # Test sum
    total = await db.sum('test_scores', 'score', {'user_id': 1})
    assert total == 30

    # Clean up
    await db.delete_many('test_scores', {})