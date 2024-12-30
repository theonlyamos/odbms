import pytest
from odbms.orms.mongodb import MongoDB
import asyncio

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
    # Create new event loop if needed
    if db._loop is None or db._loop.is_closed():
        db._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(db._loop)
    await db.remove_async('test_users', {})
    await db.remove_async('test_scores', {})

def test_crud_operations(db):
    """Test basic CRUD operations."""
    # Test insert
    data = {'name': 'John Doe', 'age': 30}
    user_id = db.insert('test_users', data)
    assert user_id is not None

    # Test find_one
    user = db.find_one('test_users', {'_id': user_id})
    assert user is not None
    assert user['name'] == 'John Doe'
    assert user['age'] == 30

    # Test find
    users = db.find('test_users', {'age': 30})
    assert len(users) == 1
    assert users[0]['name'] == 'John Doe'

    # Test update
    updated = db.update('test_users', {'_id': user_id}, {'age': 31})
    assert updated == 1
    user = db.find_one('test_users', {'_id': user_id})
    assert user['age'] == 31

    # Test remove
    removed = db.remove('test_users', {'_id': user_id})
    assert removed == 1
    user = db.find_one('test_users', {'_id': user_id})
    assert user is None

def test_query_operators(db):
    """Test MongoDB query operators."""
    # Insert test data
    users = [
        {'name': 'John', 'age': 20},
        {'name': 'Jane', 'age': 25},
        {'name': 'Bob', 'age': 30},
        {'name': 'Alice', 'age': 35},
    ]
    for user in users:
        db.insert('test_users', user)

    # Test $lt (less than)
    young_users = db.find('test_users', {'age': {'$lt': 25}})
    assert len(young_users) == 1
    assert young_users[0]['name'] == 'John'

    # Test $lte (less than or equal)
    young_users = db.find('test_users', {'age': {'$lte': 25}})
    assert len(young_users) == 2
    assert {user['name'] for user in young_users} == {'John', 'Jane'}

    # Test $gt (greater than)
    older_users = db.find('test_users', {'age': {'$gt': 30}})
    assert len(older_users) == 1
    assert older_users[0]['name'] == 'Alice'

    # Test $gte (greater than or equal)
    older_users = db.find('test_users', {'age': {'$gte': 30}})
    assert len(older_users) == 2
    assert {user['name'] for user in older_users} == {'Bob', 'Alice'}

    # Test $ne (not equal)
    not_john = db.find('test_users', {'name': {'$ne': 'John'}})
    assert len(not_john) == 3
    assert all(user['name'] != 'John' for user in not_john)

    # Test $in (in array)
    selected_users = db.find('test_users', {'name': {'$in': ['John', 'Jane']}})
    assert len(selected_users) == 2
    assert {user['name'] for user in selected_users} == {'John', 'Jane'}

    # Test $nin (not in array)
    other_users = db.find('test_users', {'name': {'$nin': ['John', 'Jane']}})
    assert len(other_users) == 2
    assert {user['name'] for user in other_users} == {'Bob', 'Alice'}

    # Clean up
    db.remove('test_users', {})

@pytest.mark.asyncio
async def test_async_query_operators(db):
    """Test MongoDB query operators with async operations."""
    # Insert test data
    users = [
        {'name': 'John', 'age': 20},
        {'name': 'Jane', 'age': 25},
        {'name': 'Bob', 'age': 30},
        {'name': 'Alice', 'age': 35},
    ]
    for user in users:
        await db.insert_async('test_users', user)

    # Test $lt (less than)
    young_users = await db.find_async('test_users', {'age': {'$lt': 25}})
    assert len(young_users) == 1
    assert young_users[0]['name'] == 'John'

    # Test $lte (less than or equal)
    young_users = await db.find_async('test_users', {'age': {'$lte': 25}})
    assert len(young_users) == 2
    assert {user['name'] for user in young_users} == {'John', 'Jane'}

    # Test $gt (greater than)
    older_users = await db.find_async('test_users', {'age': {'$gt': 30}})
    assert len(older_users) == 1
    assert older_users[0]['name'] == 'Alice'

    # Test $gte (greater than or equal)
    older_users = await db.find_async('test_users', {'age': {'$gte': 30}})
    assert len(older_users) == 2
    assert {user['name'] for user in older_users} == {'Bob', 'Alice'}

    # Test $ne (not equal)
    not_john = await db.find_async('test_users', {'name': {'$ne': 'John'}})
    assert len(not_john) == 3
    assert all(user['name'] != 'John' for user in not_john)

    # Test $in (in array)
    selected_users = await db.find_async('test_users', {'name': {'$in': ['John', 'Jane']}})
    assert len(selected_users) == 2
    assert {user['name'] for user in selected_users} == {'John', 'Jane'}

    # Test $nin (not in array)
    other_users = await db.find_async('test_users', {'name': {'$nin': ['John', 'Jane']}})
    assert len(other_users) == 2
    assert {user['name'] for user in other_users} == {'Bob', 'Alice'}

    # Clean up
    await db.remove_async('test_users', {})

@pytest.mark.asyncio
async def test_async_crud_operations(db):
    """Test async CRUD operations."""
    # Test insert_async
    data = {'name': 'Jane Doe', 'age': 25}
    user_id = await db.insert_async('test_users', data)
    assert user_id is not None

    # Test find_one_async
    user = await db.find_one_async('test_users', {'_id': user_id})
    assert user is not None
    assert user['name'] == 'Jane Doe'
    assert user['age'] == 25

    # Test find_async
    users = await db.find_async('test_users', {'age': 25})
    assert len(users) == 1
    assert users[0]['name'] == 'Jane Doe'

    # Test update_async
    updated = await db.update_async('test_users', {'_id': user_id}, {'age': 26})
    assert updated == 1
    user = await db.find_one_async('test_users', {'_id': user_id})
    assert user['age'] == 26

    # Test remove_async
    removed = await db.remove_async('test_users', {'_id': user_id})
    assert removed == 1
    user = await db.find_one_async('test_users', {'_id': user_id})
    assert user is None

def test_sum_operation(db):
    """Test sum operation."""
    # Insert test data
    db.insert('test_scores', {'user_id': 1, 'score': 10})
    db.insert('test_scores', {'user_id': 1, 'score': 20})
    db.insert('test_scores', {'user_id': 2, 'score': 30})
    
    # Test sum
    total = db.sum('test_scores', 'score', {'user_id': 1})
    assert total == 30

    # Clean up
    db.remove('test_scores', {})

@pytest.mark.asyncio
async def test_sum_async_operation(db):
    """Test async sum operation."""
    # Insert test data
    await db.insert_async('test_scores', {'user_id': 1, 'score': 10})
    await db.insert_async('test_scores', {'user_id': 1, 'score': 20})
    await db.insert_async('test_scores', {'user_id': 2, 'score': 30})
    
    # Test sum_async
    total = await db.sum_async('test_scores', 'score', {'user_id': 1})
    assert total == 30

    # Clean up
    await db.remove_async('test_scores', {}) 