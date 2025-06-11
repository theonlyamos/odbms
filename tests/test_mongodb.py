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