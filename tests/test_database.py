from bson import ObjectId
import pytest

from odbms.dbms import DBMS
from odbms.model import Model

# Only mark async tests with asyncio
pytestmark = []

class User(Model):
    name: str
    email: str
    age: int

@pytest.fixture(scope="session")
def database():
    """Initialize database for testing."""
    DBMS.initialize(
        dbms='sqlite',
        database=':memory:'  # In-memory database
    )
    
    # Create users table
    if DBMS.Database is not None and DBMS.Database.dbms != 'mongodb':
        DBMS.Database.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                age INTEGER,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
        """)
        
    yield DBMS.Database
    
    # Cleanup
    if DBMS.Database is not None:
        if DBMS.Database.dbms != 'mongodb':
            DBMS.Database.execute("DROP TABLE IF EXISTS users")
        DBMS.Database.disconnect()

@pytest.fixture
def test_data():
    """Test data fixture."""
    return {
        'name': 'John Doe',
        'email': f'john{id(object())}@example.com',  # Generate unique email
        'age': 30
    }

@pytest.fixture(autouse=True)
def cleanup(database):
    """Clean up after each test."""
    yield
    
    if database is not None:
        try:
            if database.dbms == 'mongodb':
                database.remove('users', {})
            else:
                database.execute("DELETE FROM users")
        except Exception:
                pass  # Ignore cleanup errors

# Synchronous tests
def test_insert(database, test_data):
    """Test inserting a record."""
    result = database.insert('users', test_data)
    assert result is not None

    # Verify the record was inserted
    if database.dbms != 'mongodb':
        record = database.find_one('users', {'id': result})
    else:
        record = database.find_one('users', {'_id': ObjectId(result)})
    assert record is not None
    assert record['name'] == test_data['name']
    assert record['email'] == test_data['email']

def test_insert_many(database):
    """Test inserting multiple records."""
    test_data = [
        {'name': 'John Doe', 'email': f'john{id(object())}@example.com', 'age': 30},
        {'name': 'Jane Doe', 'email': f'jane{id(object())}@example.com', 'age': 25}
    ]
    
    result = database.insert_many('users', test_data)
    assert result == 2  # Should insert 2 records
    
    # Verify the records were inserted
    records = database.find('users')
    assert len(records) == 2

def test_find(database, test_data):
    """Test finding records."""
    # Insert test data
    database.insert('users', test_data)
    
    # Test finding all records
    records = database.find('users')
    assert len(records) == 1
    
    # Test finding with conditions
    records = database.find('users', {'name': test_data['name']})
    assert len(records) == 1
    assert records[0]['email'] == test_data['email']
    
    # Test finding with non-matching conditions
    records = database.find('users', {'name': 'Not Exists'})
    assert len(records) == 0

def test_find_one(database, test_data):
    """Test finding a single record."""
    # Insert test data
    database.insert('users', test_data)
    
    # Test finding existing record
    record = database.find_one('users', {'email': test_data['email']})
    assert record is not None
    assert record['name'] == test_data['name']
    
    # Test finding non-existent record
    record = database.find_one('users', {'email': 'notexists@example.com'})
    assert record is None

def test_update(database, test_data):
    """Test updating records."""
    # Insert test data
    user_id = database.insert('users', test_data)
    
    # Update record
    update_data = {'name': 'John Smith', 'age': 31}
    if database.dbms != 'mongodb':
        result = database.update('users', {'id': user_id}, update_data)
    else:
        result = database.update('users', {'_id': ObjectId(user_id)}, update_data)
    assert result == 1  # Should update 1 record
    
    # Verify the update
    if database.dbms != 'mongodb':
        record = database.find_one('users', {'id': user_id})
    else:
        record = database.find_one('users', {'_id': ObjectId(user_id)})
    assert record is not None
    assert record['name'] == 'John Smith'
    assert record['age'] == 31

def test_remove(database, test_data):
    """Test removing records."""
    # Insert test data
    database.insert('users', test_data)
    
    # Remove record
    result = database.remove('users', {'email': test_data['email']})
    assert result == 1  # Should remove 1 record
    
    # Verify the record was removed
    record = database.find_one('users', {'email': test_data['email']})
    assert record is None

# Asynchronous tests
@pytest.mark.asyncio
async def test_async_operations(database, test_data):
    """Test async database operations."""
    # Test async insert
    result = await database.insert_async('users', test_data)
    assert result is not None
    
    # Test async find_one
    if database.dbms != 'mongodb':
        record = await database.find_one_async('users', {'id': result})
    else:
        record = await database.find_one_async('users', {'_id': ObjectId(result)})
    assert record is not None
    assert record['name'] == test_data['name']
    
    # Test async update
    update_data = {'name': 'John Smith'}
    if database.dbms != 'mongodb':
        result = await database.update_async('users', {'id': result}, update_data)
    else:
        result = await database.update_async('users', {'_id': ObjectId(result)}, update_data)
    assert result == 1
    
    # Test async find
    records = await database.find_async('users')
    assert len(records) == 1
    assert records[0]['name'] == 'John Smith'
    
    # Test async remove
    result = await database.remove_async('users', {'name': 'John Smith'})
    assert result == 1

@pytest.mark.asyncio
async def test_async_model_operations(test_data):
    """Test async model-level operations."""
    # Create user with proper timestamp format
    user = User(**test_data)
    new_user = await user.save_async()
    assert new_user.id is not None
    
    # Get user
    retrieved_user = await User.get_async(user.id)
    assert retrieved_user is not None
    assert retrieved_user.name == test_data['name']
    
    # Update user
    new_name = 'John Smith'
    retrieved_user.name = new_name
    await retrieved_user.save_async()
    
    # Verify update
    updated_user = await User.get_async(user.id)
    assert updated_user is not None
    assert updated_user.name == new_name
    
    # Delete user
    await retrieved_user.delete_async()
    
    # Verify deletion
    deleted_user = await User.get_async(user.id)
    assert deleted_user is None 