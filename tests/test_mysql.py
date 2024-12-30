import pytest
from odbms.orms.mysqldb import MysqlDB

@pytest.fixture
def db():
    """Database fixture."""
    settings = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': 'root',
        'database': 'test_db'
    }
    MysqlDB.connect(settings)
    yield MysqlDB
    MysqlDB.disconnect()

def test_crud_operations(db):
    """Test basic CRUD operations."""
    # Create test table
    db._run_sync(db._pool.acquire().__aenter__().cursor().__aenter__().execute("""
        CREATE TABLE IF NOT EXISTS test_users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100),
            age INTEGER
        )
    """))
    
    # Test insert
    data = {'name': 'John Doe', 'age': 30}
    user_id = db.insert('test_users', data)
    assert user_id > 0

    # Test find_one
    user = db.find_one('test_users', {'id': user_id})
    assert user is not None
    assert user['name'] == 'John Doe'
    assert user['age'] == 30

    # Test find
    users = db.find('test_users', {'age': 30})
    assert len(users) == 1
    assert users[0]['name'] == 'John Doe'

    # Test update
    updated = db.update('test_users', {'id': user_id}, {'age': 31})
    assert updated == 1
    user = db.find_one('test_users', {'id': user_id})
    assert user['age'] == 31

    # Test remove
    removed = db.remove('test_users', {'id': user_id})
    assert removed == 1
    user = db.find_one('test_users', {'id': user_id})
    assert user is None

    # Clean up
    db._run_sync(db._pool.acquire().__aenter__().cursor().__aenter__().execute("DROP TABLE test_users"))

@pytest.mark.asyncio
async def test_async_crud_operations(db):
    """Test async CRUD operations."""
    # Create test table
    async with db._pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS test_users (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100),
                    age INTEGER
                )
            """)
    
    # Test insert_async
    data = {'name': 'Jane Doe', 'age': 25}
    user_id = await db.insert_async('test_users', data)
    assert user_id > 0

    # Test find_one_async
    user = await db.find_one_async('test_users', {'id': user_id})
    assert user is not None
    assert user['name'] == 'Jane Doe'
    assert user['age'] == 25

    # Test find_async
    users = await db.find_async('test_users', {'age': 25})
    assert len(users) == 1
    assert users[0]['name'] == 'Jane Doe'

    # Test update_async
    updated = await db.update_async('test_users', {'id': user_id}, {'age': 26})
    assert updated == 1
    user = await db.find_one_async('test_users', {'id': user_id})
    assert user['age'] == 26

    # Test remove_async
    removed = await db.remove_async('test_users', {'id': user_id})
    assert removed == 1
    user = await db.find_one_async('test_users', {'id': user_id})
    assert user is None

    # Clean up
    async with db._pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DROP TABLE test_users")

def test_sum_operation(db):
    """Test sum operation."""
    # Create test table
    db._run_sync(db._pool.acquire().__aenter__().cursor().__aenter__().execute("""
        CREATE TABLE IF NOT EXISTS test_scores (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INTEGER,
            score INTEGER
        )
    """))
    
    # Insert test data
    db._run_sync(db._pool.acquire().__aenter__().cursor().__aenter__().execute("""
        INSERT INTO test_scores (user_id, score)
        VALUES (1, 10), (1, 20), (2, 30)
    """))
    
    # Test sum
    total = db.sum('test_scores', 'score', {'user_id': 1})
    assert total == 30

    # Clean up
    db._run_sync(db._pool.acquire().__aenter__().cursor().__aenter__().execute("DROP TABLE test_scores"))

@pytest.mark.asyncio
async def test_sum_async_operation(db):
    """Test async sum operation."""
    # Create test table
    async with db._pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS test_scores (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    user_id INTEGER,
                    score INTEGER
                )
            """)
            
            # Insert test data
            await cur.execute("""
                INSERT INTO test_scores (user_id, score)
                VALUES (1, 10), (1, 20), (2, 30)
            """)
    
    # Test sum_async
    total = await db.sum_async('test_scores', 'score', {'user_id': 1})
    assert total == 30

    # Clean up
    async with db._pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DROP TABLE test_scores") 