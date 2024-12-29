import pytest
from odbms.dbms import DBMS

@pytest.fixture(scope="session")
def database():
    """Initialize database for testing."""
    DBMS.initialize(
        dbms='sqlite',
        database=':memory:'
    )
    
    # Create tables
    if DBMS.Database is not None and DBMS.Database.dbms != 'mongodb':
        DBMS.Database.execute("""
            CREATE TABLE IF NOT EXISTS addresses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                street TEXT NOT NULL,
                city TEXT NOT NULL,
                country TEXT NOT NULL,
                postal_code TEXT
            )
        """)
        
        DBMS.Database.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                published BOOLEAN DEFAULT 0,
                user_id INTEGER,
                FOREIGN KEY (user_id) REFERENCES users (id)
            )
        """)
        
        DBMS.Database.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                age INTEGER,
                address_id INTEGER,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                FOREIGN KEY (address_id) REFERENCES addresses (id)
            )
        """)
    
    yield DBMS.Database
    
    # Cleanup
    if DBMS.Database is not None:
        if DBMS.Database.dbms != 'mongodb':
            DBMS.Database.execute("DROP TABLE IF EXISTS users")
            DBMS.Database.execute("DROP TABLE IF EXISTS posts")
            DBMS.Database.execute("DROP TABLE IF EXISTS addresses")
        else:
            DBMS.Database.remove('users', {})
            DBMS.Database.remove('posts', {})
            DBMS.Database.remove('addresses', {})
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
        
    if database is not None:
        if database.dbms != 'mongodb':
            database.execute("DELETE FROM addresses")
            database.execute("DELETE FROM posts")
            database.execute("DELETE FROM users")
        else:
            database.remove('addresses', {})
            database.remove('posts', {})
            database.remove('users', {})
        database.disconnect()