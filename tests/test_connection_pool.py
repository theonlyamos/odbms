import pytest
import threading
from typing import List

from odbms.dbms import DBMS
from odbms.connection_pool import ConnectionPool

@pytest.fixture
def pool():
    """Connection pool fixture."""
    pool = ConnectionPool()
    yield pool
    pool.close_all()

@pytest.fixture
def config():
    """Test config fixture."""
    return {
        'database': ':memory:'  # Use in-memory SQLite for testing
    }

def test_pool_initialization(pool, config):
    """Test pool initialization."""
    pool.initialize_pool('sqlite', config, pool_size=5)
    assert len(pool._available_connections) == 5
    assert len(pool._in_use_connections) == 0

def test_get_connection(pool, config):
    """Test getting a connection from the pool."""
    pool.initialize_pool('sqlite', config, pool_size=3)
    
    # Get a connection
    with pool.get_connection('sqlite', config) as conn:
        assert conn is not None
        assert len(pool._available_connections) == 2
        assert len(pool._in_use_connections) == 1
    
    # Connection should be returned to pool
    assert len(pool._available_connections) == 3
    assert len(pool._in_use_connections) == 0

def test_pool_exhaustion(pool, config):
    """Test behavior when pool is exhausted."""
    pool.initialize_pool('sqlite', config, pool_size=2)
    
    # Get all connections
    conn1 = pool.get_connection('sqlite', config)
    conn2 = pool.get_connection('sqlite', config)
    
    # Try to get another connection (should wait)
    def get_connection():
        with pool.get_connection('sqlite', config) as conn:
            assert conn is not None
    
    thread = threading.Thread(target=get_connection)
    thread.start()
    
    # Return a connection to the pool
    conn1.close()
    
    # Thread should complete
    thread.join(timeout=1)
    assert not thread.is_alive()
    
    # Clean up
    conn2.close()

def test_connection_reuse(pool, config):
    """Test that connections are reused."""
    pool.initialize_pool('sqlite', config, pool_size=1)
    
    # Get and use a connection
    with pool.get_connection('sqlite', config) as conn1:
        conn1_id = id(conn1)
    
    # Get another connection (should be the same one)
    with pool.get_connection('sqlite', config) as conn2:
        conn2_id = id(conn2)
    
    assert conn1_id == conn2_id

def test_concurrent_access(pool, config):
    """Test concurrent access to the connection pool."""
    pool.initialize_pool('sqlite', config, pool_size=3)
    results: List[bool] = []
    
    def worker():
        try:
            with pool.get_connection('sqlite', config) as conn:
                # Simulate some work
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                results.append(True)
        except Exception:
            results.append(False)
    
    # Start multiple threads
    threads = [threading.Thread(target=worker) for _ in range(5)]
    for thread in threads:
        thread.start()
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    
    # All operations should succeed
    assert len(results) == 5
    assert all(results)

def test_connection_error_handling(pool):
    """Test handling of connection errors."""
    # Initialize pool with invalid config
    invalid_config = {
        'database': '/nonexistent/path/db.sqlite'
    }
    
    pool.initialize_pool('sqlite', invalid_config, pool_size=1)
    
    # Attempting to get a connection should raise an error
    with pytest.raises(Exception):
        with pool.get_connection('sqlite', invalid_config):
            pass

def test_close_all_connections(pool, config):
    """Test closing all connections."""
    pool.initialize_pool('sqlite', config, pool_size=3)
    
    # Get some connections
    conn1 = pool.get_connection('sqlite', config)
    conn2 = pool.get_connection('sqlite', config)
    
    # Close all connections
    pool.close_all()
    
    # Pool should be empty
    assert len(pool._available_connections) == 0
    assert len(pool._in_use_connections) == 0
    
    # Trying to use closed connections should raise an error
    with pytest.raises(Exception):
        conn1.cursor()
    with pytest.raises(Exception):
        conn2.cursor() 