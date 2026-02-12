import threading
import time
import hashlib
import json
from typing import Dict, Any, Optional, Tuple, Callable, List
from functools import wraps
from collections import OrderedDict


class QueryCache:
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, 'initialized'):
            self._cache: OrderedDict[str, Tuple[Any, float, float]] = OrderedDict()
            self._cache_lock = threading.RLock()
            self._default_ttl = 300.0
            self._max_size = 1000
            self.initialized = True
    
    def _generate_key(self, query: str, params: tuple = ()) -> str:
        key_data = f"{query}:{json.dumps(params, sort_keys=True, default=str)}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def get(self, query: str, params: tuple = ()) -> Optional[Any]:
        with self._cache_lock:
            key = self._generate_key(query, params)
            if key in self._cache:
                result, timestamp, ttl = self._cache[key]
                if time.time() - timestamp < ttl:
                    self._cache.move_to_end(key)
                    return result
                del self._cache[key]
        return None
    
    def set(self, query: str, params: tuple, result: Any, ttl: Optional[float] = None) -> None:
        with self._cache_lock:
            key = self._generate_key(query, params)
            while len(self._cache) >= self._max_size:
                self._cache.popitem(last=False)
            self._cache[key] = (result, time.time(), ttl or self._default_ttl)
    
    def invalidate(self, table: str) -> None:
        with self._cache_lock:
            keys_to_remove = [
                k for k in self._cache.keys() 
                if table.lower() in k.lower()
            ]
            for key in keys_to_remove:
                del self._cache[key]
    
    def clear(self) -> None:
        with self._cache_lock:
            self._cache.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        with self._cache_lock:
            return {
                'size': len(self._cache),
                'max_size': self._max_size,
                'default_ttl': self._default_ttl
            }


def cached_query(ttl: Optional[float] = None, skip_tables: Optional[List[str]] = None):
    skip_tables = skip_tables or []
    
    def decorator(func: Callable) -> Callable:
        cache = QueryCache()
        
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            table = args[1] if len(args) > 1 else kwargs.get('table', '')
            if table.lower() in [t.lower() for t in skip_tables]:
                return await func(*args, **kwargs)
            
            query_part = kwargs.get('query', '')
            params_part = args[2] if len(args) > 2 else kwargs.get('params', ())
            
            if query_part:
                cached = cache.get(query_part, params_part if isinstance(params_part, tuple) else ())
                if cached is not None:
                    return cached
            
            result = await func(*args, **kwargs)
            
            if query_part:
                cache.set(query_part, params_part if isinstance(params_part, tuple) else (), result, ttl)
            
            return result
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            table = args[1] if len(args) > 1 else kwargs.get('table', '')
            if table.lower() in [t.lower() for t in skip_tables]:
                return func(*args, **kwargs)
            
            query_part = kwargs.get('query', '')
            params_part = args[2] if len(args) > 2 else kwargs.get('params', ())
            
            if query_part:
                cached = cache.get(query_part, params_part if isinstance(params_part, tuple) else ())
                if cached is not None:
                    return cached
            
            result = func(*args, **kwargs)
            
            if query_part:
                cache.set(query_part, params_part if isinstance(params_part, tuple) else (), result, ttl)
            
            return result
        
        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper
    
    return decorator
