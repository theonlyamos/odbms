import asyncio
from pathlib import Path
from .database import Database
from .orms.mongodb import MongoDB
from .orms.mysqldb import MysqlDB
from .orms.sqlitedb import SQLiteDB
from typing import Optional, Type, Union
from .orms.postgresqldb import PostgresqlDB

class DBMS:
    Database: Optional[Union[MongoDB, SQLiteDB, PostgresqlDB, MysqlDB]] = None
    _initialized: bool = False
    
    @classmethod
    async def initialize_async(cls, dbms: str, host: str = 'localhost', port: Optional[int] = None, 
              username: Optional[str] = None, password: Optional[str] = None, 
              database: str = 'runit') -> None:
        if dbms == 'mongodb':
            cls.Database = MongoDB(
                host=host,
                port=port or 27017,
                database=database
            )
            cls.Database.connect()
            cls.Database.dbms = 'mongodb'
        elif dbms == 'sqlite':
            database_path = Path(database)
            database_path.parent.mkdir(parents=True, exist_ok=True)
            cls.Database = SQLiteDB(database=database_path)
            cls.Database.connect()
            cls.Database.dbms = 'sqlite'
        elif dbms == 'postgresql':
            cls.Database = PostgresqlDB()
            dbsettings = {
                'host': host,
                'port': port or 5432,
                'database': database,
                'user': username,
                'password': password
            }
            await cls.Database.connect(dbsettings=dbsettings)
            cls.Database.dbms = 'postgresql'
        elif dbms == 'mysql':
            cls.Database = MysqlDB()
            dbsettings = {
                'host': host,
                'port': port or 3306,
                'database': database,
                'user': username,
                'password': password
            }
            cls.Database.connect(dbsettings=dbsettings)
            cls.Database.dbms = 'mysql'
        else:
            raise ValueError(f"Unsupported database type: {dbms}")
        cls._initialized = True

    @classmethod
    def initialize(cls, dbms: str, host: str = 'localhost', port: Optional[int] = None, 
                   username: Optional[str] = None, password: Optional[str] = None, 
                   database: str = 'runit') -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        
        if loop and loop.is_running():
            import warnings
            warnings.warn(
                "initialize() called from async context. Use initialize_async() instead.",
                RuntimeWarning
            )
            asyncio.create_task(cls.initialize_async(dbms, host, port, username, password, database))
            return
        
        asyncio.run(cls.initialize_async(dbms, host, port, username, password, database))
    
    @classmethod
    async def disconnect_async(cls) -> None:
        if cls.Database:
            if hasattr(cls.Database, 'disconnect'):
                if asyncio.iscoroutinefunction(cls.Database.disconnect):
                    await cls.Database.disconnect()
                else:
                    cls.Database.disconnect()
            cls.Database = None
            cls._initialized = False
    
    @classmethod
    def disconnect(cls) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        
        if loop and loop.is_running():
            asyncio.create_task(cls.disconnect_async())
            return
        
        asyncio.run(cls.disconnect_async())
    
    @classmethod
    def is_connected(cls) -> bool:
        return cls._initialized and cls.Database is not None
