from typing import Optional, Type, Union
from .database import Database
from .orms.mongodb import MongoDB
from .orms.sqlitedb import SQLiteDB
from .orms.postgresqldb import PostgresqlDB
from .orms.mysqldb import MysqlDB

class DBMS:
    """Database Management System class."""
    
    Database: Optional[Union[MongoDB, SQLiteDB, PostgresqlDB, MysqlDB]] = None
    
    @classmethod
    def initialize(cls, dbms: str, database: str, host: str = 'localhost', port: Optional[int] = None, 
                  username: Optional[str] = None, password: Optional[str] = None) -> None:
        """Initialize the database connection."""
        if dbms == 'mongodb':
            cls.Database = MongoDB(
                host=host,
                port=port or 27017,
                database=database
            )
            cls.Database.connect()
            cls.Database.dbms = 'mongodb'
        elif dbms == 'sqlite':
            cls.Database = SQLiteDB(database=database)
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
            cls.Database.connect(dbsettings=dbsettings)
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
