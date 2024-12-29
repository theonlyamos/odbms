#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2022-08-06 17:37:00
# @Author  : Amos Amissah (theonlyamos@gmail.com)
# @Link    : link
# @Version : 1.0.0

from typing import Optional, Type, Union
from .database import Database
from .orms.mongodb import MongoDB
from .orms.sqlitedb import SQLiteDB
from .orms.postgresqldb import PostgresqlDB

class DBMS:
    """Database Management System class."""
    
    Database: Optional[Union[MongoDB, SQLiteDB, PostgresqlDB]] = None
    
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
            cls.Database = PostgresqlDB(
                host=host,
                port=port or 5432,
                database=database,
                username=username,
                password=password
            )
            cls.Database.connect()
            cls.Database.dbms = 'postgresql'
        else:
            raise ValueError(f"Unsupported database type: {dbms}")
