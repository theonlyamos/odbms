#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2022-08-06 17:37:00
# @Author  : Amos Amissah (theonlyamos@gmail.com)
# @Link    : link
# @Version : 1.0.0

from typing import Type, Literal
from .mongodb import MongoDB
from .mysqldb import MysqlDB
from .sqlitedb import SqliteDB

Database: Type[MongoDB]|Type[MysqlDB]|None = None

class DBMS(object):
    @staticmethod
    def initialize(dbms: Literal['mysql', 'mongodb'], host: str = '127.0.0.1', port: int = 0, username: str = '', password: str = '', database: str = '')-> Type[Database]:
        '''
        Static method for select and connecting \n 
        to specified database system.
        
        @param dbsm Database system to use. Currently supports mysql and mongodb
        @param host Host address for database connection. 
        @param port Port for database connection.
        @param username Username for database connection
        @param password Password for database connection
        @param database Databse name
        @return None
        '''
        
        global Database
        
        if dbms == 'mongodb':
            MongoDB.initialize(host, port, database)
            Database = MongoDB
            
        elif dbms == 'mysql':
            MysqlDB.initialize(host, port, username, password, database)
            Database = MysqlDB
        
        elif dbms == 'sqlite':
            SqliteDB.initialize(database)
            Database = SqliteDB
        
        return Database
            
    @staticmethod
    def initialize_with_defaults(dbms, database):
        settings = {}
        settings['dbms'] = dbms
        settings['host'] = '127.0.0.1'
        settings['port'] = 3306 if dbms == 'mysql' else 27017
        settings['username'] = 'root' if dbms == 'mysql' else ''
        settings['password'] = ''
        if database:
            settings['database'] = database
        
        return DBMS.initialize(**settings)
    