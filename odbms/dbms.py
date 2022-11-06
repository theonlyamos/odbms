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

from bson.objectid import ObjectId

def normalise(content: dict, optype: str = 'dbresult')-> dict:
    '''
    Static method of normalising database results\n
    Converts _id from mongodb to id

    @param optype str type of operation: dbresult or params
    @param content Dict|List[Dict] Database result
    @return Dict|List[List] of normalized content
    '''
    normalized = {}
    if DBMS.Database.dbms == 'mongodb':
        if optype == 'dbresult':
            elem = dict(content)
            elem['id'] = str(elem['_id'])
            del elem['_id']
            for key in elem.keys():
                if key.endswith('_id'):
                    elem[key] = str(elem[key])
            normalized =  elem
            
        else:
            if 'id' in content.keys():
                content['_id'] = ObjectId(content['id'])
                del content['id']
            for key in content.keys():
                if key.endswith('_id'):
                    content[key] = ObjectId(content[key])
            for key, value in content.items():
                if type(value) == list:
                    content[key] = '::'.join([str(v) for v in value])
            normalized = content
        return normalized
    return content

class DBMS(object):
    Database: Type[MongoDB]|Type[MysqlDB]|None = None
    
    @staticmethod
    def initialize(dbms: Literal['mysql', 'mongodb'], host: str = '127.0.0.1', port: int = 0, username: str = '', password: str = '', database: str = ''):
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
            DBMS.Database = MongoDB
            
        elif dbms == 'mysql':
            MysqlDB.initialize(host, port, username, password, database)
            DBMS.Database = MysqlDB
        
        elif dbms == 'sqlite':
            SqliteDB.initialize(database)
            DBMS.Database = SqliteDB
            
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
        
        DBMS.initialize(**settings)
    