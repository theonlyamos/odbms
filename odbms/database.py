#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2022-08-06 17:37:00
# @Author  : Amos Amissah (theonlyamos@gmail.com)
# @Link    : link
# @Version : 1.0.0

from typing import Type, Literal, Optional
from .orms.mongodb import MongoDB
from .orms.mysqldb import MysqlDB
from .orms.sqlitedb import SqliteDB
from .orms.postgresqldb import PostgresqlDB
# from .model import Model


class Database():
    db: Type[MysqlDB]|Type[MongoDB]|Type[PostgresqlDB]|Type[SqliteDB]|None = None
    dbms: str|None = None
    # models: Type[Models] = Models()

    @staticmethod
    def initialize(dbsettings: dict, app=None):
        Database.dbms = dbsettings['dbms']
        if dbsettings['dbms'] == 'mongodb':
            MongoDB.initialize(dbsettings['dbhost'], 
                dbsettings['dbport'],
                dbsettings['dbname'])
            Database.db = MongoDB
            
        elif dbsettings['dbms'] == 'mysql':
            MysqlDB.initialize(dbsettings['dbhost'], 
                dbsettings['dbport'],
                dbsettings['dbname'],
                dbsettings['dbusername'],
                dbsettings['dbpassword'])
            Database.db = MysqlDB
            
        elif dbsettings['dbms'] == 'postgresql':
            PostgresqlDB.initialize(dbsettings['dbhost'], 
                dbsettings['dbport'],
                dbsettings['dbname'],
                dbsettings['dbusername'],
                dbsettings['dbpassword'])
            Database.db = PostgresqlDB
            
        elif dbsettings['dbms'] == 'sqlite':
            SqliteDB.initialize(dbsettings['dbname'])
            Database.db = SqliteDB
    
    # @staticmethod
    # def add_model(model: Model):
    #     '''Add a model to be loaded as a database table'''
    #     setattr(Database.models, model.TABLE_NAME, model)
    #     model.Database = Database
    
    @staticmethod
    def setup():
        if Database.dbms in ['postgresql', 'mysql', 'sqlite']:
            print('[-!-] Creating Database Tables')
            print('[~] Creating users table')
            Database.db.query('''
            CREATE TABLE IF NOT EXISTS users
            (
            id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
            name varchar(50) not null,
            email varchar(100) not null,
            password varchar(500) not null,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
            ''')

    
    @staticmethod
    def load_defaults(dbms: Literal['mysql','mongodb', 'postgresql', 'sqlite'], database: Optional[str] = None):
        settings = {}
        settings['dbms'] = dbms
        settings['dbhost'] = '127.0.0.1'
        settings['dbport'] = 3306 if dbms == 'mysql' else 27017
        settings['dbusername'] = 'root' if dbms == 'mysql' else ''
        settings['dbpassword'] = ''
        if database:
            settings['dbname'] = database
        
        Database.initialize(settings)
