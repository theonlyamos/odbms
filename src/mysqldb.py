#!python3
# -*- coding: utf-8 -*-
# @Date    : 2022-07-22 11:42:39
# @Author  : Amos Amissah (theonlyamos@gmai.com)
# @Link    : link
# @Version : 1.0.0

import os
from mysql import connector
from sys import exit

class MysqlDB:
    db = None
    cursor = None
    database_exists = True
    
    @staticmethod
    def connect(dbsettings: dict):
        '''Connection method'''
        try:
            return connector.connect(**dbsettings, auth_plugin='mysql_native_password')
        except Exception as e:
            if 'Unknown database' in str(e):
                MysqlDB.database_exists = False
                del dbsettings['database']
                return connector.connect(**dbsettings, auth_plugin='mysql_native_password')
            else:
                print(str(e))
                exit(1)
    
    @staticmethod
    def initialize(host, port, database, user, password):
        dbsettings = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database
        }

        db = MysqlDB.connect(dbsettings)

        cursor = db.cursor(buffered=True, dictionary=True)
        MysqlDB.db = db
        MysqlDB.cursor = cursor
    
    @staticmethod
    def insert(table: str, data: dict):

        query = f'INSERT INTO {table}('
        query += ', '.join(data.keys())
        query += ") VALUES('"

        values = [str(val) for val in data.values()]
        query += "','".join(values)
        query += "')"
        try:
            MysqlDB.cursor.execute(query, None)
            MysqlDB.db.commit()

            return str(MysqlDB.cursor.lastrowid)

        except Exception as e:
            return {'status': 'Error', 'message': str(e)}
    
    @staticmethod
    def update(table: str, params: dict, data: dict):

        query = f'UPDATE {table} SET'
        for key in data.keys():
            query += f" {key}='{data[key]}',"
        
        query = query.rstrip(',')
        
        query += f" WHERE "
        for key, value in params.items():
            query += f"{key}='{value}',"
        query = query.rstrip(',')

        try:
            MysqlDB.cursor.execute(query, None)
            MysqlDB.db.commit()

            return str(MysqlDB.cursor.lastrowid)

        except Exception as e:
            return {'status': 'Error', 'message': str(e)}
    
    @staticmethod
    def find(table: str, params: dict = {}):
        query = f'SELECT * FROM {table}'

        if len(params.keys()):
            query += ' WHERE '
            for key, value in params.items():
                query += f"{key}='{value}',"
            query = query.rstrip(',')
        
        try:
            MysqlDB.cursor.execute(query, None)
            MysqlDB.db.commit()

            return [x for x in MysqlDB.cursor.fetchall()]

        except Exception as e:
            return {'status': 'Error', 'message': str(e)}
    
    @staticmethod
    def find_one(table: str, params: dict = {}):
        query = f'SELECT * FROM {table}'
        
        if len(params.keys()):
            query += ' WHERE '
            for key, value in params.items():
                query += f"{key}='{value}',"
            query = query.rstrip(',')
        
        try:
            MysqlDB.cursor.execute(query, None)
            MysqlDB.db.commit()

            resp = [x for x in MysqlDB.cursor.fetchall()]
            return resp[0]

        except Exception as e:
            return {'status': 'Error', 'message': str(e)}
    
    @staticmethod
    def count(table: str, params: dict = {})-> int:
        query = f'SELECT * FROM {table}'

        if len(params.keys()):
            query += ' WHERE '
            for key, value in params.items():
                query += f"{key}='{value}',"
            query = query.rstrip(',')

        try:
            MysqlDB.cursor.execute(query, None)
            MysqlDB.db.commit()

            return MysqlDB.cursor.rowcount

        except Exception as e:
            return {'status': 'Error', 'message': str(e)}
    
    @staticmethod
    def sum(table: str, column: str):
        query = f'SELECT SUM({column}) as sum FROM {table}'
        
        try:
            MysqlDB.cursor.execute(query, None)
            MysqlDB.db.commit()

            return MysqlDB.cursor.fetchall()[0]['sum']

        except Exception as e:
            return {'status': 'Error', 'message': str(e)}
    
    @staticmethod
    def query(query: str):
        try:
            MysqlDB.cursor.execute(query, None)
            MysqlDB.db.commit()

            return [x for x in MysqlDB.cursor.fetchall()]

        except Exception as e:
            return {'status': 'Error', 'message': str(e)}
    
    @staticmethod
    def remove(table: str, params: dict):

        query = f'DELETE FROM {table} WHERE '
        for key, value in params.items():
            query += f"{key}='{value}',"
        query = query.rstrip(',')
        
        try:
            MysqlDB.cursor.execute(query, None)
            MysqlDB.db.commit()

            return str(MysqlDB.cursor.lastrowid)

        except Exception as e:
            return {'status': 'Error', 'message': str(e)}