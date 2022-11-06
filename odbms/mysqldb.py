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
    dbms = 'mysql'
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
    def initialize(host, port, username, password, database):
        dbsettings = {
            'host': host,
            'port': port,
            'user': username,
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
            MysqlDB.cursor.execute(query)
            MysqlDB.db.commit()

            return str(MysqlDB.cursor.lastrowid)

        except Exception as e:
            return {'status': 'Error', 'message': str(e)}
    
    @staticmethod
    def update(table: str, filter: dict, data: dict):

        query = f'UPDATE {table} SET'
        for key in data.keys():
            query += f" {key}= ?,"
        query = query.rstrip(',')
        
        if len(filter.keys()):
            query += ' WHERE '
            for key, value in filter.items():
                query += f"{key} = ?,"
            query = query.rstrip(',')
        
        parameters = list(data.values())+list(filter.values())

        try:
            MysqlDB.cursor.execute(query, tuple(parameters))
            MysqlDB.db.commit()

            return str(MysqlDB.cursor.lastrowid)

        except Exception as e:
            return {'status': 'Error', 'message': str(e)}
    
    @staticmethod
    def find(table: str, filter: dict = {}, columns: list = ['*']):
        columns = ', '.join(columns)
        query = f'SELECT {columns} FROM {table}'
        
        if len(filter.keys()):
            query += ' WHERE '
            for key, value in filter.items():
                query += f"{key} = ?,"
            query = query.rstrip(',')

        try:
            MysqlDB.cursor.execute(query, tuple(filter.values()))
            MysqlDB.db.commit()

            return [x for x in MysqlDB.cursor.fetchall()]

        except Exception as e:
            return {'status': 'Error', 'message': str(e)}
    
    @staticmethod
    def find_one(table: str, filter: dict = {}, columns: list = ['*']):
        columns = ', '.join(columns)
        query = f'SELECT {columns} FROM {table}'
        
        if len(filter.keys()):
            query += ' WHERE '
            for key, value in filter.items():
                query += f"{key}= ?,"
            query = query.rstrip(',')
        
        try:
            MysqlDB.cursor.execute(query, tuple(filter.values()))
            MysqlDB.db.commit()

            return MysqlDB.cursor.fetchone()

        except Exception as e:
            return {'status': 'Error', 'message': str(e)}
    
    @staticmethod
    def count(table: str, filter: dict = {}, columns: list = ['*'])-> int:
        columns = ', '.join(columns)
        query = f'SELECT {columns} FROM {table}'

        if len(filter.keys()):
            query += ' WHERE '
            for key, value in filter.items():
                query += f"{key} = ?,"
            query = query.rstrip(',')

        try:
            MysqlDB.cursor.execute(query, tuple(filter.values()))
            MysqlDB.db.commit()

            return MysqlDB.cursor.rowcount

        except Exception as e:
            return {'status': 'Error', 'message': str(e)}
    
    @staticmethod
    def sum(table: str, column: str, filter: dict = {}):
        query = f'SELECT SUM({column}) as sum FROM {table}'
        
        if len(filter.keys()):
            query += ' WHERE '
            for key, value in filter.items():
                query += f"{key} = ?,"
            query = query.rstrip(',')
        
        try:
            MysqlDB.cursor.execute(query, tuple(filter.values()))
            MysqlDB.db.commit()

            return MysqlDB.cursor.fetchall()[0]['sum']

        except Exception as e:
            return {'status': 'Error', 'message': str(e)}
    
    @staticmethod
    def query(query: str):
        try:
            MysqlDB.cursor.execute(query)
            MysqlDB.db.commit()

            return [x for x in MysqlDB.cursor.fetchall()]

        except Exception as e:
            return {'status': 'Error', 'message': str(e)}
    
    @staticmethod
    def remove(table: str, filter: dict):
        query = f'DELETE FROM {table} WHERE '
        
        if len(filter.keys()):
            query += ' WHERE '
            for key, value in filter.items():
                query += f"{key} = ?,"
            query = query.rstrip(',')
        
        try:
            MysqlDB.cursor.execute(query, tuple(filter.values()))
            MysqlDB.db.commit()

            return str(MysqlDB.cursor.lastrowid)

        except Exception as e:
            return {'status': 'Error', 'message': str(e)}
    
    @staticmethod
    def delete(table: str, filter: dict):
        query = f'DELETE FROM {table} WHERE '
        
        if len(filter.keys()):
            query += ' WHERE '
            for key, value in filter.items():
                query += f"{key} = ?,"
            query = query.rstrip(',')
        
        try:
            MysqlDB.cursor.execute(query, tuple(filter.values()))
            MysqlDB.db.commit()

            return str(MysqlDB.cursor.lastrowid)

        except Exception as e:
            return {'status': 'Error', 'message': str(e)}
            
    @staticmethod
    def import_from_file(filename: str):
        '''
        Run Database command from file
        
        @param filename Name of file containing commands
        @return Database result
        '''

        try:
            result = None
            with open(filename, 'rt') as file:
                result = MysqlDB.query(file.read())
                
            return result
                
        except Exception as e:
            return {'status': 'Error', 'message': str(e)}