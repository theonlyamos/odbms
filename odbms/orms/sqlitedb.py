#!python3
# -*- coding: utf-8 -*-
# @Date    : 2022-10-23 10:02:39
# @Author  : Amos Amissah (theonlyamos@gmai.com)
# @Link    : link
# @Version : 1.0.0

import os
import logging
import sqlite3
from sqlite3 import Connection, Cursor
from typing import Union

from .base import ORM

class SqliteDB(ORM):
    db: Connection
    cursor: Cursor
    dbms = 'sqlite'
    
    @staticmethod
    def initialize(database):
        db = sqlite3.connect(database)

        cursor = db.cursor()
        SqliteDB.db = db
        SqliteDB.cursor = cursor
    
    @staticmethod
    def insert(table: str, data: dict):

        query = f'INSERT INTO {table} ('
        query += ', '.join(data.keys())
        query += ") VALUES ('"

        values = [str(val) for val in data.values()]
        query += "','".join(values)
        query += "')"
        try:
            SqliteDB.cursor.execute(query)
            SqliteDB.db.commit()

            return str(SqliteDB.cursor.lastrowid)

        except Exception as e:
            logging.error(f"Error: {str(e)}")
            return 0
    
    @staticmethod
    def insert_many(tale: str, data: list[dict]):
        return {}
    
    @staticmethod
    def update(table: str, params: dict, data: dict)-> Union[int, None]:

        query = f'UPDATE {table} SET'
        for key in data.keys():
            query += f" {key}= ?,"
        query = query.rstrip(',')
        
        if len(params.keys()):
            query += ' WHERE '
            for key, value in params.items():
                query += f"{key} = ?,"
            query = query.rstrip(',')
        
        parameters = list(data.values())+list(params.values())

        try:
            SqliteDB.cursor.execute(query, tuple(parameters))
            SqliteDB.db.commit()

            return SqliteDB.cursor.lastrowid

        except Exception as e:
            logging.error(str(e))
            return None
    
    @staticmethod
    def find(table: str, params: dict = {}, columns: list = ['*']):
        query = f'SELECT {", ".join(columns)} FROM {table}'

        if len(params.keys()):
            query += ' WHERE '
            for key, value in params.items():
                query += f"{key} = ?,"
            query = query.rstrip(',')
            
        try:
            SqliteDB.cursor.execute(query, tuple(params.values()))
            SqliteDB.db.commit()
            
            return SqliteDB.cursor.fetchall()

        except Exception as e:
            logging.error({'status': 'Error', 'message': str(e)})
            return []
    
    @staticmethod
    def find_one(table: str, params: dict = {}):
        query = f'SELECT * FROM {table}'
        
        if len(params.keys()):
            query += ' WHERE '
            for key, value in params.items():
                query += f"{key} = ?,"
            query = query.rstrip(',')
        
        try:
            SqliteDB.cursor.execute(query, tuple(params.values()))
            SqliteDB.db.commit()

            resp = [x for x in SqliteDB.cursor.fetchall()]
            
            return resp[0] if len(resp) else {}

        except Exception as e:
            logging.error({'status': 'Error', 'message': str(e)})
            return []
    
    @staticmethod
    def count(table: str, params: dict = {})-> int:
        query = f'SELECT * FROM {table}'

        if len(params.keys()):
            query += ' WHERE '
            for key, value in params.items():
                query += f"{key} = ?,"
            query = query.rstrip(',')

        try:
            SqliteDB.cursor.execute(query, tuple(params.values()))
            SqliteDB.db.commit()

            return SqliteDB.cursor.rowcount

        except Exception as e:
            logging.error(str(e))
            return 0
    
    @staticmethod
    def sum(table: str, column: str, params: dict = {})-> int:
        query = f'SELECT SUM({column}) as sum FROM {table}'
        
        if len(params.keys()):
            query += ' WHERE '
            for key, value in params.items():
                query += f"{key} = ?,"
            query = query.rstrip(',')
        
        try:
            SqliteDB.cursor.execute(query, tuple(params.values()))
            SqliteDB.db.commit()

            return SqliteDB.cursor.fetchall()[0]['sum']

        except Exception as e:
            logging.error(str(e))
            return 0
    
    @staticmethod
    def execute(query: str):
        try:
            SqliteDB.cursor.execute(query)
            SqliteDB.db.commit()

            return [x for x in SqliteDB.cursor.fetchall()]

        except Exception as e:
            return {'status': 'Error', 'message': str(e)}
    
    @staticmethod
    def remove(table: str, params: dict = {}):
        query = f'DELETE FROM {table}'
        
        if len(params.keys()):
            query += ' WHERE '
            for key, value in params.items():
                query += f"{key} = ?,"
            query = query.rstrip(',')
        
        try:
            SqliteDB.cursor.execute(query, tuple(params.values()))
            SqliteDB.db.commit()

            return str(SqliteDB.cursor.lastrowid)

        except Exception as e:
            return {'status': 'Error', 'message': str(e)}
    
    @staticmethod
    def delete(table: str, params: dict = {}):
        query = f'DELETE FROM {table}'
        
        if len(params.keys()):
            query += ' WHERE '
            for key, value in params.items():
                query += f"{key} = ?,"
            query = query.rstrip(',')
        
        try:
            SqliteDB.cursor.execute(query, tuple(params.values()))
            SqliteDB.db.commit()

            return str(SqliteDB.cursor.lastrowid)

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
                result = SqliteDB.execute(file.read())
                
            return result
                
        except Exception as e:
            return {'status': 'Error', 'message': str(e)}