from pymongo import MongoClient

import os
from typing import Union
from dotenv import load_dotenv

load_dotenv()


class MongoDB(object):
    db = None
    dbms = 'mongodb'
    
    @staticmethod
    def initialize(host, port, database, user=None, password=None):
        if port:
            client = MongoClient(host, int(port))
        else:
            client = MongoClient(host)
        MongoDB.db = client[database]

    @staticmethod
    def insert(collection: str, data: dict):
        return MongoDB.db[collection].insert_one(data)

    @staticmethod
    def insert_many(collection: str, data: dict):
        return MongoDB.db[collection].insert_many(data)

    @staticmethod
    def find(collection: str, filter: dict = {}, projection: Union[list,dict] = []):
        return MongoDB.db[collection].find(filter, projection)

    @staticmethod
    def find_one(collection: str, filter: dict = {}, projection: Union[list,dict] = []):
        return MongoDB.db[collection].find_one(filter, projection)

    @staticmethod
    def remove(collection: str, filter: dict):
        return MongoDB.db[collection].delete_many(filter)

    @staticmethod
    def delete(collection: str, filter: dict):
        return MongoDB.db[collection].delete_many(filter)


    @staticmethod
    def update(collection: str, filter: dict, data: dict):
        print(filter)
        return MongoDB.db[collection].update_one(filter, {'$set': data}, upsert=True)
    
    @staticmethod
    def update_many(collection: str, filter: dict, data: dict):
        return MongoDB.db[collection].update_many(filter, {'$set': data}, upsert=True)
    
    @staticmethod
    def count(collection: str, filter: dict = {})->int:
        return MongoDB.db[collection].count_documents(filter)

