from pymongo import MongoClient
from flask import Flask

import os
from dotenv import load_dotenv

load_dotenv()


class MongoDB(object):
    db = None
    dbms = 'mongodb'
    
    @staticmethod
    def initialize(host, port, database, user=None, password=None):
        client = MongoClient(host, int(port))
        MongoDB.db = client[database]

    @staticmethod
    def insert(collection, data):
        return MongoDB.db[collection].insert_one(data)

    @staticmethod
    def find(collection, query):
        return MongoDB.db[collection].find(query)

    @staticmethod
    def find_one(collection, query):
        return MongoDB.db[collection].find_one(query)

    @staticmethod
    def remove(collection, query):
        return MongoDB.db[collection].delete_many(query)

    @staticmethod
    def update(collection, query, data):
        return MongoDB.db[collection].update_one(query, {'$set': data}, upsert=True)
    
    @staticmethod
    def update_many(collection, query, data):
        return MongoDB.db[collection].update_many(query, {'$set': data}, upsert=True)
    
    @staticmethod
    def count(collection, query: dict = {})->int:
        return MongoDB.db[collection].count_documents(query)

