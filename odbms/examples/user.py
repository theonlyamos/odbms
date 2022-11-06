from datetime import datetime
from typing import Dict, List
import uuid

from bson.objectid import ObjectId

from odbms import DBMS, Model


class User(Model):
    '''A model class for user'''
    TABLE_NAME = 'users'

    def __init__(self, email, name, password, created_at=None, updated_at=None, id=None):
        super().__init__(created_at, updated_at, id)
        self.email = email
        self.name = name
        self.password = password
    
    @staticmethod
    def create_table():
        '''
        Create Model Table in database
        
        @paramas None
        @return Database query result
        '''
        
        AUTO_INCREMENT = 'AUTO_INCREMENT' if DBMS.Database.dbms == 'mysql' else 'AUTOINCREMENT'
        INT = 'INT' if DBMS.Database.dbms == 'mysql' else 'INTEGER'
        return DBMS.Database.query(f'''
            CREATE TABLE IF NOT EXISTS {User.TABLE_NAME}
            (
            id {INT} NOT NULL PRIMARY KEY {AUTO_INCREMENT} ,
            name varchar(50) not null,
            email varchar(100) not null,
            password varchar(500) not null,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
            ''')

    def save(self):
        '''
        Instance Method for saving User instance to database

        @params None
        @return None
        '''

        data = {
            "name": self.name,
            "email": self.email,
            "password": Utils.hash_password(self.password)
        }

        if DBMS.Database.dbms == 'mongodb':
            data["created_at"] = self.created_at
            data["updated_at"] = self.updated_at

        return DBMS.Database.insert(User.TABLE_NAME, data)
    
    def reset_password(self, new_password: str):
        '''
        Instance Method for resetting user password

        @param new_password User's new password
        @return None
        '''

        DBMS.Database.update(User.TABLE_NAME, User.normalise({'id': self.id}, 'params'), {'password': new_password})
    
    def json(self)-> dict:
        '''
        Instance Method for converting User Instance to Dict

        @paramas None
        @return dict() format of Function instance
        '''

        return {
            "id": str(self.id),
            "name": self.name,
            "email": self.email,
            "created_at": self.created_at,
            "updated_at": self.updated_at
        }

    @classmethod
    def get_by_email(cls, email: str):
        '''
        Class Method for retrieving user by email address

        @param email email address of the user 
        @return User instance
        '''
        user = DBMS.Database.find_one(User.TABLE_NAME, {"email": email})
        return cls(**Model.normalise(user)) if user else None