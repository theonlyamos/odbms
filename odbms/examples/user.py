
from bson.objectid import ObjectId
from odbms import DBMS, Model
from datetime import datetime
from utils import Utils

class User(Model):
    '''A model class for user'''
    TABLE_NAME = 'users'

    def __init__(self, email: str, name: str, password: str, 
                 image: str = '', gat: str ='', 
                 grt: str ='', created_at=None, 
                 updated_at=None, id=None):
        super().__init__(created_at, updated_at, id)
        self.email = email
        self.name = name
        self.password = password
        self.image = image
        self.gat = gat
        self.grt = grt
    
    def save(self):
        '''
        Instance Method for saving User instance to Database

        @params None
        @return None
        '''
        
        data = self.__dict__.copy()

        if DBMS.Database.dbms != 'mongodb':
            data['updated_at'] = (datetime.now()).strftime("%a %b %d %Y %H:%M:%S")
            del data["created_at"]
            del data["updated_at"]

        if isinstance(self.id, ObjectId):
            data['password'] = Utils.hash_password(self.password)
            return DBMS.Database.insert(User.TABLE_NAME, Model.normalise(data, 'params'))
        
        # Update the existing record in database
        del data['password']
        return DBMS.Database.update(self.TABLE_NAME, self.normalise({'id': self.id}, 'params'), self.normalise(data, 'params'))

if __name__ == '__main__':
    DBMS.initialize_with_defaults('sqlite', 'database')
    # DBMS.initialize_with_defaults('mongodb', 'database')
    # DBMS.initialize('postgresql', port=5432, username='postgres', password='', database='database')
    # User.create_table()
    # new_user = User('test@user.com', 'Test User', 'My Passwrod').save()
    users = User.all()
    # user = User.get('661fe85e132b3db29c05a8ba')
