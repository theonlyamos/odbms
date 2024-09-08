from odbms import DBMS, Model

class User(Model):
    '''A model class for user'''
    name: str
    email: str

if __name__ == '__main__':
    DBMS.initialize_with_defaults('mongodb', 'test')
    # User.create_table()
    # new_user = User(name='Test User', email='test@user.com').save()
    users = User.all()
    print([user.dict() for user in users])
    # user = User.get('661fe85e132b3db29c05a8ba')
