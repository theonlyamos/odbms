# odbms

odbms is a Python package for managing MySQL, MongoDB, SQLite, and PostgreSQL database instances.

## Features

- Supports multiple database management systems (MySQL, MongoDB, SQLite, PostgreSQL)
- Provides a unified interface for database operations across different DBMSs
- Includes an ORM-like model system for easy data manipulation
- Supports table creation, alteration, and basic CRUD operations

## Installation

Install odbms using pip:

```shell
pip install odbms
```

## Usage

```python
from odbms import DBMS

# Initialize with default settings
DBMS.initialize_with_defaults('sqlite', 'database_name')

# Or initialize with custom settings
DBMS.initialize('postgresql', port=5432, username='postgres', password='', database='database_name')
```

```python
from odbms import Model

class User(Model):
    TABLE_NAME = 'users'

    def init(self, email: str, name: str, password: str, image: str = '', gat: str ='', grt: str ='', created_at=None, updated_at=None, id=None):
        super().init(created_at, updated_at, id)

        self.email = email
        self.name = name
        self.password = password
        self.image = image
        self.gat = gat
        self.grt = grt

#Create the table if not mongodb (sqlite, mysql, postgresql)
User.create_table()
```

```python
# Insert a new user
new_user = User('test@user.com', 'Test User', 'MyPassword')
new_user.save()

# Retrieve all users
users = User.all()

# Find a specific user
user = User.find_one({'email': 'test@user.com'})

# Update a user
user.name = 'Updated Name'
user.save()

# Get the json representation of the user
user.json()

# Delete a user
Delete a user
User.remove({'email': 'test@user.com'})
```
