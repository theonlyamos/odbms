# ODBMS - Object Document/Relational Mapping System

A flexible and modern Python ORM supporting multiple databases (MongoDB, PostgreSQL, MySQL) with both synchronous and asynchronous operations.

## Features

- Support for multiple databases:
  - SQLite (using sqlite3)
  - MongoDB (using Motor)
  - PostgreSQL (using aiopg)
  - MySQL (using aiomysql)
- Both synchronous and asynchronous operations
- Connection pooling for better performance
- Type-safe field definitions
- Pydantic integration for validation
- Automatic table/collection creation
- Relationship handling
- Computed fields
- Flexible query interface

## Installation

```bash
pip install -r requirements.txt
```

## Quick Start

```python
from odbms import Model, StringField, IntegerField, EmailField
from odbms.dbms import DBMS

# Initialize database connection
DBMS.initialize(
    dbms='postgresql',  # or 'mongodb', 'mysql'
    host='localhost',
    port=5432,
    database='mydb',
    username='user',
    password='pass'
)

# Define your model
class User(Model):
    name: str = StringField()
    email: str = EmailField()
    age: int = IntegerField(min_value=0)

# Create a new user
user = User(name='John Doe', email='john@example.com', age=30)
await user.save_async()  # or user.save() for sync operation

# Find users
users = await User.find_async({'age': {'$gte': 25}})  # or User.find() for sync
```

## Field Types

- `StringField`: For text data
- `IntegerField`: For integer values
- `FloatField`: For floating-point numbers
- `BooleanField`: For true/false values
- `DateTimeField`: For timestamps
- `EmailField`: For email addresses with validation
- `IDField`: For primary keys/IDs
- `ComputedField`: For dynamically computed values
- `ListField`: For arrays/lists
- `DictField`: For nested documents/objects

## Database Operations

### Synchronous Operations

```python
# Create
user = User(name='John', email='john@example.com')
user.save()

# Read
user = User.find_one({'email': 'john@example.com'})
users = User.find({'age': {'$gte': 25}})
all_users = User.all()

# Update
User.update({'age': {'$lt': 18}}, {'is_minor': True})

# Delete
User.remove({'status': 'inactive'})

# Aggregation
total_age = User.sum('age', {'country': 'US'})
```

### Asynchronous Operations

```python
# Create
user = User(name='Jane', email='jane@example.com')
await user.save_async()

# Read
user = await User.find_one_async({'email': 'jane@example.com'})
users = await User.find_async({'age': {'$gte': 25}})
all_users = await User.all_async()

# Update
await User.update_async({'age': {'$lt': 18}}, {'is_minor': True})

# Delete
await User.remove_async({'status': 'inactive'})

# Aggregation
total_age = await User.sum_async('age', {'country': 'US'})
```

## Relationships

```python
class Post(Model):
    title: str = StringField()
    content: str = StringField()
    author_id: str = IDField()

class User(Model):
    name: str = StringField()
    posts: List[Post] = ListField(model=Post)

# Create related records
user = User(name='John')
await user.save_async()

post = Post(title='Hello', content='World', author_id=user.id)
await post.save_async()

# Access relationships
user_posts = await user.posts  # Automatically fetches related posts
```

## Testing

Run the test suite:

```bash
pytest tests/
```

The test suite includes comprehensive tests for:

- All database operations (CRUD)
- Both sync and async operations
- Field validations
- Relationships
- Computed fields
- Aggregations

## Requirements

- Python 3.7+
- pydantic >= 2.0.0
- motor >= 3.3.0 (for MongoDB)
- aiopg >= 1.4.0 (for PostgreSQL)
- aiomysql >= 0.2.0 (for MySQL)
- inflect >= 5.0.0
- python-dotenv >= 0.19.0

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

MIT License
