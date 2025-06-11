from datetime import datetime
from typing import Optional, Self, Union, Any, List, Dict, Type, ClassVar, cast, Callable, Coroutine, Annotated, get_args, get_origin
import inspect
import json
import asyncio

from bson.objectid import ObjectId
import inflect
from pydantic import BaseModel, Field, ValidationError, field_serializer
from pydantic.json import pydantic_encoder
from .dbms import DBMS
from .orms.mongodb import MongoDB
from .fields import (
    Field as ModelField,
    RelationshipField,
    ComputedField,
    OneToMany,
    ManyToMany,
)

class ModelMetaclass(type(BaseModel)):
    """Metaclass for Model to handle field definitions and inheritance."""
    
    def __new__(mcs, name: str, bases: tuple, attrs: dict):
        if name == 'Model':
            return super().__new__(mcs, name, bases, attrs)
        
        # Collect fields from parent classes
        fields: Dict[str, ModelField] = {}
        for base in bases:
            if hasattr(base, '_fields'):
                fields.update(getattr(base, '_fields', {}))
        
        # Process field definitions
        new_fields = {}
        for key, value in attrs.items():
            if isinstance(value, ModelField):
                new_fields[key] = value
                # Remove the field definition to avoid descriptor issues
                attrs[key] = value.default if value.default is not None else None
        
        # Update with new fields
        fields.update(new_fields)
        
        # Store fields in class
        attrs['_fields'] = fields
        
        # Create computed properties for relationships
        for name, field in fields.items():
            if isinstance(field, RelationshipField):
                attrs[f'_get_{name}'] = mcs._create_relationship_getter(name, field)
                attrs[f'_set_{name}'] = mcs._create_relationship_setter(name, field)
                attrs[name] = property(attrs[f'_get_{name}'], attrs[f'_set_{name}'])
        
        return super().__new__(mcs, name, bases, attrs)
    
    @staticmethod
    def _create_relationship_getter(name: str, field: RelationshipField):
        def getter(self):
            if field.lazy and field._cached_value is None:
                # Import here to avoid circular imports
                from importlib import import_module
                
                # Get the related model class
                module_path, model_name = field.model.rsplit('.', 1)
                module = import_module(module_path)
                model_class = getattr(module, model_name)
                
                # Handle different relationship types
                if isinstance(field, (OneToMany, ManyToMany)):
                    ids = getattr(self, f'_{name}_ids', [])
                    field._cached_value = [model_class.get(id) for id in ids if id]
                else:
                    id = getattr(self, f'_{name}_id')
                    field._cached_value = model_class.get(id) if id else None
            
            return field._cached_value
        return getter
    
    @staticmethod
    def _create_relationship_setter(name: str, field: RelationshipField):
        def setter(self, value):
            if isinstance(field, (OneToMany, ManyToMany)):
                if not isinstance(value, (list, tuple)):
                    raise ValueError(f"{name} must be a list or tuple")
                setattr(self, f'_{name}_ids', [item.id for item in value])
            else:
                setattr(self, f'_{name}_id', value.id if value else None)
            field._cached_value = value
        return setter

class PyObjectId(ObjectId):
    @classmethod
    def __get_pydantic_core_schema__(cls, _source_type, _handler):
        from pydantic_core.core_schema import str_schema
        return str_schema()

    @classmethod
    def validate(cls, v, k = None):
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid ObjectId")
        return ObjectId(v)

    @classmethod
    def __get_pydantic_json_schema__(cls, field_schema):
        field_schema.update(type="string")

class Model(BaseModel, metaclass=ModelMetaclass):
    '''Base model class with enhanced features'''
    
    model_config = {
        'arbitrary_types_allowed': True,
        'from_attributes': True
    }
    
    id: Optional[Union[str, int, PyObjectId]] = Field(alias="_id", default=None)
    created_at: Annotated[datetime, Field(default_factory=datetime.now)]
    updated_at: Annotated[datetime, Field(default_factory=datetime.now)]
    
    # Class variables for table configuration
    __abstract__: ClassVar[bool] = False
    __table_name__: ClassVar[Optional[str]] = None
    _fields: ClassVar[Dict[str, ModelField]] = {}
    
    # Async event hooks
    _before_save_hooks: ClassVar[List[Callable[[Any], Coroutine[Any, Any, None]]]] = []
    _after_save_hooks: ClassVar[List[Callable[[Any], Coroutine[Any, Any, None]]]] = []
    _before_delete_hooks: ClassVar[List[Callable[[Any], Coroutine[Any, Any, None]]]] = []
    _after_delete_hooks: ClassVar[List[Callable[[Any], Coroutine[Any, Any, None]]]] = []
    
    def __init__(self, **data):
        # First call Pydantic's __init__ to properly initialize the model
        super().__init__(**data)
        
        # Initialize _dynamic_fields
        object.__setattr__(self, '_dynamic_fields', {})
        
        # Initialize relationships
        for name, field in self._fields.items():
            if isinstance(field, RelationshipField):
                if isinstance(field, (OneToMany, ManyToMany)):
                    setattr(self, f'_{name}_ids', [])
                else:
                    setattr(self, f'_{name}_id', None)
        
        # Process any remaining data that wasn't handled by Pydantic
        for key, value in data.items():
            if key not in self.__annotations__:
                self._dynamic_fields[key] = value
    
    def __setattr__(self, name, value):
        # Check if it's a defined field in the model
        if name in self.__annotations__:
            super().__setattr__(name, value)
        else:
            # Store undefined fields in _dynamic_fields
            self._dynamic_fields[name] = value
    
    def __getattr__(self, name):
        # This is only called for attributes that don't exist in normal lookup
        if name in self._dynamic_fields:
            return self._dynamic_fields[name]
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")
    
    def model_dump(self, *args, **kwargs) -> Dict[str, Any]:
        """Override model_dump to include dynamic fields."""
        data = super().model_dump(*args, **kwargs)
        data.update(self._dynamic_fields)
        return data
    
    @classmethod
    def table_name(cls) -> str:
        '''Get the table name for the model.'''
        if cls.__table_name__:
            return cls.__table_name__
        
        name = cls.__name__.lower()
        p = inflect.engine()
        return cast(str, p.plural(name)) #type: ignore
    
    @classmethod
    async def create_table(cls):
        """
        Create the database table for the model (Only for relational databases).
        """
        if DBMS.Database is not None and DBMS.Database.dbms != 'mongodb':
            excluded = ['created_at', 'updated_at', 'id']
            columns = []
            additional_columns = {
                'sqlite': [
                    'created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP',
                    'updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP',
                    'id TEXT PRIMARY KEY',
                ],
                'postgresql': [
                    'created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP',
                    'updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP',
                    'id TEXT PRIMARY KEY',
                ],
                'mysql': [
                    'created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP',
                    'updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
                    'id VARCHAR(255) PRIMARY KEY',
                ],
                
            }
            # Get the model fields from Pydantic's model_fields attribute
            for field_name, field_info in cls.model_fields.items():
                param_name = field_name
                param_type = field_info.annotation
                
                column_type = cls.get_column_type(param_type)
                
                if param_name not in excluded:
                    column_def = f"{param_name} {column_type}"
                    columns.append(column_def)
                        
            columns += additional_columns.get(DBMS.Database.dbms, [])
            columns_str = ', '.join(columns)
            table_definition = f"CREATE TABLE IF NOT EXISTS {cls.table_name()} ({columns_str});"
            
            await DBMS.Database.query(table_definition)
            
            if DBMS.Database.dbms == 'sqlite':
                await DBMS.Database.query(f'''CREATE TRIGGER IF NOT EXISTS update_{cls.table_name()}
                AFTER UPDATE ON {cls.table_name()}
                BEGIN
                    UPDATE {cls.table_name()} SET updated_at = STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW') WHERE id = NEW.id;
                END;''')
            elif DBMS.Database.dbms == 'postgresql':
                await DBMS.Database.query(f"""CREATE OR REPLACE FUNCTION update_{cls.table_name()}_timestamp()
                RETURNS TRIGGER AS $$
                BEGIN
                    NEW.updated_at := CURRENT_TIMESTAMP;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;""")
                await DBMS.Database.query(f"""CREATE TRIGGER update_{cls.table_name()}_timestamp
                BEFORE UPDATE ON {cls.table_name()}
                FOR EACH ROW
                EXECUTE PROCEDURE update_{cls.table_name()}_timestamp();""")
    
    @classmethod
    async def drop_table(cls):
        """
        Drop the database table for the model.
        """
        if DBMS.Database is not None and DBMS.Database.dbms != 'mongodb':
            table_name = cls.table_name()
            query = f"DROP TABLE IF EXISTS {table_name};"
            await DBMS.Database.query(query)
    
    @staticmethod
    def get_column_type(attr_type: Any) -> str:
        """
        Map Python types to SQL column types.
        """
        
        type_mapping = {
            str: "TEXT",
            int: "INTEGER",
            float: "REAL",
            bool: "BOOLEAN",
            list: "TEXT",
            dict: "TEXT"
            # Add more mappings as needed
        }
        return type_mapping.get(attr_type, "TEXT")

    @classmethod
    async def alter_table(cls, changes: dict):
        """
        Alter the table structure by adding, modifying, or dropping columns.

        @param changes: A dictionary mapping column names to their new data types.
        """
        if DBMS.Database is not None and DBMS.Database.dbms != 'mongodb':
            # Fetch existing columns from the database
            fetch_columns_sql = f"SELECT column_name FROM information_schema.columns WHERE table_name='{cls.table_name()}';"
            existing_columns = {row['column_name'] for row in await DBMS.Database.query(fetch_columns_sql)} # type: ignore
            default_columns = {'id', 'created_at', 'updated_at'}
            # Determine columns to add or modify and columns to drop
            specified_columns = set(changes.keys())
            specified_columns.update(default_columns)
            columns_to_drop = existing_columns - specified_columns
            columns_to_add_or_modify = specified_columns - existing_columns
            
            alter_statements = []

            # Handle adding or modifying columns
            for column, data_type in changes.items():
                column_type = cls.get_column_type(data_type)
                if column in columns_to_add_or_modify:
                    alter_statements.append(f"ADD COLUMN {column} {column_type}")
                else:
                    # Modify existing column
                    if DBMS.Database.dbms in ['mysql', 'postgresql']:
                        alter_statements.append(f"ALTER COLUMN {column} TYPE {column_type}")
                    elif DBMS.Database.dbms == 'sqlite':
                        # SQLite does not support MODIFY COLUMN directly, needs table recreation
                        continue  # Handle SQLite modifications separately if needed

            # Handle dropping columns
            for column in columns_to_drop:
                if DBMS.Database.dbms in ['mysql', 'postgresql']:
                    alter_statements.append(f"DROP COLUMN {column}")
                elif DBMS.Database.dbms == 'sqlite':
                    # SQLite does not support DROP COLUMN directly, needs table recreation
                    continue  # Handle SQLite drops separately if needed

            # Execute all alter statements
            for statement in alter_statements:
                alter_sql = f"ALTER TABLE {cls.table_name()} {statement};"
                await DBMS.Database.query(alter_sql)
    
    def validate_fields(self):
        """Validate all fields."""
        for name, field in self._fields.items():
            value = getattr(self, name)
            try:
                validated = field.validate(value)
                setattr(self, name, validated)
            except ValueError as e:
                raise ValidationError(f"Validation error for field {name}: {str(e)}")
    
    def compute_fields(self):
        """Compute values for computed fields."""
        for name, field in self._fields.items():
            if isinstance(field, ComputedField):
                setattr(self, name, field.compute(self))
    
    @classmethod
    def normalise(cls, content: Optional[Dict[str, Any]] = None, optype: str = 'dbresult') -> Dict[str, Any]:
        if content is None:
            return {}
        
        if DBMS.Database is None:
            raise RuntimeError("Database not initialized")
            
        if isinstance(DBMS.Database, MongoDB):
            if optype == 'dbresult':
                content = dict(content)
                content['id'] = str(content.pop('_id'))
            else:
                if 'id' in content:
                    content['_id'] = ObjectId(content.pop('id'))
        else:
            if optype == 'params':
                # Handle MongoDB-style operators for SQL databases
                normalized = {}
                for key, value in content.items():
                    if isinstance(value, dict) and all(k.startswith('$') for k in value.keys()):
                        # Convert MongoDB operators to SQL
                        for op, val in value.items():
                            if op == '$lt':
                                normalized[f"{key} < ?"] = val
                            elif op == '$lte':
                                normalized[f"{key} <= ?"] = val
                            elif op == '$gt':
                                normalized[f"{key} > ?"] = val
                            elif op == '$gte':
                                normalized[f"{key} >= ?"] = val
                            elif op == '$ne':
                                normalized[f"{key} != ?"] = val
                            elif op == '$in':
                                normalized[f"{key} IN ?"] = tuple(val)
                            elif op == '$nin':
                                normalized[f"{key} NOT IN ?"] = tuple(val)
                    else:
                        # Handle normal key-value pairs
                        if isinstance(value, ObjectId):
                            normalized[key] = str(value)
                        elif isinstance(value, list):
                            normalized[key] = '::'.join(str(v) for v in value)
                        elif isinstance(value, datetime):
                            normalized[key] = value.isoformat()
                        elif isinstance(value, dict):
                            normalized[key] = json.dumps(value, default=pydantic_encoder)
                        else:
                            normalized[key] = value
                return normalized
            else:
                if not content:
                    return {}

                normalized_content = dict(content)
                model_fields = cls.model_fields

                for key, value in normalized_content.items():
                    if value is None or key not in model_fields:
                        continue

                    field_info = model_fields[key]
                    
                    # Handle Optional[T] by getting the underlying type
                    field_type = field_info.annotation
                    origin = get_origin(field_type)
                    
                    is_optional = origin is Union and type(None) in get_args(field_type)
                    if is_optional:
                        # Get the type other than None
                        field_type = next((t for t in get_args(field_type) if t is not type(None)), None)
                        if field_type is None:
                            continue
                        origin = get_origin(field_type) or field_type

                    try:
                        is_model_subclass = inspect.isclass(field_type) and issubclass(field_type, Model)
                        
                        if (origin is list or field_type is list) and isinstance(value, str):
                            normalized_content[key] = value.split('::') if value else []
                        elif (origin is dict or field_type is dict or is_model_subclass) and isinstance(value, str):
                            normalized_content[key] = json.loads(value) if value else {}
                        elif (field_type is datetime) and isinstance(value, str):
                            normalized_content[key] = datetime.fromisoformat(value)
                    except (ValueError, TypeError, json.JSONDecodeError):
                        # Keep original value if parsing fails.
                        pass
                return normalized_content
        return content
    
    async def delete(self, cascade: bool = False):
        """Delete the model instance and optionally related objects."""
        # Run before_delete hooks
        await self._run_hooks(self._before_delete_hooks)
        
        if cascade:
            # Delete related objects
            for name, field in self._fields.items():
                if isinstance(field, RelationshipField) and field.cascade:
                    related = getattr(self, name)
                    if isinstance(related, list):
                        for item in related:
                            await item.delete(cascade=True)
                    elif related:
                        await related.delete(cascade=True)
        
        if DBMS.Database is None:
            raise RuntimeError("Database not initialized")
        result = await DBMS.Database.delete_one(self.table_name(), self.normalise({'id': self.id}, 'params'))
        
        # Run after_delete hooks
        await self._run_hooks(self._after_delete_hooks)
        
        return result
    
    @classmethod
    async def delete_many(cls, conditions: Dict[str, Any], cascade: bool = False):
        """Delete multiple model instances matching the conditions."""
        if DBMS.Database is None:
            raise RuntimeError("Database not initialized")
        
        if cascade:
            # Delete related objects
            for name, field in cls._fields.items():
                if isinstance(field, RelationshipField) and field.cascade:
                    related = getattr(cls, name)
                    if isinstance(related, list):
                        for item in related:
                            await item.delete(cascade=True)
        result =  await DBMS.Database.delete_many(cls.table_name(), cls.normalise(conditions, 'params'))
        return result
    
    def json(self) -> dict:
        """Convert model to JSON, including relationships."""
        data = self.model_dump(exclude={'password'}, by_alias=True)
        if isinstance(data['created_at'], datetime):
            data['created_at'] = data['created_at'].isoformat()
        if isinstance(data['updated_at'], datetime):
            data['updated_at'] = data['updated_at'].isoformat()
        # Add relationships
        for name, field in self._fields.items():
            if isinstance(field, RelationshipField):
                related = getattr(self, name)
                if isinstance(related, list):
                    data[name] = [item.json() for item in related]
                elif related:
                    data[name] = related.json()
                else:
                    data[name] = None
        
        return data
    
    @classmethod
    def before_save(cls, func: Callable[[Any], Coroutine[Any, Any, None]]) -> Callable[[Any], Coroutine[Any, Any, None]]:
        """Decorator to register an async before_save hook."""
        cls._before_save_hooks.append(func)
        return func
    
    @classmethod
    def after_save(cls, func: Callable[[Any], Coroutine[Any, Any, None]]) -> Callable[[Any], Coroutine[Any, Any, None]]:
        """Decorator to register an async after_save hook."""
        cls._after_save_hooks.append(func)
        return func
    
    @classmethod
    def before_delete(cls, func: Callable[[Any], Coroutine[Any, Any, None]]) -> Callable[[Any], Coroutine[Any, Any, None]]:
        """Decorator to register an async before_delete hook."""
        cls._before_delete_hooks.append(func)
        return func
    
    @classmethod
    def after_delete(cls, func: Callable[[Any], Coroutine[Any, Any, None]]) -> Callable[[Any], Coroutine[Any, Any, None]]:
        """Decorator to register an async after_delete hook."""
        cls._after_delete_hooks.append(func)
        return func
    
    async def _run_hooks(self, hooks: List[Callable[[Any], Coroutine[Any, Any, None]]]) -> None:
        """Run a list of async hooks."""
        for hook in hooks:
            await hook(self)

    @classmethod
    async def get(cls, id: Union[str, int, PyObjectId]) -> Optional[Self]:
        """Get a model instance by ID asynchronously."""
        if DBMS.Database is None:
            raise RuntimeError("Database not initialized")
        
        result = cls.normalise(cast(Dict[str, Any], await DBMS.Database.find_one(cls.table_name(), cls.normalise({'id': id}, 'params'))))
        return cls(**result) if result else None
    
    @classmethod
    async def get_related(cls, instance_id: str, relationship: str):
        """Get related objects for a relationship asynchronously."""
        field = cls._fields.get(relationship)
        if not field or not isinstance(field, RelationshipField):
            raise ValueError(f"Invalid relationship: {relationship}")
        
        # Import related model
        from importlib import import_module
        module_path, model_name = field.model.rsplit('.', 1)
        module = import_module(module_path)
        related_model = getattr(module, model_name)
        
        instance = await cls.get(instance_id)
        if not instance:
            return None
        
        return getattr(instance, relationship)
    
    @classmethod
    async def insert(cls, data: Dict[str, Any]):
        """Insert a new model instance asynchronously."""
        if DBMS.Database is None:
            raise RuntimeError("Database not initialized")
        
        result = await DBMS.Database.insert_one(cls.table_name(), cls.normalise(data, 'params'))
        return result
    
    @classmethod
    async def insert_many(cls, data: List[Dict[str, Any]]) -> List[Self]:
        """Insert multiple model instances asynchronously."""
        if DBMS.Database is None:
            raise RuntimeError("Database not initialized")
        
        result = await DBMS.Database.insert_many(cls.table_name(), [cls.normalise(item, 'params') for item in data]) # type: ignore
        return [cls(**cls.normalise(item)) for item in result] # type: ignore

    @classmethod
    async def find_many(cls, conditions: Dict[str, Any] = {}) -> List[Self]:
        """Find model instances matching the conditions."""
        if DBMS.Database is None:
            raise RuntimeError("Database not initialized")
        
        results = await DBMS.Database.find(cls.table_name(), cls.normalise(conditions, 'params') if conditions else {})
        return [cls(**cls.normalise(result)) for result in results]
    
    @classmethod
    async def find_one(cls, conditions: Dict[str, Any] = {}) -> Optional[Self]:
        """Find one model instance matching the conditions."""
        if DBMS.Database is None:
            raise RuntimeError("Database not initialized")
        
        result = await DBMS.Database.find_one(cls.table_name(), cls.normalise(conditions, 'params') if conditions else None) #type: ignore
        return cls(**cls.normalise(result)) if result else None
    
    @classmethod
    async def find(cls, conditions: Dict[str, Any] = {}) -> List[Self]:
        """Find model instances matching the conditions."""
        return await cls.find_many(conditions)
    
    @classmethod
    async def all(cls) -> List[Self]:
        """Get all model instances."""
        if DBMS.Database is None:
            raise RuntimeError("Database not initialized")
        return await cls.find_many()
    
    @classmethod
    async def update_one(cls, conditions: Dict[str, Any], data: Dict[str, Any]) -> int:
        """Update model instance matching the conditions asynchronously.
        
        Args:
            conditions: Dictionary of conditions to match
            data: Dictionary of fields to update. Can include fields not defined in the model.
            
        Returns:
            Update Result
        """
        if DBMS.Database is None:
            raise RuntimeError("Database not initialized")
        
        data['updated_at'] = datetime.now()
        
        return await DBMS.Database.update_one(cls.table_name(), cls.normalise(conditions, 'params'), cls.normalise(data, 'params'))
    
    @classmethod
    async def update_many(cls, conditions: Dict[str, Any], data: Dict[str, Any]):
        """Update model instances matching the conditions asynchronously.
        
        Args:
            conditions: Dictionary of conditions to match
            data: Dictionary of fields to update. Can include fields not defined in the model.
            
        Returns:
            Update Result
        """
        if DBMS.Database is None:
            raise RuntimeError("Database not initialized")
        
        return await DBMS.Database.update_many(cls.table_name(), cls.normalise(conditions, 'params'), cls.normalise(data, 'params'))
    
    async def update(self, data: Dict[str, Any]):
        """Update model instances matching the conditions.
        
        Args:
            conditions: Dictionary of conditions to match
            data: Dictionary of fields to update. Can include fields not defined in the model.
            
        Returns:
            Number of records updated
        """
        return await self.update_one({'id': self.id}, data)

    async def remove(self, conditions: Dict[str, Any]):
        """Remove model instances matching the conditions asynchronously.
        
        Args:
            conditions: Dictionary of conditions to match
            
        Returns:
            Number of records removed
        """
        if DBMS.Database is None:
            raise RuntimeError("Database not initialized")
        
        # Normalize conditions
        normalized_conditions = self.normalise(conditions, 'params')
        
        # Remove records
        return await DBMS.Database.delete_one(self.table_name(), normalized_conditions)
    
    async def save(self) -> Self:
        '''Save the model instance to database.'''
        # Run before_save hooks
        await self._run_hooks(self._before_save_hooks)
        
        # Validate fields
        self.validate_fields()
        
        # Compute fields
        self.compute_fields()
        
        # Prepare data for save
        data = self.model_dump()
        
        # Handle relationships
        for name, field in self._fields.items():
            if isinstance(field, RelationshipField):
                if isinstance(field, (OneToMany, ManyToMany)):
                    data[f'{name}_ids'] = getattr(self, f'_{name}_ids', [])
                else:
                    data[f'{name}_id'] = getattr(self, f'_{name}_id')
                
                if name in data:
                    del data[name]
        
        # Check if this is a new record or existing one
        if DBMS.Database is None:
            raise RuntimeError("Database not initialized")
        if data.get('id'):
            # Update timestamps
            self.updated_at = datetime.now()
            data['updated_at'] = self.updated_at
            
            result = await DBMS.Database.update_one(
                self.table_name(),
                self.normalise({'id': data.get('id')}, 'params'),
                self.normalise(data, 'params')
            )
            # existing = await DBMS.Database.find_one(self.table_name(), self.normalise({'id': self.id}, 'params'))

        
        else:
            # This is a new record, perform insert
            result = await DBMS.Database.insert_one(self.table_name(), self.normalise(data, 'params'))
            # Update instance id if provided
            if result:
                if isinstance(DBMS.Database, MongoDB):
                    self.id = str(result)  # Convert ObjectId to string
                else:
                    self.id = result
        
        # Run after_save hooks
        await self._run_hooks(self._after_save_hooks)
        
        return self
