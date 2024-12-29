from datetime import datetime
from typing import Optional, Self, Union, Any, List, Dict, Type, ClassVar, cast, Callable, Coroutine, Annotated
import inspect
import json
import asyncio

from bson.objectid import ObjectId
import inflect
from pydantic import BaseModel, Field, ValidationError, field_serializer
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

class Model(BaseModel, metaclass=ModelMetaclass):
    '''Base model class with enhanced features'''
    
    model_config = {
        'arbitrary_types_allowed': True,
        'from_attributes': True
    }
    
    id: Annotated[Union[str, int, ObjectId], Field(default_factory=lambda: str(ObjectId()))]
    created_at: Annotated[datetime, Field(default_factory=datetime.now)]
    updated_at: Annotated[datetime, Field(default_factory=datetime.now)]
    
    # Class variables for table configuration
    __abstract__: ClassVar[bool] = False
    __table_name__: ClassVar[Optional[str]] = None
    _fields: ClassVar[Dict[str, ModelField]] = {}
    
    # Event hooks
    _before_save_hooks: ClassVar[List[Callable[[Any], None]]] = []
    _after_save_hooks: ClassVar[List[Callable[[Any], None]]] = []
    _before_delete_hooks: ClassVar[List[Callable[[Any], None]]] = []
    _after_delete_hooks: ClassVar[List[Callable[[Any], None]]] = []
    
    # Async event hooks
    _before_save_hooks_async: ClassVar[List[Callable[[Any], Coroutine[Any, Any, None]]]] = []
    _after_save_hooks_async: ClassVar[List[Callable[[Any], Coroutine[Any, Any, None]]]] = []
    _before_delete_hooks_async: ClassVar[List[Callable[[Any], Coroutine[Any, Any, None]]]] = []
    _after_delete_hooks_async: ClassVar[List[Callable[[Any], Coroutine[Any, Any, None]]]] = []
    
    @classmethod
    def before_save(cls, func: Callable[[Any], None]) -> Callable[[Any], None]:
        """Decorator to register a before_save hook."""
        cls._before_save_hooks.append(func)
        return func
    
    @classmethod
    def after_save(cls, func: Callable[[Any], None]) -> Callable[[Any], None]:
        """Decorator to register an after_save hook."""
        cls._after_save_hooks.append(func)
        return func
    
    @classmethod
    def before_delete(cls, func: Callable[[Any], None]) -> Callable[[Any], None]:
        """Decorator to register a before_delete hook."""
        cls._before_delete_hooks.append(func)
        return func
    
    @classmethod
    def after_delete(cls, func: Callable[[Any], None]) -> Callable[[Any], None]:
        """Decorator to register an after_delete hook."""
        cls._after_delete_hooks.append(func)
        return func
    
    def _run_hooks(self, hooks: List[Callable[[Any], None]]) -> None:
        """Run a list of hooks."""
        for hook in hooks:
            hook(self)
    
    def __init__(self, **data):
        # Initialize relationships
        for name, field in self._fields.items():
            if isinstance(field, RelationshipField):
                if isinstance(field, (OneToMany, ManyToMany)):
                    setattr(self, f'_{name}_ids', [])
                else:
                    setattr(self, f'_{name}_id', None)
        
        super().__init__(**data)
    
    def model_dump(self, exclude: Optional[set] = None) -> dict:
        """Convert model to dictionary, excluding relationship fields."""
        data = super().model_dump(exclude=exclude)
        
        # Remove relationship fields from data
        for name, field in self._fields.items():
            if isinstance(field, RelationshipField):
                if name in data:
                    del data[name]
                # Add relationship ID field
                if isinstance(field, (OneToMany, ManyToMany)):
                    data[f'{name}_ids'] = getattr(self, f'_{name}_ids', [])
                else:
                    data[f'{name}_id'] = getattr(self, f'_{name}_id')
        
        return data
    
    def save(self) -> Self:
        '''Save the model instance to database.'''
        # Run before_save hooks
        self._run_hooks(self._before_save_hooks)
        
        # Validate fields
        self.validate_fields()
        
        # Compute fields
        self.compute_fields()
        
        # Update timestamps
        now = datetime.now()
        if self.created_at is None:
            self.created_at = now
        self.updated_at = now
        
        # Prepare data for save
        data = self.model_dump(exclude={'id'})
        
        # Check if this is a new record or existing one
        existing = None
        if self.id and not isinstance(self.id, ObjectId):
            if DBMS.Database is None:
                raise RuntimeError("Database not initialized")
            existing = DBMS.Database.find_one(self.table_name(), self.normalise({'id': self.id}, 'params'))
        
        if not existing:
            # This is a new record, perform insert
            if DBMS.Database is None:
                raise RuntimeError("Database not initialized")
            result = DBMS.Database.insert(self.table_name(), self.normalise(data, 'params'))
            # Update instance id if provided by database
            if result and hasattr(result, 'inserted_id'):  # type: ignore
                self.id = str(result.inserted_id)  # type: ignore
            else:
                self.id = result
        else:
            # This is an existing record, perform update
            if DBMS.Database is None:
                raise RuntimeError("Database not initialized")
            result = DBMS.Database.update(
                self.table_name(),
                self.normalise({'id': self.id}, 'params'),
                self.normalise(data, 'params')
            )
        
        # Run after_save hooks
        self._run_hooks(self._after_save_hooks)
        
        return self
    
    @classmethod
    def table_name(cls) -> str:
        '''Get the table name for the model.'''
        if cls.__table_name__:
            return cls.__table_name__
        
        name = cls.__name__.lower()
        p = inflect.engine()
        return cast(str, p.plural(name)) #type: ignore
    
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
    
    @staticmethod
    def normalise(content: dict|None, optype: str = 'dbresult') -> dict:
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
                if '_id' in content:
                    content['id'] = str(content.pop('_id'))
                for key, value in content.items():
                    if isinstance(value, ObjectId):
                        content[key] = str(value)
                    elif isinstance(value, list):
                        content[key] = '::'.join(str(v) for v in value)
                    elif isinstance(value, datetime):
                        content[key] = value.isoformat()
                    elif isinstance(value, dict):
                        content[key] = json.dumps(value)
            else:
                for key, value in content.items():
                    if isinstance(value, str) and '::' in value:
                        content[key] = value.split('::')
                    elif key in ('created_at', 'updated_at') and value:
                        try:
                            content[key] = datetime.fromisoformat(value)
                        except (ValueError, TypeError):
                            pass
        return content
    
    @classmethod
    def get(cls, id: Union[str, int, ObjectId]) -> Optional[Self]:
        """Get a model instance by ID."""
        if DBMS.Database is None:
            raise RuntimeError("Database not initialized")
        result = cls.normalise(cast(Dict[str, Any], DBMS.Database.find_one(cls.table_name(), cls.normalise({'id': id}, 'params'))))
        return cls(**result) if result else None
    
    @classmethod
    def get_related(cls, instance_id: str, relationship: str):
        """Get related objects for a relationship."""
        field = cls._fields.get(relationship)
        if not field or not isinstance(field, RelationshipField):
            raise ValueError(f"Invalid relationship: {relationship}")
        
        # Import related model
        from importlib import import_module
        module_path, model_name = field.model.rsplit('.', 1)
        module = import_module(module_path)
        related_model = getattr(module, model_name)
        
        instance = cls.get(instance_id)
        if not instance:
            return None
        
        return getattr(instance, relationship)
    
    def delete(self, cascade: bool = False):
        """Delete the model instance and optionally related objects."""
        # Run before_delete hooks
        self._run_hooks(self._before_delete_hooks)
        
        if cascade:
            # Delete related objects
            for name, field in self._fields.items():
                if isinstance(field, RelationshipField) and field.cascade:
                    related = getattr(self, name)
                    if isinstance(related, list):
                        for item in related:
                            item.delete(cascade=True)
                    elif related:
                        related.delete(cascade=True)
        
        if DBMS.Database is None:
            raise RuntimeError("Database not initialized")
        result = DBMS.Database.remove(self.table_name(), self.normalise({'id': self.id}, 'params'))
        
        # Run after_delete hooks
        self._run_hooks(self._after_delete_hooks)
        
        return result
    
    def json(self) -> dict:
        """Convert model to JSON, including relationships."""
        data = self.model_dump(exclude={'password'})
        
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
    def before_save_async(cls, func: Callable[[Any], Coroutine[Any, Any, None]]) -> Callable[[Any], Coroutine[Any, Any, None]]:
        """Decorator to register an async before_save hook."""
        cls._before_save_hooks_async.append(func)
        return func
    
    @classmethod
    def after_save_async(cls, func: Callable[[Any], Coroutine[Any, Any, None]]) -> Callable[[Any], Coroutine[Any, Any, None]]:
        """Decorator to register an async after_save hook."""
        cls._after_save_hooks_async.append(func)
        return func
    
    @classmethod
    def before_delete_async(cls, func: Callable[[Any], Coroutine[Any, Any, None]]) -> Callable[[Any], Coroutine[Any, Any, None]]:
        """Decorator to register an async before_delete hook."""
        cls._before_delete_hooks_async.append(func)
        return func
    
    @classmethod
    def after_delete_async(cls, func: Callable[[Any], Coroutine[Any, Any, None]]) -> Callable[[Any], Coroutine[Any, Any, None]]:
        """Decorator to register an async after_delete hook."""
        cls._after_delete_hooks_async.append(func)
        return func
    
    async def _run_hooks_async(self, hooks: List[Callable[[Any], Coroutine[Any, Any, None]]]) -> None:
        """Run a list of async hooks."""
        for hook in hooks:
            await hook(self)
    
    async def save_async(self) -> Self:
        '''Save the model instance to database asynchronously.'''
        # Run async before_save hooks
        await self._run_hooks_async(self._before_save_hooks_async)
        
        # Run sync before_save hooks in a thread pool
        if self._before_save_hooks:
            await asyncio.get_event_loop().run_in_executor(None, self._run_hooks, self._before_save_hooks)
        
        # Validate fields
        self.validate_fields()
        
        # Compute fields
        self.compute_fields()
        
        # Update timestamps
        self.updated_at = datetime.now()
        
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
        existing = None
        if self.id and not isinstance(self.id, ObjectId):
            if DBMS.Database is None:
                raise RuntimeError("Database not initialized")
            existing = await DBMS.Database.find_one_async(self.table_name(), self.normalise({'id': self.id}, 'params'))
        
        if not existing:
            # This is a new record, perform insert
            if DBMS.Database is None:
                raise RuntimeError("Database not initialized")
            result = await DBMS.Database.insert_async(self.table_name(), self.normalise(data, 'params'))
            # Update instance id if provided by database
            if result:
                self.id = str(result.inserted_id) if hasattr(result, 'inserted_id') else result  # type: ignore
                    
        else:
            # This is an existing record, perform update
            if DBMS.Database is None:
                raise RuntimeError("Database not initialized")
            result = await DBMS.Database.update_async(
                self.table_name(),
                self.normalise({'id': self.id}, 'params'),
                self.normalise(data, 'params')
            )
        
        # Run async after_save hooks
        await self._run_hooks_async(self._after_save_hooks_async)
        
        # Run sync after_save hooks in a thread pool
        if self._after_save_hooks:
            await asyncio.get_event_loop().run_in_executor(None, self._run_hooks, self._after_save_hooks)
        
        return self
    
    @classmethod
    async def get_async(cls, id: Union[str, int, ObjectId]) -> Optional[Self]:
        """Get a model instance by ID asynchronously."""
        if DBMS.Database is None:
            raise RuntimeError("Database not initialized")
        
        result = cls.normalise(cast(Dict[str, Any], await DBMS.Database.find_one_async(cls.table_name(), cls.normalise({'id': id}, 'params'))))
        return cls(**result) if result else None
    
    @classmethod
    async def get_related_async(cls, instance_id: str, relationship: str):
        """Get related objects for a relationship asynchronously."""
        field = cls._fields.get(relationship)
        if not field or not isinstance(field, RelationshipField):
            raise ValueError(f"Invalid relationship: {relationship}")
        
        # Import related model
        from importlib import import_module
        module_path, model_name = field.model.rsplit('.', 1)
        module = import_module(module_path)
        related_model = getattr(module, model_name)
        
        instance = await cls.get_async(instance_id)
        if not instance:
            return None
        
        return getattr(instance, relationship)
    
    async def delete_async(self, cascade: bool = False):
        """Delete the model instance and optionally related objects asynchronously."""
        # Run async before_delete hooks
        await self._run_hooks_async(self._before_delete_hooks_async)
        
        # Run sync before_delete hooks in a thread pool
        if self._before_delete_hooks:
            await asyncio.get_event_loop().run_in_executor(None, self._run_hooks, self._before_delete_hooks)
        
        if cascade:
            # Delete related objects
            for name, field in self._fields.items():
                if isinstance(field, RelationshipField) and field.cascade:
                    related = getattr(self, name)
                    if isinstance(related, list):
                        await asyncio.gather(*[item.delete_async(cascade=True) for item in related])
                    elif related:
                        await related.delete_async(cascade=True)
        
        if DBMS.Database is None:
            raise RuntimeError("Database not initialized")
        result = await DBMS.Database.remove_async(self.table_name(), self.normalise({'id': self.id}, 'params'))
        
        # Run async after_delete hooks
        await self._run_hooks_async(self._after_delete_hooks_async)
        
        # Run sync after_delete hooks in a thread pool
        if self._after_delete_hooks:
            await asyncio.get_event_loop().run_in_executor(None, self._run_hooks, self._after_delete_hooks)
        
        return result
    
    @classmethod
    def find(cls, conditions: Optional[dict] = None) -> List[Self]:
        """Find model instances matching the conditions."""
        if DBMS.Database is None:
            raise RuntimeError("Database not initialized")
        
        results = DBMS.Database.find(cls.table_name(), cls.normalise(conditions, 'params') if conditions else None)
        return [cls(**cls.normalise(result)) for result in results]
    
    @classmethod
    def all(cls) -> List[Self]:
        """Get all model instances."""
        return cls.find()
    
    @classmethod
    async def find_async(cls, conditions: Optional[dict] = None) -> List[Self]:
        """Find model instances matching the conditions asynchronously."""
        if DBMS.Database is None:
            raise RuntimeError("Database not initialized")
        
        results = await DBMS.Database.find_async(cls.table_name(), cls.normalise(conditions, 'params') if conditions else None)
        return [cls(**cls.normalise(result)) for result in results]
    
    @classmethod
    async def all_async(cls) -> List[Self]:
        """Get all model instances asynchronously."""
        return await cls.find_async()
