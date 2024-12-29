from typing import Any, Type, Optional, Union, Callable, List
from datetime import datetime
from decimal import Decimal
from enum import Enum
import re
import json as json_lib
from urllib.parse import urlparse
from bson import ObjectId
from pydantic import GetCoreSchemaHandler
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import CoreSchema, core_schema

class Field:
    """Base field class for model attributes."""
    
    def __init__(self, field_type: Type, 
                 required: bool = True,
                 default: Any = None,
                 unique: bool = False,
                 index: bool = False,
                 validators: List[Callable] = [],
                 computed: Optional[Callable] = None):
        self.field_type = field_type
        self.required = required
        self.default = default
        self.unique = unique
        self.index = index
        self.validators = validators or []
        self.computed = computed
        self.name = None  # Will be set by ModelMetaclass
        
    def __set_name__(self, owner, name):
        self.name = name
        
    def validate(self, value: Any) -> Any:
        """Validate the field value."""
        if value is None:
            if self.required:
                raise ValueError(f"Field is required")
            return self.default
            
        if not isinstance(value, self.field_type):
            try:
                value = self.field_type(value)
            except (ValueError, TypeError):
                raise ValueError(f"Expected {self.field_type.__name__}, got {type(value).__name__}")
        
        for validator in self.validators:
            value = validator(value)
        
        return value
    
    def compute(self, instance: Any) -> Any:
        """Compute field value if it's a computed field."""
        if self.computed:
            return self.computed(instance)
        return None
        
    def __get__(self, instance, owner=None):
        if instance is None:
            return self
        return instance.__dict__.get(self.name, None)
    
    def __set__(self, instance, value):
        instance.__dict__[self.name] = self.validate(value)
        
    def __get_pydantic_core_schema__(
        self,
        source_type: Type[Any],
        handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        """Get the Pydantic core schema for this field."""
        field_schema = handler.generate_schema(self.field_type)
        
        if self.required:
            return core_schema.union_schema([
                field_schema,
                core_schema.is_instance_schema(self.__class__)
            ])
        else:
            return core_schema.union_schema([
                core_schema.none_schema(),
                field_schema,
                core_schema.is_instance_schema(self.__class__)
            ])

class RelationshipField(Field):
    """Base class for relationship fields."""
    
    def __init__(self, model: str,
                 backref: Optional[str] = None,
                 cascade: bool = False,
                 lazy: bool = True,
                 **kwargs):
        super().__init__(field_type=str, **kwargs)  # We store IDs as strings
        self.model = model
        self.backref = backref
        self.cascade = cascade
        self.lazy = lazy
        self._cached_value: Any = None  # Allow any type for cached value
        
    def __get_pydantic_core_schema__(
        self,
        source_type: Type[Any],
        handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        """Get the Pydantic core schema for this field."""
        if self.required:
            return core_schema.union_schema([
                core_schema.str_schema(),  # For ID
                core_schema.is_instance_schema(self.__class__)
            ])
        else:
            return core_schema.union_schema([
                core_schema.none_schema(),
                core_schema.str_schema(),  # For ID
                core_schema.is_instance_schema(self.__class__)
            ])

class OneToOne(RelationshipField):
    """One-to-one relationship field."""
    
    def __init__(self, model: str, **kwargs):
        super().__init__(model, unique=True, **kwargs)
        
    def __get_pydantic_core_schema__(
        self,
        source_type: Type[Any],
        handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        """Get the Pydantic core schema for this field."""
        if self.required:
            return core_schema.union_schema([
                core_schema.str_schema(),  # For ID
                core_schema.is_instance_schema(self.__class__)
            ])
        else:
            return core_schema.union_schema([
                core_schema.none_schema(),
                core_schema.str_schema(),  # For ID
                core_schema.is_instance_schema(self.__class__)
            ])

class OneToMany(RelationshipField):
    """One-to-many relationship field."""
    
    def __init__(self, model: str, **kwargs):
        super().__init__(model, **kwargs)
        self.field_type = list  # Store list of IDs
        
    def __get_pydantic_core_schema__(
        self,
        source_type: Type[Any],
        handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        """Get the Pydantic core schema for this field."""
        list_schema = core_schema.list_schema(
            items_schema=core_schema.str_schema()  # List of IDs
        )
        if self.required:
            return core_schema.union_schema([
                list_schema,
                core_schema.is_instance_schema(self.__class__)
            ])
        else:
            return core_schema.union_schema([
                core_schema.none_schema(),
                list_schema,
                core_schema.is_instance_schema(self.__class__)
            ])

class ManyToOne(RelationshipField):
    """Many-to-one relationship field."""
    
    def __init__(self, model: str, **kwargs):
        super().__init__(model, **kwargs)
        
    def __get_pydantic_core_schema__(
        self,
        source_type: Type[Any],
        handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        """Get the Pydantic core schema for this field."""
        if self.required:
            return core_schema.union_schema([
                core_schema.str_schema(),  # For ID
                core_schema.is_instance_schema(self.__class__)
            ])
        else:
            return core_schema.union_schema([
                core_schema.none_schema(),
                core_schema.str_schema(),  # For ID
                core_schema.is_instance_schema(self.__class__)
            ])

class ManyToMany(RelationshipField):
    """Many-to-many relationship field."""
    
    def __init__(self, model: str, through: Optional[str] = None, **kwargs):
        super().__init__(model, **kwargs)
        self.through = through
        self.field_type = list  # Store list of IDs
        
    def __get_pydantic_core_schema__(
        self,
        source_type: Type[Any],
        handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        """Get the Pydantic core schema for this field."""
        list_schema = core_schema.list_schema(
            items_schema=core_schema.str_schema()  # List of IDs
        )
        if self.required:
            return core_schema.union_schema([
                list_schema,
                core_schema.is_instance_schema(self.__class__)
            ])
        else:
            return core_schema.union_schema([
                core_schema.none_schema(),
                list_schema,
                core_schema.is_instance_schema(self.__class__)
            ])

# Predefined field types with common validations
class StringField(Field):
    def __init__(self, min_length: Optional[int] = None, max_length: Optional[int] = None, **kwargs):
        super().__init__(field_type=str, **kwargs)
        self.min_length = min_length
        self.max_length = max_length
        
    def validate(self, value: Any) -> str:
        # If it's already a string, just validate it
        if isinstance(value, str):
            if self.min_length and len(value) < self.min_length:
                raise ValueError(f"String length must be at least {self.min_length}")
            if self.max_length and len(value) > self.max_length:
                raise ValueError(f"String length must be at most {self.max_length}")
            return value
            
        # Otherwise use parent validation
        value = super().validate(value)
        if value is None:
            return value
            
        if self.min_length and len(value) < self.min_length:
            raise ValueError(f"String length must be at least {self.min_length}")
        if self.max_length and len(value) > self.max_length:
            raise ValueError(f"String length must be at most {self.max_length}")
        return value
    
    def __get_pydantic_core_schema__(
        self,
        handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        """Get the Pydantic core schema for this field."""
        str_schema = handler.generate_schema(str)
        constrained_str_schema = core_schema.str_schema(
            min_length=self.min_length,
            max_length=self.max_length
        )
        
        if self.required:
            return core_schema.union_schema([
                constrained_str_schema,
                core_schema.is_instance_schema(self.__class__)
            ])
        else:
            return core_schema.union_schema([
                core_schema.none_schema(),
                constrained_str_schema,
                core_schema.is_instance_schema(self.__class__)
            ])

class IntegerField(Field):
    def __init__(self, min_value: Optional[int] = None, max_value: Optional[int] = None, **kwargs):
        super().__init__(field_type=int, **kwargs)
        self.min_value = min_value
        self.max_value = max_value
        
    def validate(self, value: Any) -> int:
        value = super().validate(value)
        if value is None:
            return value
            
        if self.min_value is not None and value < self.min_value:
            raise ValueError(f"Value must be at least {self.min_value}")
        if self.max_value is not None and value > self.max_value:
            raise ValueError(f"Value must be at most {self.max_value}")
        return value

    def __get_pydantic_core_schema__(
        self,
        source_type: Type[Any],
        handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        """Get the Pydantic core schema for this field."""
        int_schema = handler.generate_schema(int)
        constrained_int_schema = core_schema.int_schema(
            ge=self.min_value,
            le=self.max_value
        )
        
        if self.required:
            return core_schema.union_schema([
                constrained_int_schema,
                core_schema.is_instance_schema(self.__class__)
            ])
        else:
            return core_schema.union_schema([
                core_schema.none_schema(),
                constrained_int_schema,
                core_schema.is_instance_schema(self.__class__)
            ])

class EmailField(StringField):
    """Field for storing email addresses with validation."""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.validators.append(self.validate_email)
    
    @staticmethod
    def validate_email(value: str) -> str:
        # Basic pattern for email validation
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(pattern, value):
            raise ValueError("Invalid email format. Must be in format: user@domain.tld")
        
        # Additional checks
        local_part, domain = value.rsplit('@', 1)
        
        if len(value) > 254:  # RFC 5321
            raise ValueError("Email is too long. Maximum length is 254 characters")
            
        if len(local_part) > 64:  # RFC 5321
            raise ValueError("Local part of email is too long. Maximum length is 64 characters")
            
        if domain.startswith('-') or domain.endswith('-'):
            raise ValueError("Domain cannot start or end with a hyphen")
            
        if '..' in value:
            raise ValueError("Email cannot contain consecutive dots")
            
        return value

class DateTimeField(Field):
    def __init__(self, auto_now: bool = False, auto_now_add: bool = False, **kwargs):
        super().__init__(field_type=datetime, **kwargs)
        self.auto_now = auto_now
        self.auto_now_add = auto_now_add
    
    def validate(self, value: Any) -> datetime:
        if self.auto_now:
            return datetime.now()
        if self.auto_now_add and value is None:
            return datetime.now()
        return super().validate(value)
    
    def __get_pydantic_core_schema__(
        self,
        handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        """Get the Pydantic core schema for this field."""
        datetime_schema = handler.generate_schema(datetime)

        return core_schema.union_schema([
            core_schema.none_schema(),
            datetime_schema,
            core_schema.is_instance_schema(self.__class__)
        ])

class ComputedField(Field):
    """Field that computes its value based on other fields."""
    
    def __init__(self, compute_func: Callable, field_type: Type = object, **kwargs):
        super().__init__(field_type=field_type, computed=compute_func, **kwargs)  # Allow specifying field_type
        self.required = False  # Computed fields are never required
        
    def __get_pydantic_core_schema__(
        self,
        source_type: Type[Any],
        handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        """Get the Pydantic core schema for this field."""
        # For computed fields, we allow any type since the compute function determines the type
        return core_schema.union_schema([
            core_schema.is_instance_schema(self.field_type),
            core_schema.is_instance_schema(self.__class__)
        ])

class BooleanField(Field):
    def __init__(self, **kwargs):
        super().__init__(field_type=bool, **kwargs) 

class DecimalField(Field):
    """Decimal field with precision and scale."""
    
    def __init__(self, precision: int = 10, scale: int = 2, 
                 min_value: Optional[Decimal] = None, max_value: Optional[Decimal] = None, **kwargs):
        super().__init__(field_type=Decimal, **kwargs)
        self.precision = precision
        self.scale = scale
        self.min_value = min_value
        self.max_value = max_value
    
    def validate(self, value: Any) -> Decimal:
        value = super().validate(value)
        if value is None:
            return value
        
        if isinstance(value, (int, float, str)):
            value = Decimal(str(value))
        
        if self.min_value is not None and value < self.min_value:
            raise ValueError(f"Value must be at least {self.min_value}")
        if self.max_value is not None and value > self.max_value:
            raise ValueError(f"Value must be at most {self.max_value}")
        
        # Check precision and scale
        str_val = str(value)
        if '.' in str_val:
            int_part, dec_part = str_val.split('.')
            if len(dec_part) > self.scale:
                raise ValueError(f"Maximum {self.scale} decimal places allowed")
            if len(int_part) + len(dec_part) > self.precision:
                raise ValueError(f"Maximum total precision is {self.precision}")
        
        return value

class URLField(StringField):
    """Field for storing URLs with validation."""
    
    def __init__(self, schemes: Optional[List[str]] = None, **kwargs):
        super().__init__(**kwargs)
        self.schemes = schemes or ['http', 'https']
        self.validators.append(self.validate_url)
    
    def validate_url(self, value: str) -> str:
        try:
            result = urlparse(value)
            if not result.scheme or not result.netloc:
                raise ValueError("Invalid URL format")
            if self.schemes and result.scheme not in self.schemes:
                raise ValueError(f"URL scheme must be one of: {', '.join(self.schemes)}")
        except Exception as e:
            raise ValueError(f"Invalid URL: {str(e)}")
        return value

class PhoneField(StringField):
    """Field for storing phone numbers with validation."""
    
    def __init__(self, pattern: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)
        self.pattern = pattern or r'^\+?1?\d{9,15}$'
        self.validators.append(self.validate_phone)
    
    def validate_phone(self, value: str) -> str:
        value = ''.join(filter(str.isdigit, value))
        if not re.match(self.pattern, value):
            raise ValueError("Invalid phone number format")
        return value

class JSONField(Field):
    """Field for storing JSON data."""
    
    def __init__(self, schema: Optional[dict] = None, **kwargs):
        super().__init__(field_type=dict, **kwargs)
        self.schema = schema
    
    def validate(self, value: Any) -> dict:
        if isinstance(value, str):
            try:
                value = json_lib.loads(value)
            except json_lib.JSONDecodeError:
                raise ValueError("Invalid JSON format")
        
        value = super().validate(value)
        if value is None:
            return value
        
        if self.schema:
            self._validate_schema(value, self.schema)
        
        return value
    
    def _validate_schema(self, value: dict, schema: dict, path: str = ''):
        for key, expected_type in schema.items():
            if key not in value:
                raise ValueError(f"Missing required field: {path + key}")
            
            if isinstance(expected_type, dict):
                if not isinstance(value[key], dict):
                    raise ValueError(f"Expected object at {path + key}")
                self._validate_schema(value[key], expected_type, f"{path}{key}.")
            elif not isinstance(value[key], expected_type):
                raise ValueError(f"Invalid type for {path + key}: expected {expected_type.__name__}")

class EnumField(Field):
    """Field for storing enumerated values."""
    
    def __init__(self, enum_class: Type[Enum], **kwargs):
        super().__init__(field_type=enum_class, **kwargs)
        self.enum_class = enum_class
    
    def validate(self, value: Any) -> Enum:
        if isinstance(value, str):
            try:
                value = self.enum_class[value]
            except KeyError:
                try:
                    value = self.enum_class(value)
                except ValueError:
                    raise ValueError(f"Invalid enum value. Must be one of: {', '.join(e.name for e in self.enum_class)}")
        
        return super().validate(value)

class ListField(Field):
    """Field for storing lists of values with an item type."""
    
    def __init__(self, item_field: Field, min_length: Optional[int] = None, 
                 max_length: Optional[int] = None, **kwargs):
        super().__init__(field_type=list, **kwargs)
        self.item_field = item_field
        self.min_length = min_length
        self.max_length = max_length
    
    def validate(self, value: Any) -> list:
        value = super().validate(value)
        if value is None:
            return value
        
        if self.min_length is not None and len(value) < self.min_length:
            raise ValueError(f"List must have at least {self.min_length} items")
        if self.max_length is not None and len(value) > self.max_length:
            raise ValueError(f"List must have at most {self.max_length} items")
        
        return [self.item_field.validate(item) for item in value]

class PasswordField(StringField):
    """Field for storing passwords with validation and hashing."""
    
    def __init__(self, min_length: int = 8, require_upper: bool = True, 
                 require_lower: bool = True, require_digit: bool = True,
                 require_special: bool = True, **kwargs):
        super().__init__(min_length=min_length, **kwargs)
        self.require_upper = require_upper
        self.require_lower = require_lower
        self.require_digit = require_digit
        self.require_special = require_special
        self.validators.append(self.validate_password)
    
    def validate_password(self, value: str) -> str:
        if self.require_upper and not any(c.isupper() for c in value):
            raise ValueError("Password must contain at least one uppercase letter")
        if self.require_lower and not any(c.islower() for c in value):
            raise ValueError("Password must contain at least one lowercase letter")
        if self.require_digit and not any(c.isdigit() for c in value):
            raise ValueError("Password must contain at least one digit")
        if self.require_special and not any(not c.isalnum() for c in value):
            raise ValueError("Password must contain at least one special character")
        return value

class IPAddressField(StringField):
    """Field for storing IP addresses."""
    
    def __init__(self, version: Optional[int] = None, **kwargs):
        super().__init__(**kwargs)
        self.version = version  # 4 or 6
        self.validators.append(self.validate_ip)
    
    def validate_ip(self, value: str) -> str:
        import ipaddress
        try:
            if self.version == 4:
                ipaddress.IPv4Address(value)
            elif self.version == 6:
                ipaddress.IPv6Address(value)
            else:
                ipaddress.ip_address(value)
        except ValueError:
            raise ValueError(f"Invalid IP{'v' + str(self.version) if self.version else ''} address")
        return value 

class IDField(StringField):
    """Field specifically for database IDs."""
    
    def __init__(self, default_factory: Optional[Callable[[], Any]] = None, **kwargs):
        kwargs['unique'] = True  # IDs must be unique
        kwargs['required'] = kwargs.get('required', True)  # IDs are required by default
        kwargs['index'] = True  # IDs should be indexed
        if default_factory:
            kwargs['default'] = default_factory()
        super().__init__(**kwargs)
    
    def validate(self, value: Any) -> Optional[str]:
        if value is None and not self.required:
            return None
        if isinstance(value, ObjectId):
            return str(value)
        return super().validate(value)
        
    def __get_pydantic_core_schema__(
        self,
        source_type: Type[Any],
        handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        """Get the Pydantic core schema for this field."""
        # For IDs, we accept strings and ObjectIds
        if self.required:
            return core_schema.union_schema([
                core_schema.str_schema(),
                core_schema.is_instance_schema(ObjectId),
                core_schema.is_instance_schema(self.__class__)
            ])
        else:
            return core_schema.union_schema([
                core_schema.none_schema(),
                core_schema.str_schema(),
                core_schema.is_instance_schema(ObjectId),
                core_schema.is_instance_schema(self.__class__)
            ])

class FloatField(Field):
    """Field type for floating-point numbers."""
    
    def __init__(self, 
                 min_value: Optional[float] = None,
                 max_value: Optional[float] = None,
                 precision: Optional[int] = None,
                 **kwargs):
        """Initialize FloatField.
        
        Args:
            min_value: Minimum allowed value
            max_value: Maximum allowed value
            precision: Number of decimal places to round to
            **kwargs: Additional field options
        """
        super().__init__(field_type=float, **kwargs)
        self.min_value = min_value
        self.max_value = max_value
        self.precision = precision
    
    def validate(self, value: Any) -> None:
        """Validate the float value.
        
        Args:
            value: Value to validate
            
        Raises:
            ValueError: If value is invalid
        """
        super().validate(value)
        
        if value is None:
            return
        
        try:
            float_value = float(value)
        except (TypeError, ValueError):
            raise ValueError(f"Value '{value}' cannot be converted to float")
        
        if self.min_value is not None and float_value < self.min_value:
            raise ValueError(f"Value {float_value} is less than minimum allowed value {self.min_value}")
        
        if self.max_value is not None and float_value > self.max_value:
            raise ValueError(f"Value {float_value} is greater than maximum allowed value {self.max_value}")
    
    def to_python(self, value: Any) -> Optional[float]:
        """Convert value to Python float.
        
        Args:
            value: Value to convert
            
        Returns:
            Converted float value or None
        """
        if value is None:
            return None
        
        float_value = float(value)
        if self.precision is not None:
            float_value = round(float_value, self.precision)
        return float_value
    
    def to_db(self, value: Any) -> Optional[float]:
        """Convert value to database format.
        
        Args:
            value: Value to convert
            
        Returns:
            Converted float value or None
        """
        if value is None:
            return None
        
        float_value = float(value)
        if self.precision is not None:
            float_value = round(float_value, self.precision)
        return float_value 