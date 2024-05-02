from typing import List, Dict, Tuple, Optional, Any, Callable, Union
from validators import validate_email

class Field(type):
    def __init__(self, value: Optional[str] = None, is_unique: bool = False, validators: List[Callable] = []) -> None:
        if value is not None:
            self.value = value
        else:
            self.value = ''
        self.is_unique = is_unique
        self.validators = validators
    
    def validate(self):
        for validator in self.validators:
            validator(self.value)
    
    def __instancecheck__(self, instance):
        return hasattr(instance, 'value') and hasattr(instance, 'is_unique') and hasattr(instance, 'validators')
    
    def __new__(cls, value=None):
        if isinstance(value, str):
            return cls(value)
        else:
            return value


class Email(Field):
    def __init__(self, value: Optional[str] = None, is_unique: bool = False, validators: List[Callable] = []) -> None:
        super().__init__(value, is_unique, validators)
        self.is_unique = is_unique
        self.validators = validators
        self.validators.insert(0, validate_email)
        self.validate()


class User:
    def __init__(self, name: Field, email: Email, password: str) -> None:
        self.name = name
        self.email = email
        self.password = password

if __name__ == "__main__":
    new_user = User('Amos Amissah', 'theonlyamos', 'password')
    print(type(new_user.email))
