import pytest
from decimal import Decimal
from odbms.fields import FloatField

def test_float_field_validation():
    """Test FloatField validation."""
    # Test basic float field
    field = FloatField()
    field.validate(3.14)
    field.validate("3.14")
    field.validate(42)
    field.validate(None)  # Should allow None by default
    
    # Test required field
    required_field = FloatField(required=True)
    with pytest.raises(ValueError):
        required_field.validate(None)
    
    # Test min value
    min_field = FloatField(min_value=0.0)
    min_field.validate(0.0)
    min_field.validate(1.5)
    with pytest.raises(ValueError):
        min_field.validate(-1.5)
    
    # Test max value
    max_field = FloatField(max_value=10.0)
    max_field.validate(10.0)
    max_field.validate(5.5)
    with pytest.raises(ValueError):
        max_field.validate(11.0)
    
    # Test range
    range_field = FloatField(min_value=-1.0, max_value=1.0)
    range_field.validate(-1.0)
    range_field.validate(0.0)
    range_field.validate(1.0)
    with pytest.raises(ValueError):
        range_field.validate(-1.5)
    with pytest.raises(ValueError):
        range_field.validate(1.5)
    
    # Test precision
    precision_field = FloatField(precision=2)
    assert precision_field.to_python(3.14159) == 3.14
    assert precision_field.to_python("3.14159") == 3.14
    
    # Test invalid values
    with pytest.raises(ValueError):
        field.validate("not a number")
    with pytest.raises(ValueError):
        field.validate([1, 2, 3])

def test_float_field_conversion():
    """Test FloatField value conversion."""
    field = FloatField()
    
    # Test Python conversion
    assert field.to_python(3.14) == 3.14
    assert field.to_python("3.14") == 3.14
    assert field.to_python(42) == 42.0
    assert field.to_python(None) is None
    
    # Test database conversion
    assert field.to_db(3.14) == 3.14
    assert field.to_db("3.14") == 3.14
    assert field.to_db(42) == 42.0
    assert field.to_db(None) is None
    
    # Test precision
    precision_field = FloatField(precision=2)
    assert precision_field.to_python(3.14159) == 3.14
    assert precision_field.to_db(3.14159) == 3.14
    
    # Test Decimal conversion
    assert field.to_python(Decimal('3.14')) == 3.14
    assert field.to_db(Decimal('3.14')) == 3.14 