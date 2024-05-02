import re

def validate_email(email): 
  pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+.[A-Z|a-z]{2,}\b'
  match =  re.fullmatch(pattern, email)
  
  if not match:
      raise TypeError('Provide a valid email address')
  
  return True

# print(validate_email("a._example@gmail.com.com")) # Output: True
# print(validate_email("invalid_email")) # Output: False