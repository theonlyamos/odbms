from datetime import datetime
from typing import Optional, Union, get_type_hints, Any
import inspect
import json

from bson.objectid import ObjectId
from .dbms import DBMS

class Model():
    '''A model class'''
    TABLE_NAME: str = ''
    SELECTED_COLUMNS = []
    WHERE_CLAUSE = []
    GROUP_BY: str = ''
    ORDER_BY = ()
    LIMIT = 0

    def __init__(self, created_at: Optional[str] = None, updated_at: Optional[str] = None, id: Optional[str] = None):
        self.created_at = (datetime.now()).strftime("%a %b %d %Y %H:%M:%S") \
            if not created_at else created_at
        self.updated_at = (datetime.now()).strftime("%a %b %d %Y %H:%M:%S") \
            if not updated_at else updated_at
        self.id = ObjectId() if not id else str(id)

    
    @classmethod
    def create_table(cls):
        """
        Create the database table for the model (Only for relational databases).
        """
        if DBMS.Database.dbms != 'mongodb':
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
            # Get the __init__ method signature
            init_signatures = inspect.signature(cls.__init__)
            
            init_parameters = [param for param in init_signatures.parameters.values()
                            if param.name != 'self']
            
            all_parameters = init_parameters
            
            for param in all_parameters:
                param_name = param.name
                param_type = param.annotation
                
                column_type = cls.get_column_type(param_type)
                
                if param_name not in excluded:
                    column_def = f"{param_name} {column_type}"
                    columns.append(column_def)
                        
            columns += additional_columns.get(DBMS.Database.dbms, [])
            columns_str = ', '.join(columns)
            table_definition = f"CREATE TABLE IF NOT EXISTS {cls.TABLE_NAME} ({columns_str});"
            
            DBMS.Database.execute(table_definition)
            
            if DBMS.Database.dbms == 'sqlite':
                DBMS.Database.execute(f'''CREATE TRIGGER IF NOT EXISTS update_{cls.TABLE_NAME}_timestamp
                AFTER UPDATE ON {cls.TABLE_NAME}
                BEGIN
                    UPDATE {cls.TABLE_NAME} SET updated_at = STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW') WHERE id = NEW.id;
                END;''')
            elif DBMS.Database.dbms == 'postgresql':
                DBMS.Database.execute(f"""CREATE OR REPLACE FUNCTION update_{cls.TABLE_NAME}_timestamp()
                RETURNS TRIGGER AS $$
                BEGIN
                    NEW.updated_at := CURRENT_TIMESTAMP;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;""")
                DBMS.Database.execute(f"""CREATE TRIGGER update_{cls.TABLE_NAME}_timestamp
                BEFORE UPDATE ON {cls.TABLE_NAME}
                FOR EACH ROW
                EXECUTE PROCEDURE update_{cls.TABLE_NAME}_timestamp();""")
    
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
    def alter_table(cls, changes: dict):
        """
        Alter the table structure by adding, modifying, or dropping columns.

        @param changes: A dictionary mapping column names to their new data types.
        """
        if DBMS.Database.dbms != 'mongodb':
            # Fetch existing columns from the database
            fetch_columns_sql = f"SELECT column_name FROM information_schema.columns WHERE table_name='{cls.TABLE_NAME}';"
            existing_columns = {row['column_name'] for row in DBMS.Database.execute(fetch_columns_sql)} # type: ignore
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
                alter_sql = f"ALTER TABLE {cls.TABLE_NAME} {statement};"
                DBMS.Database.execute(alter_sql)

    def save(self):
        '''
        Instance Method for saving Model instance to database

        @params None
        @return None
        '''

        data = self.__dict__.copy()
        
        if DBMS.Database.dbms != 'mongodb':
            data['updated_at'] = (datetime.now()).strftime("%a %b %d %Y %H:%M:%S")
            del data["created_at"]
            del data["updated_at"]
            
        if isinstance(self.id, ObjectId):
            return DBMS.Database.insert(self.TABLE_NAME, self.normalise(data, 'params'))
        
        # Update the existing record in database
        data.pop('id', None)
        return DBMS.Database.update(self.TABLE_NAME, self.normalise({'id': self.id}, 'params'), self.normalise(data, 'params'))

    @staticmethod
    def insert(document):
        '''
        Static Method for saving documents into database

        @param documents Data to be saved
        @return Mongodb InsertManyResult
        '''

        data = {}
        
        return DBMS.Database.insert(Model.TABLE_NAME, document)
        
        
    
    @staticmethod
    def insert_many(documents):
        '''
        Static Method for saving documents into database

        @param documents Data to be saved
        @return Mongodb InsertManyResult
        '''

        data = {}
        
        return DBMS.Database.insert_many(Model.TABLE_NAME, documents)
    
    @classmethod
    def update(cls, query: dict ={}, update: dict = {}):
        '''
        Class Method for updating model in database

        @param update Content to be update in dictionary format
        @return None
        '''
        if 'id' in update.keys():
            del update['id']

        if DBMS.Database.dbms == 'mongodb':
            update['updated_at'] = (datetime.now()).strftime("%a %b %d %Y %H:%M:%S")
        
        return DBMS.Database.update(cls.TABLE_NAME, cls.normalise(query, 'params'), cls.normalise(update, 'params'))
    
    @classmethod
    def remove(cls, query: dict):
        '''
        Class Method for deleting document in database

        @param query filter parameters
        @return None
        '''

        return DBMS.Database.remove(cls.TABLE_NAME, cls.normalise(query, 'params'))
    
    @classmethod
    def count(cls, query: dict = {})-> int|None:
        '''
        Class Method for counting documents in collection

        @params query Filter dictionary
        @return int Count of Projects
        '''

        return DBMS.Database.count(cls.TABLE_NAME, cls.normalise(query, 'params'))

    # @classmethod
    # def get(cls, id = None):
    #     '''
    #     Class Method for retrieving function(s) by _id 
    #     or all if _id is None

    #     @param _id ID of the function in database
    #     @return Function instance(s)
    #     '''

    #     if id is None:
    #         return [cls(**cls.normalise(elem)) for elem in DBMS.Database.find(cls.TABLE_NAME)]

    #     model = DBMS.Database.find_one(cls.TABLE_NAME, cls.normalise({'id': id}, 'params'))
    #     print(model)
    #     return cls(**cls.normalise(model)) if model else None
    
    @classmethod
    def sum(cls, column: str)->int:
        '''
        Class method for retrieving sum of\n
        of specified column in table

        @params None
        @return int Sum of column
        '''
        return DBMS.Database.sum(cls.TABLE_NAME, column) # type: ignore

    @classmethod
    def get(cls, id: str):
        '''
        Class Method for retrieving \n
        model data from database

        @param _id ID of Model
        @return Model instance(s)
        '''

        result = cls.normalise(DBMS.Database.find_one(cls.TABLE_NAME, cls.normalise({'id': id}, 'params'))) # type: ignore

        return cls(**result) if len(result.keys()) else None
    
        
        # query = 'SELECT '
        # if cls.SELECTED_COLUMNS:
        #     query += cls.SELECTED_COLUMNS if type(cls.SELECTED_COLUMNS) is str \
        #         else ', '.join(cls.SELECTED_COLUMNS)
        # else:
        #     query += "*"
        
        # query += f" FROM {cls.TABLE_NAME}"

        # if len(cls.WHERE_CLAUSE):
        #     query += " WHERE"
        #     for clause in cls.WHERE_CLAUSE:
        #         if type(clause) is str:
        #             query += f" ({clause}) AND"
        #         elif type(clause) is dict:
        #             query += ' ('
        #             for key, value in clause.items():
        #                 query += f"{key}='{value}' AND "
        #             query = query.rstrip('AND ').strip()
        #             query += ') AND'
        
        # query = query.rstrip('AND').strip()
        # if cls.GROUP_BY:
        #     query += f" GROUP BY {cls.GROUP_BY}"
        
        # if len(cls.ORDER_BY):
        #     query += f" ORDER BY {cls.ORDER_BY[0]} {cls.ORDER_BY[1]}" # type: ignore
        
        # if cls.LIMIT:
        #     query += f" LIMIT {cls.LIMIT}"
        
        # cls.clear()
        
        # return DBMS.Database.query(query)
    
    @classmethod
    def all(cls)->list:
        '''
        Class Method for retrieving all \n
        model data from database

        @params None
        @return List[Model] instance(s)
        '''
        data = []
        results = DBMS.Database.find(cls.TABLE_NAME, {})

        for elem in results:
            if isinstance(elem, dict):
                data.append(cls(**cls.normalise(elem)))
            else:
                data.append(cls(*elem))

        return data
        
    @classmethod
    def find(cls, params: dict, projection: Union[list,dict] = [])-> list:
        '''
        Class Method for retrieving models
        by provided parameters

        @param params
        @return List[Model]
        '''
        
        data = []
        results = DBMS.Database.find(cls.TABLE_NAME, cls.normalise(params, 'params'), projection) # type: ignore

        for elem in results:
            if isinstance(elem, dict):
                data.append(cls(**cls.normalise(elem)))
            else:
                data.append(cls(*elem))

        return data
    
    @classmethod
    def find_one(cls, params: dict, projection: Union[list,dict] = []):
        '''
        Class Method for retrieving one model
        imstance by provided parameters

        @param params
        @return List[Model]
        '''
        if isinstance(projection, list):
            if len(projection) and 'id' not in projection:
                projection.append('id')  

        result = cls.normalise(DBMS.Database.find_one(cls.TABLE_NAME, cls.normalise(params, 'params'), projection)) # type: ignore

        return cls(**result) if len(result.keys()) else None
    
    @classmethod
    def query(cls, column: str, search: str):
        '''
        Class Method for retrieving products
        by their names

        @param name
        @return Product Instance
        '''
        if DBMS.Database.dbms != 'mongodb':
            sql = f"SELECT * from {cls.TABLE_NAME} WHERE "
            sql += f"{column} LIKE '%{search}%'"
            
            return [cls(**cls.normalise(elem)) for elem in DBMS.Database.query(sql) if elem] # type: ignore
        
        return None
    
    @classmethod
    def clear(cls):
        '''
        Clear all Class settings
        '''
        cls.SELECTED_COLUMNS = []
        cls.WHERE_CLAUSE = []
        cls.GROUP_BY = ''
        cls.ORDER_BY = ()
        cls.LIMIT = 0
    
    @classmethod
    def select(cls, columns: str|list):
        '''
        Class Method for retrieving model \n
        grouped by specified column

        @param column Column Name to group by
        @return Class
        '''

        cls.SELECTED_COLUMNS = columns
        return cls
    
    @classmethod
    def where(cls, clause: str|list):
        '''
        Class Method for retrieving model \n
        grouped by specified column

        @param column Column Name to group by
        @return Class
        '''

        cls.WHERE_CLAUSE.append(clause)
        return cls
    
    @classmethod
    def group_by(cls, column: str):
        '''
        Class Method for retrieving model \n
        grouped by specified column

        @param column Column Name to group by
        @return Class
        '''

        cls.GROUP_BY = column
        return cls
    
    @classmethod
    def order_by(cls, column: str, order: str = 'ASC'):
        '''
        Class Method for retrieving model \n
        ordered by specified column

        @param column Column Name to group by
        @return Class
        '''

        cls.ORDER_BY = (column, order.upper())
        return cls
    
    @classmethod
    def limit(cls, count: int = 0, offset: int = 0):
        '''
        Class Method for retrieving model \n
        ordered by specified column

        @param column Column Name to group by
        @return Class
        '''

        cls.LIMIT = f"{count}"
        if offset:
            cls.LIMIT += f", {offset}"
        return cls
    
    def json(self)-> dict:
        '''
        Instance Method for converting Model Instance to Dict

        @paramas None
        @return dict() format of Function instance
        '''
        
        data = self.__dict__.copy()
        
        if isinstance(data['created_at'], datetime):
            data['created_at'] = data['created_at'].strftime("%a %b %d %Y %H:%M:%S")
            
        if isinstance(data['updated_at'], datetime):
            data['updated_at'] = data['updated_at'].strftime("%a %b %d %Y %H:%M:%S")

        data.pop('password', None)

        return data
    
    @classmethod
    def normalise(cls, content: dict|None, optype: str = 'dbresult')-> dict:
        '''
        Static method of normalising database results\n
        Converts _id from mongodb to id

        @param optype str type of operation: dbresult or params
        @param content Dict|List[Dict] Database result
        @return Dict|List[List] of normalized content
        '''
        
        if content is None:
            return {}

        normalized = {}
        if DBMS.Database.dbms == 'mongodb':
            if optype == 'dbresult':
                elem = dict(content)
                elem['id'] = str(elem['_id'])
                del elem['_id']
                for key in elem.keys():
                    if key.endswith('_id'):
                        elem[key] = str(elem[key])
                normalized =  elem
                
            else:
                if 'id' in content.keys():
                    content['_id'] = ObjectId(content['id'])
                    del content['id']
                for key in content.keys():
                    if key.endswith('_id'):
                        content[key] = ObjectId(content[key])
                for key, value in content.items():
                    if type(value) == list:
                        content[key] = '::'.join([str(v) for v in value])
                normalized = content
            return normalized
        else:
            if optype == 'params':
                if '_id' in content.keys():
                    content['id'] = str(content['_id'])
                    del content['_id']
                for key, value in content.items():
                    if isinstance(value, ObjectId):
                        content[key] = str(value)
                    elif type(value) == list:
                        content[key] = '::'.join([str(v) for v in value])
                    elif type(value) == datetime:
                        content[key] = value.strftime("%a %b %d %Y %H:%M:%S")
                    elif type(value) == dict:
                        content[key] = json.dumps(value) # type: ignore
                
            else:
                init_signatures = inspect.signature(cls.__init__)
            
                init_parameters = [param for param in init_signatures.parameters.values()
                                if param.name != 'self']
                
                all_parameters = init_parameters
                type_mapping = [str, int, float, bool, list, dict]
                for param in all_parameters:
                    param_name = param.name
                    param_type = param.annotation
                    
                    if param_type in type_mapping:
                        if param_type is dict:
                            content[param_name] = json.loads(content[param_name])
                        elif param_type is list:
                            content[param_name] = content[param_name].split('::')
                        else:
                            content[param_name] = param_type(content[param_name])
            
        return content
