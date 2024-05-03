import logging
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection, cursor as Cursor
from typing import Union, List, Dict

from .base import ORM

class PostgresqlDB(ORM):
    db: connection
    cursor: Cursor
    dbms = 'postgresql'

    @staticmethod
    def initialize(host, port, user, password, database):
        try:
            # Connect to the PostgreSQL server without specifying a database
            conn = psycopg2.connect(
                host=host,
                port=port,
                user=user,
                password=password
            )
            conn.autocommit = True
            cursor = conn.cursor()

            # Check if the database exists
            cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{database}'")
            exists = cursor.fetchone()

            if not exists:
                # If the database doesn't exist, create it
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
                logging.info(f"Database '{database}' created successfully.")

            cursor.close()
            conn.close()
            
            # Connect to the database
            PostgresqlDB.db = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password
            )
            PostgresqlDB.db.set_session(autocommit=False)
            PostgresqlDB.cursor = PostgresqlDB.db.cursor()
            
        except (Exception, psycopg2.Error) as error:
            logging.error("Error connecting to PostgreSQL database:", error)
    
    @staticmethod
    def execute(query, params=None):
        try:
            if params:
                PostgresqlDB.cursor.execute(query, params)
            else:
                PostgresqlDB.cursor.execute(query)
                if PostgresqlDB.cursor.description:  # Check if description is not None
                    column_names = [desc.name for desc in PostgresqlDB.cursor.description]
                    result = [dict(zip(column_names, row)) for row in PostgresqlDB.cursor.fetchall()]
                    PostgresqlDB.db.commit()
                    return result
                else:
                    PostgresqlDB.db.commit()
                    return []  # Return an empty list or appropriate value for queries that do not return rows
        except (Exception, psycopg2.Error) as error:
            logging.exception(error)
            PostgresqlDB.db.rollback()
            return []

    @staticmethod
    def insert(table: str, data: Dict):
        columns = sql.SQL(', ').join(sql.Identifier(col) for col in data.keys())
        values = ', '.join(['%s'] * len(data))
        sql_query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) RETURNING id").format(
            sql.Identifier(table),
            columns,
            sql.SQL(values)
        )
        try:
            PostgresqlDB.cursor.execute(sql_query, list(data.values()))
            PostgresqlDB.db.commit()
            new_inserts = PostgresqlDB.cursor.fetchone()
            insert_id =   new_inserts[0] if new_inserts else 0
            return str(insert_id)
        except (Exception, psycopg2.Error) as error:
            print("Error inserting data:", error)
            PostgresqlDB.db.rollback()
            return 0

    @staticmethod
    def insert_many(table: str, data: List[Dict]):
        columns = sql.SQL(', ').join(sql.Identifier(col) for col in data[0].keys())
        values_template = ', '.join(['%s'] * len(data[0]))
        values = ', '.join(['(' + values_template + ')'] * len(data))
        sql_query = sql.SQL("INSERT INTO {} ({}) VALUES {}").format(
            sql.Identifier(table),
            columns,
            sql.SQL(values)
        )
        try:
            PostgresqlDB.cursor.execute(sql_query, [value for row in data for value in row.values()])
            PostgresqlDB.db.commit()
            return True
        except (Exception, psycopg2.Error) as error:
            print("Error inserting data:", error)
            PostgresqlDB.db.rollback()
            return False

    @staticmethod
    def find(table: str, filter: Dict = {}, projection: Union[List, Dict] = []):
        columns = sql.SQL(', ').join(map(sql.Identifier, projection)) if projection else sql.SQL('*')
        conditions = ' AND '.join(f"{key} = %s" for key in filter.keys())
        sql_query = sql.SQL("SELECT {} FROM {}").format(
            columns,
            sql.Identifier(table),
        )
        if filter:
            sql_query = sql.SQL("{} WHERE {}").format(
                sql_query,
                sql.SQL(conditions)
            )
        try:
            PostgresqlDB.cursor.execute(sql_query, list(filter.values()))
            column_names = [desc.name for desc in PostgresqlDB.cursor.description] # type: ignore
            result = [dict(zip(column_names, row)) for row in PostgresqlDB.cursor.fetchall()]
            PostgresqlDB.db.commit()
            return result
        except (Exception, psycopg2.Error) as error:
            logging.error({'status': 'Error', 'message': str(error)})
            PostgresqlDB.db.rollback()
            return []

    @staticmethod
    def find_one(table: str, filter: Dict = {}, projection: Union[List, Dict] = []):
        columns = sql.SQL(', ').join(map(sql.Identifier, projection)) if projection else sql.SQL('*')
        conditions = ' AND '.join(f"{key} = %s" for key in filter.keys())
        sql_query = sql.SQL("SELECT {} FROM {} WHERE {} LIMIT 1").format(
            columns,
            sql.Identifier(table),
            sql.SQL(conditions)
        )
        try:
            PostgresqlDB.cursor.execute(sql_query, list(filter.values()))
            column_names = [desc.name for desc in PostgresqlDB.cursor.description] # type: ignore
            result = dict(zip(column_names, PostgresqlDB.cursor.fetchone())) if PostgresqlDB.cursor.rowcount > 0 else None # type: ignore
            PostgresqlDB.db.commit()
            
            return result
        except (Exception, psycopg2.Error) as error:
            print("Error executing SQL:", error)
            PostgresqlDB.db.rollback()
            return {}

    @staticmethod
    def remove(table: str, filter: Dict):
        conditions = ' AND '.join(f"{key} = %s" for key in filter.keys())
        sql_query = sql.SQL("DELETE FROM {} WHERE {}").format(
            sql.Identifier(table),
            sql.SQL(conditions)
        )
        try:
            PostgresqlDB.cursor.execute(sql_query, list(filter.values()))
            PostgresqlDB.db.commit()
            return True
        except (Exception, psycopg2.Error) as error:
            print("Error removing data:", error)
            PostgresqlDB.db.rollback()
            return False

    @staticmethod
    def update(table: str, filter: Dict, data: Dict):
        set_clause = ', '.join(f"{key} = %s" for key in data.keys())
        conditions = ' AND '.join(f"{key} = %s" for key in filter.keys())
        sql_query = sql.SQL("UPDATE {} SET {} WHERE {}").format(
            sql.Identifier(table),
            sql.SQL(set_clause),
            sql.SQL(conditions)
        )
        try:
            PostgresqlDB.cursor.execute(sql_query, list(data.values()) + list(filter.values()))
            PostgresqlDB.db.commit()
            return True
        except (Exception, psycopg2.Error) as error:
            print("Error updating data:", error)
            PostgresqlDB.db.rollback()
            return False

    @staticmethod
    def count(table: str, filter: Dict = {}):
        conditions = ' AND '.join(f"{key} = %s" for key in filter.keys())
        sql_query = sql.SQL("SELECT COUNT(*) FROM {} WHERE {}").format(
            sql.Identifier(table),
            sql.SQL(conditions)
        )
        try:
            PostgresqlDB.cursor.execute(sql_query, list(filter.values()))
            result = PostgresqlDB.cursor.fetchone()
            result = result[0] if result else None
            PostgresqlDB.db.commit()
            return result
        except (Exception, psycopg2.Error) as error:
            print("Error executing SQL:", error)
            PostgresqlDB.db.rollback()

    @staticmethod
    def sum(table: str, column: str, params: Dict = {}):
        conditions = ' AND '.join(f"{sql.Identifier(key)} = %s" for key in params.keys())
        sql_query = sql.SQL("SELECT SUM({}) FROM {} WHERE {}").format(
            sql.Identifier(column),
            sql.Identifier(table),
            sql.SQL(conditions)
        )
        try:
            PostgresqlDB.cursor.execute(sql_query, list(params.values()))
            result = PostgresqlDB.cursor.fetchone()
            result = result[0] if result else None
            PostgresqlDB.db.commit()
            return result
        except (Exception, psycopg2.Error) as error:
            print("Error executing SQL:", error)
            PostgresqlDB.db.rollback()