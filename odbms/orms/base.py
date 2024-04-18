from typing import Any

class ORM:
    db: Any
    dbms: str

    def initialize(self, *args, **kwargs):
        pass

    def insert(self, table: str, data: dict):
        pass

    def insert_many(self, table: str, data: list[dict]):
        pass

    def find(self, table: str, filter: dict = {}, projection: list | dict = []):
        pass

    def find_one(self, table: str, filter: dict = {}, projection: list | dict = []):
        pass

    def remove(self, table: str, filter: dict):
        pass

    def delete(self, table: str, filter: dict):
        pass

    def update(self, table: str, filter: dict, data: dict):
        pass

    def update_many(self, table: str, filter: dict, data: dict):
        pass

    def count(self, table: str, filter: dict = {}):
        pass

    def sum(self, table: str, column: str, params: dict = {}):
        pass

    def execute(self, query: str):
        pass

    def import_from_file(self, filename: str):
        pass
    
    def command(self, query: str, table: str):
        pass