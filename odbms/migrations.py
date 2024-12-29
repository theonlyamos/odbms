from datetime import datetime
import os
import json
from typing import Dict, Any, List, Optional
from .dbms import DBMS

class Migration:
    """Base class for database migrations."""
    
    def __init__(self, version: str, description: str):
        self.version = version
        self.description = description
        self.created_at = datetime.now()
    
    def up(self):
        """Implement the migration."""
        raise NotImplementedError
    
    def down(self):
        """Implement the rollback."""
        raise NotImplementedError

class MigrationManager:
    """Manages database migrations."""
    
    def __init__(self, migrations_dir: str = "migrations"):
        self.migrations_dir = migrations_dir
        self._ensure_migrations_table()
        self._ensure_migrations_directory()
    
    def _ensure_migrations_directory(self):
        """Ensure migrations directory exists."""
        if not os.path.exists(self.migrations_dir):
            os.makedirs(self.migrations_dir)
    
    def _ensure_migrations_table(self):
        """Ensure migrations table exists in the database."""
        if DBMS.Database.dbms != 'mongodb':
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS migrations (
                version VARCHAR(255) PRIMARY KEY,
                description TEXT,
                created_at TIMESTAMP,
                executed_at TIMESTAMP
            );
            """
            DBMS.Database.execute(create_table_sql)
        # else:
        #     # For MongoDB, we'll create a collection if it doesn't exist
        #     DBMS.Database.find('migrations', {}).limit(1)
    
    def create_migration(self, description: str) -> str:
        """
        Create a new migration file.
        
        @param description: Description of the migration
        @return: Path to the created migration file
        """
        version = datetime.now().strftime('%Y%m%d%H%M%S')
        filename = f"{version}_{description.lower().replace(' ', '_')}.json"
        filepath = os.path.join(self.migrations_dir, filename)
        
        migration_template = {
            "version": version,
            "description": description,
            "up": {
                "operations": []
            },
            "down": {
                "operations": []
            }
        }
        
        with open(filepath, 'w') as f:
            json.dump(migration_template, f, indent=2)
        
        return filepath
    
    def _get_applied_migrations(self) -> List[str]:
        """Get list of applied migration versions."""
        if DBMS.Database.dbms != 'mongodb':
            results = DBMS.Database.execute("SELECT version FROM migrations ORDER BY version")
            return [row['version'] for row in results]
        else:
            results = DBMS.Database.find('migrations', {}, ['version'])
            return [doc.get('version') for doc in results if doc.get('version')]
    
    def _apply_migration(self, migration_data: Dict[str, Any], direction: str = 'up'):
        """Apply a single migration."""
        operations = migration_data[direction]['operations']
        
        for operation in operations:
            op_type = operation['type']
            if op_type == 'create_table':
                if DBMS.Database.dbms != 'mongodb':
                    columns = [f"{col['name']} {col['type']}" for col in operation['columns']]
                    sql = f"CREATE TABLE {operation['table']} ({', '.join(columns)})"
                    DBMS.Database.execute(sql)
            
            elif op_type == 'add_column':
                if DBMS.Database.dbms != 'mongodb':
                    sql = f"ALTER TABLE {operation['table']} ADD COLUMN {operation['column']} {operation['type']}"
                    DBMS.Database.execute(sql)
            
            elif op_type == 'drop_table':
                if DBMS.Database.dbms != 'mongodb':
                    sql = f"DROP TABLE IF EXISTS {operation['table']}"
                    DBMS.Database.execute(sql)
                else:
                    DBMS.Database.command('drop', operation['table'])
            
            elif op_type == 'custom_sql':
                if DBMS.Database.dbms != 'mongodb':
                    DBMS.Database.execute(operation['sql'])
            
            elif op_type == 'mongodb_command':
                if DBMS.Database.dbms == 'mongodb':
                    DBMS.Database.command(operation['command'], operation.get('options', {}))
    
    def migrate(self, target_version: Optional[str] = None):
        """
        Run migrations up to target_version.
        If target_version is None, run all pending migrations.
        """
        applied = set(self._get_applied_migrations())
        
        # Get all migration files
        migration_files = []
        for filename in os.listdir(self.migrations_dir):
            if filename.endswith('.json'):
                with open(os.path.join(self.migrations_dir, filename)) as f:
                    migration_data = json.load(f)
                    migration_files.append((migration_data['version'], migration_data))
        
        # Sort by version
        migration_files.sort(key=lambda x: x[0])
        
        if target_version is not None:
            # If rolling back
            if target_version in applied:
                for version, migration_data in reversed(migration_files):
                    if version in applied and version > target_version:
                        self._apply_migration(migration_data, 'down')
                        if DBMS.Database.dbms != 'mongodb':
                            DBMS.Database.execute("DELETE FROM migrations WHERE version = %s", (version,))
                        else:
                            DBMS.Database.remove('migrations', {'version': version})
        else:
            # Apply pending migrations
            for version, migration_data in migration_files:
                if version not in applied:
                    self._apply_migration(migration_data, 'up')
                    
                    migration_record = {
                        'version': version,
                        'description': migration_data['description'],
                        'created_at': migration_data.get('created_at', datetime.now()),
                        'executed_at': datetime.now()
                    }
                    
                    if DBMS.Database.dbms != 'mongodb':
                        DBMS.Database.execute(
                            "INSERT INTO migrations (version, description, created_at, executed_at) VALUES (%s, %s, %s, %s)",
                            (migration_record['version'], migration_record['description'],
                             migration_record['created_at'], migration_record['executed_at'])
                        )
                    else:
                        DBMS.Database.insert('migrations', migration_record)
    
    def rollback(self, steps: int = 1):
        """
        Rollback the specified number of migrations.
        
        @param steps: Number of migrations to roll back
        """
        applied = self._get_applied_migrations()
        if not applied:
            return
        
        target_version = applied[-steps-1] if steps < len(applied) else ''
        self.migrate(target_version) 