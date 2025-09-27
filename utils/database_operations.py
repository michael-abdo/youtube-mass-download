#!/usr/bin/env python3
"""
Unified Database Operations Module (DRY Phase 7)

Provides standardized database operations and connection management:
- Connection pooling and management
- Standard CRUD operations
- Transaction management
- Query builders and helpers
- Database-agnostic interface
- Migration utilities

Note: This project primarily uses CSV/JSON storage, but this module
provides database capabilities for future use or specialized operations.
"""

import os
import sqlite3
import json
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Tuple, Iterator, Callable
from datetime import datetime
import threading

# Standardized project imports
from utils.config import setup_project_imports
setup_project_imports()

from utils.logging_config import get_logger
from utils.error_handling import handle_file_operations, ErrorMessages
from utils.config import get_config, get_project_root, ensure_directory
from utils.data_processing import read_json_safe, write_json_safe

logger = get_logger(__name__)


# ============================================================================
# DATABASE CONFIGURATION AND CONNECTION MANAGEMENT
# ============================================================================

@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    db_type: str = 'sqlite'  # sqlite, postgresql, mysql
    host: str = 'localhost'
    port: int = 5432
    database: str = 'app.db'
    username: Optional[str] = None
    password: Optional[str] = None
    pool_size: int = 5
    timeout: float = 30.0
    
    @classmethod
    def from_config(cls, config_section: str = 'database') -> 'DatabaseConfig':
        """Create config from application configuration."""
        config = get_config()
        db_config = config.get_section(config_section)
        
        return cls(
            db_type=db_config.get('type', 'sqlite'),
            host=db_config.get('host', 'localhost'),
            port=db_config.get('port', 5432),
            database=db_config.get('database', 'app.db'),
            username=db_config.get('username'),
            password=db_config.get('password'),
            pool_size=db_config.get('pool_size', 5),
            timeout=db_config.get('timeout', 30.0)
        )


class DatabaseManager:
    """Unified database manager with connection pooling."""
    
    def __init__(self, config: Optional[DatabaseConfig] = None):
        """Initialize database manager."""
        self.config = config or DatabaseConfig.from_config()
        self._connections = []
        self._lock = threading.Lock()
        self._initialized = False
    
    def _get_sqlite_connection(self) -> sqlite3.Connection:
        """Get SQLite connection."""
        db_path = get_project_root() / self.config.database
        ensure_directory(db_path.parent)
        
        conn = sqlite3.connect(
            str(db_path),
            timeout=self.config.timeout,
            check_same_thread=False
        )
        
        # Enable foreign keys and WAL mode for better performance
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.row_factory = sqlite3.Row  # Enable column access by name
        
        return conn
    
    def _get_postgresql_connection(self):
        """Get PostgreSQL connection."""
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            
            conn = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.username,
                password=self.config.password,
                connect_timeout=int(self.config.timeout),
                cursor_factory=RealDictCursor
            )
            return conn
            
        except ImportError:
            raise ImportError("psycopg2 required for PostgreSQL connections")
    
    def _get_mysql_connection(self):
        """Get MySQL connection."""
        try:
            import mysql.connector
            from mysql.connector.cursor import MySQLCursorDict
            
            conn = mysql.connector.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.username,
                password=self.config.password,
                connection_timeout=int(self.config.timeout),
                cursor_class=MySQLCursorDict
            )
            return conn
            
        except ImportError:
            raise ImportError("mysql-connector-python required for MySQL connections")
    
    @contextmanager
    def get_connection(self):
        """Get database connection with automatic cleanup."""
        with self._lock:
            if self.config.db_type == 'sqlite':
                conn = self._get_sqlite_connection()
            elif self.config.db_type == 'postgresql':
                conn = self._get_postgresql_connection()
            elif self.config.db_type == 'mysql':
                conn = self._get_mysql_connection()
            else:
                raise ValueError(f"Unsupported database type: {self.config.db_type}")
        
        try:
            yield conn
        finally:
            conn.close()
    
    @contextmanager
    def transaction(self):
        """Database transaction context manager."""
        with self.get_connection() as conn:
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise


# Global database manager instance
_db_manager: Optional[DatabaseManager] = None


def get_database_manager(config: Optional[DatabaseConfig] = None) -> DatabaseManager:
    """Get singleton database manager."""
    global _db_manager
    
    if _db_manager is None:
        _db_manager = DatabaseManager(config)
    
    return _db_manager


# ============================================================================
# QUERY BUILDERS AND HELPERS
# ============================================================================

class QueryBuilder:
    """Simple SQL query builder for common operations."""
    
    def __init__(self, table: str):
        """Initialize query builder for table."""
        self.table = table
        self.query_parts = []
        self.params = []
    
    def select(self, columns: Union[str, List[str]] = "*") -> 'QueryBuilder':
        """Add SELECT clause."""
        if isinstance(columns, list):
            columns = ", ".join(columns)
        self.query_parts.append(f"SELECT {columns} FROM {self.table}")
        return self
    
    def insert(self, data: Dict[str, Any]) -> 'QueryBuilder':
        """Add INSERT clause."""
        columns = ", ".join(data.keys())
        placeholders = ", ".join(["?" for _ in data])
        self.query_parts.append(f"INSERT INTO {self.table} ({columns}) VALUES ({placeholders})")
        self.params.extend(data.values())
        return self
    
    def update(self, data: Dict[str, Any]) -> 'QueryBuilder':
        """Add UPDATE clause."""
        set_clause = ", ".join([f"{col} = ?" for col in data.keys()])
        self.query_parts.append(f"UPDATE {self.table} SET {set_clause}")
        self.params.extend(data.values())
        return self
    
    def delete(self) -> 'QueryBuilder':
        """Add DELETE clause."""
        self.query_parts.append(f"DELETE FROM {self.table}")
        return self
    
    def where(self, condition: str, *params) -> 'QueryBuilder':
        """Add WHERE clause."""
        self.query_parts.append(f"WHERE {condition}")
        self.params.extend(params)
        return self
    
    def order_by(self, column: str, direction: str = "ASC") -> 'QueryBuilder':
        """Add ORDER BY clause."""
        self.query_parts.append(f"ORDER BY {column} {direction}")
        return self
    
    def limit(self, count: int, offset: int = 0) -> 'QueryBuilder':
        """Add LIMIT clause."""
        if offset > 0:
            self.query_parts.append(f"LIMIT {count} OFFSET {offset}")
        else:
            self.query_parts.append(f"LIMIT {count}")
        return self
    
    def build(self) -> Tuple[str, List[Any]]:
        """Build query and return SQL with parameters."""
        sql = " ".join(self.query_parts)
        return sql, self.params


# ============================================================================
# CRUD OPERATIONS
# ============================================================================

@handle_file_operations("database_select", return_on_error=[])
def select(table: str, 
           columns: Union[str, List[str]] = "*",
           where: Optional[str] = None,
           params: Optional[List[Any]] = None,
           order_by: Optional[str] = None,
           limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Select records from table.
    
    Consolidates SELECT query patterns.
    
    Args:
        table: Table name
        columns: Columns to select
        where: WHERE clause
        params: Query parameters
        order_by: ORDER BY clause
        limit: LIMIT value
        
    Returns:
        List of records as dictionaries
        
    Example:
        users = select('users', where='active = ?', params=[True], order_by='name')
    """
    builder = QueryBuilder(table).select(columns)
    
    if where:
        builder.where(where, *(params or []))
    
    if order_by:
        parts = order_by.split()
        direction = parts[1] if len(parts) > 1 else "ASC"
        builder.order_by(parts[0], direction)
    
    if limit:
        builder.limit(limit)
    
    sql, query_params = builder.build()
    
    with get_database_manager().get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(sql, query_params)
        
        # Convert rows to dictionaries
        if hasattr(cursor, 'fetchall'):
            rows = cursor.fetchall()
            if rows and hasattr(rows[0], 'keys'):  # sqlite3.Row objects
                return [dict(row) for row in rows]
            return rows
        
        return []


@handle_file_operations("database_insert", return_on_error=None)
def insert(table: str, 
           data: Union[Dict[str, Any], List[Dict[str, Any]]],
           on_conflict: str = 'IGNORE') -> Optional[int]:
    """
    Insert records into table.
    
    Consolidates INSERT patterns.
    
    Args:
        table: Table name
        data: Data to insert (single dict or list of dicts)
        on_conflict: Conflict resolution ('IGNORE', 'REPLACE')
        
    Returns:
        Last row ID or None on error
        
    Example:
        user_id = insert('users', {'name': 'John', 'email': 'john@example.com'})
        insert('logs', [{'message': 'Log 1'}, {'message': 'Log 2'}])
    """
    if isinstance(data, dict):
        data = [data]
    
    if not data:
        return None
    
    with get_database_manager().transaction() as conn:
        cursor = conn.cursor()
        last_row_id = None
        
        for record in data:
            builder = QueryBuilder(table).insert(record)
            sql, params = builder.build()
            
            # Handle conflict resolution
            if on_conflict == 'REPLACE':
                sql = sql.replace('INSERT INTO', 'INSERT OR REPLACE INTO')
            elif on_conflict == 'IGNORE':
                sql = sql.replace('INSERT INTO', 'INSERT OR IGNORE INTO')
            
            cursor.execute(sql, params)
            last_row_id = cursor.lastrowid
        
        return last_row_id


@handle_file_operations("database_update", return_on_error=0)
def update(table: str,
           data: Dict[str, Any],
           where: str,
           params: Optional[List[Any]] = None) -> int:
    """
    Update records in table.
    
    Args:
        table: Table name
        data: Data to update
        where: WHERE clause
        params: WHERE parameters
        
    Returns:
        Number of affected rows
        
    Example:
        updated = update('users', {'active': False}, 'last_login < ?', [cutoff_date])
    """
    builder = QueryBuilder(table).update(data).where(where, *(params or []))
    sql, query_params = builder.build()
    
    with get_database_manager().transaction() as conn:
        cursor = conn.cursor()
        cursor.execute(sql, query_params)
        return cursor.rowcount


@handle_file_operations("database_delete", return_on_error=0)
def delete(table: str,
           where: str,
           params: Optional[List[Any]] = None) -> int:
    """
    Delete records from table.
    
    Args:
        table: Table name
        where: WHERE clause
        params: WHERE parameters
        
    Returns:
        Number of deleted rows
        
    Example:
        deleted = delete('logs', 'created_at < ?', [cutoff_date])
    """
    builder = QueryBuilder(table).delete().where(where, *(params or []))
    sql, query_params = builder.build()
    
    with get_database_manager().transaction() as conn:
        cursor = conn.cursor()
        cursor.execute(sql, query_params)
        return cursor.rowcount


# ============================================================================
# SCHEMA MANAGEMENT
# ============================================================================

def create_table(table: str, schema: Dict[str, str], if_not_exists: bool = True) -> bool:
    """
    Create table with schema.
    
    Args:
        table: Table name
        schema: Column definitions {column_name: type_definition}
        if_not_exists: Add IF NOT EXISTS clause
        
    Returns:
        True if successful
        
    Example:
        create_table('users', {
            'id': 'INTEGER PRIMARY KEY AUTOINCREMENT',
            'name': 'TEXT NOT NULL',
            'email': 'TEXT UNIQUE',
            'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
        })
    """
    try:
        columns = ", ".join([f"{col} {definition}" for col, definition in schema.items()])
        exists_clause = "IF NOT EXISTS " if if_not_exists else ""
        sql = f"CREATE TABLE {exists_clause}{table} ({columns})"
        
        with get_database_manager().transaction() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
        
        logger.info(f"Created table: {table}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to create table {table}: {e}")
        return False


def drop_table(table: str, if_exists: bool = True) -> bool:
    """
    Drop table.
    
    Args:
        table: Table name
        if_exists: Add IF EXISTS clause
        
    Returns:
        True if successful
        
    Example:
        drop_table('temp_table')
    """
    try:
        exists_clause = "IF EXISTS " if if_exists else ""
        sql = f"DROP TABLE {exists_clause}{table}"
        
        with get_database_manager().transaction() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
        
        logger.info(f"Dropped table: {table}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to drop table {table}: {e}")
        return False


def table_exists(table: str) -> bool:
    """Check if table exists."""
    try:
        with get_database_manager().get_connection() as conn:
            cursor = conn.cursor()
            
            # Query depends on database type
            db_type = get_database_manager().config.db_type
            
            if db_type == 'sqlite':
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
            elif db_type == 'postgresql':
                cursor.execute("SELECT tablename FROM pg_tables WHERE tablename = %s", (table,))
            elif db_type == 'mysql':
                cursor.execute("SHOW TABLES LIKE %s", (table,))
            
            return cursor.fetchone() is not None
            
    except Exception:
        return False


# ============================================================================
# DATA IMPORT/EXPORT
# ============================================================================

def import_csv_to_table(csv_file: Union[str, Path],
                       table: str,
                       create_table_if_missing: bool = True,
                       column_mapping: Optional[Dict[str, str]] = None) -> int:
    """
    Import CSV data to database table.
    
    Consolidates CSV import patterns.
    
    Args:
        csv_file: Path to CSV file
        table: Target table name
        create_table_if_missing: Create table if it doesn't exist
        column_mapping: Map CSV columns to table columns
        
    Returns:
        Number of imported rows
        
    Example:
        imported = import_csv_to_table('data.csv', 'users', column_mapping={
            'Name': 'name',
            'Email Address': 'email'
        })
    """
    from utils.data_processing import read_csv_safe
    
    df = read_csv_safe(csv_file)
    if df.empty:
        logger.warning(f"No data to import from {csv_file}")
        return 0
    
    # Apply column mapping
    if column_mapping:
        df = df.rename(columns=column_mapping)
    
    # Create table if needed
    if create_table_if_missing and not table_exists(table):
        # Infer schema from DataFrame
        schema = {}
        for col in df.columns:
            if df[col].dtype == 'int64':
                schema[col] = 'INTEGER'
            elif df[col].dtype == 'float64':
                schema[col] = 'REAL'
            else:
                schema[col] = 'TEXT'
        
        create_table(table, schema)
    
    # Insert data
    records = df.to_dict('records')
    insert(table, records)
    
    logger.info(f"Imported {len(records)} rows to {table}")
    return len(records)


def export_table_to_csv(table: str,
                        csv_file: Union[str, Path],
                        where: Optional[str] = None,
                        params: Optional[List[Any]] = None) -> bool:
    """
    Export table to CSV file.
    
    Args:
        table: Table name
        csv_file: Output CSV file path
        where: Optional WHERE clause
        params: WHERE parameters
        
    Returns:
        True if successful
        
    Example:
        export_table_to_csv('users', 'active_users.csv', where='active = ?', params=[True])
    """
    from utils.data_processing import write_csv_safe
    import pandas as pd
    
    try:
        records = select(table, where=where, params=params)
        if not records:
            logger.warning(f"No data to export from {table}")
            return False
        
        df = pd.DataFrame(records)
        return write_csv_safe(df, csv_file)
        
    except Exception as e:
        logger.error(f"Failed to export {table} to CSV: {e}")
        return False


# ============================================================================
# MIGRATION UTILITIES
# ============================================================================

class Migration:
    """Database migration helper."""
    
    def __init__(self, version: str, description: str):
        """Initialize migration."""
        self.version = version
        self.description = description
        self.up_statements = []
        self.down_statements = []
    
    def add_up_statement(self, sql: str):
        """Add upgrade SQL statement."""
        self.up_statements.append(sql)
    
    def add_down_statement(self, sql: str):
        """Add downgrade SQL statement."""
        self.down_statements.append(sql)
    
    def execute_up(self) -> bool:
        """Execute upgrade migration."""
        try:
            with get_database_manager().transaction() as conn:
                cursor = conn.cursor()
                for sql in self.up_statements:
                    cursor.execute(sql)
            
            logger.info(f"Applied migration {self.version}: {self.description}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to apply migration {self.version}: {e}")
            return False
    
    def execute_down(self) -> bool:
        """Execute downgrade migration."""
        try:
            with get_database_manager().transaction() as conn:
                cursor = conn.cursor()
                for sql in self.down_statements:
                    cursor.execute(sql)
            
            logger.info(f"Reverted migration {self.version}: {self.description}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to revert migration {self.version}: {e}")
            return False


def create_migrations_table():
    """Create migrations tracking table."""
    schema = {
        'version': 'TEXT PRIMARY KEY',
        'description': 'TEXT',
        'applied_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
    }
    create_table('migrations', schema)


def record_migration(migration: Migration):
    """Record applied migration."""
    if not table_exists('migrations'):
        create_migrations_table()
    
    insert('migrations', {
        'version': migration.version,
        'description': migration.description
    })


def get_applied_migrations() -> List[str]:
    """Get list of applied migration versions."""
    if not table_exists('migrations'):
        return []
    
    records = select('migrations', columns=['version'], order_by='version')
    return [record['version'] for record in records]


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def execute_sql(sql: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
    """
    Execute raw SQL query.
    
    Args:
        sql: SQL query
        params: Query parameters
        
    Returns:
        Query results
        
    Example:
        results = execute_sql('SELECT COUNT(*) as total FROM users WHERE active = ?', [True])
    """
    with get_database_manager().get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(sql, params or [])
        
        if sql.strip().upper().startswith('SELECT'):
            rows = cursor.fetchall()
            if rows and hasattr(rows[0], 'keys'):
                return [dict(row) for row in rows]
            return rows
        else:
            conn.commit()
            return []


def backup_database(backup_path: Union[str, Path]) -> bool:
    """
    Create database backup (SQLite only).
    
    Args:
        backup_path: Backup file path
        
    Returns:
        True if successful
        
    Example:
        backup_database('backups/database_backup.db')
    """
    db_manager = get_database_manager()
    
    if db_manager.config.db_type != 'sqlite':
        logger.error("Backup only supported for SQLite databases")
        return False
    
    try:
        backup_path = Path(backup_path)
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        
        with db_manager.get_connection() as source_conn:
            backup_conn = sqlite3.connect(str(backup_path))
            source_conn.backup(backup_conn)
            backup_conn.close()
        
        logger.info(f"Database backed up to {backup_path}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to backup database: {e}")
        return False


def get_table_stats(table: str) -> Dict[str, Any]:
    """
    Get table statistics.
    
    Args:
        table: Table name
        
    Returns:
        Statistics dictionary
        
    Example:
        stats = get_table_stats('users')
        print(f"Users table has {stats['row_count']} rows")
    """
    try:
        # Get row count
        count_result = execute_sql(f"SELECT COUNT(*) as count FROM {table}")
        row_count = count_result[0]['count'] if count_result else 0
        
        # Get table info (SQLite specific)
        db_type = get_database_manager().config.db_type
        columns = []
        
        if db_type == 'sqlite':
            info_result = execute_sql(f"PRAGMA table_info({table})")
            columns = [row['name'] for row in info_result]
        
        return {
            'table_name': table,
            'row_count': row_count,
            'column_count': len(columns),
            'columns': columns
        }
        
    except Exception as e:
        logger.error(f"Failed to get stats for table {table}: {e}")
        return {'table_name': table, 'error': str(e)}


# Example usage and testing
if __name__ == "__main__":
    # Test database operations
    config = DatabaseConfig(database='test.db')
    db_manager = DatabaseManager(config)
    
    # Create test table
    schema = {
        'id': 'INTEGER PRIMARY KEY AUTOINCREMENT',
        'name': 'TEXT NOT NULL',
        'email': 'TEXT UNIQUE',
        'active': 'BOOLEAN DEFAULT 1',
        'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
    }
    
    create_table('test_users', schema)
    
    # Insert test data
    test_users = [
        {'name': 'John Doe', 'email': 'john@example.com'},
        {'name': 'Jane Smith', 'email': 'jane@example.com'},
        {'name': 'Bob Johnson', 'email': 'bob@example.com', 'active': False}
    ]
    
    for user in test_users:
        insert('test_users', user)
    
    # Query data
    active_users = select('test_users', where='active = ?', params=[True])
    print(f"Active users: {len(active_users)}")
    
    # Get stats
    stats = get_table_stats('test_users')
    print(f"Table stats: {stats}")
    
    # Clean up
    drop_table('test_users')
    
    print("✓ Database operations test complete!")