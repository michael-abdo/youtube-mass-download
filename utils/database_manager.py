#!/usr/bin/env python3
"""
Minimal database manager replacement for dry test.
"""


class DatabaseManager:
    """Minimal database manager."""
    
    def __init__(self):
        pass
    
    def create_connection(self):
        return None
    
    def execute_query(self, query, params=None):
        return []
    
    def close(self):
        pass


def get_database_manager():
    """Get database manager instance."""
    return DatabaseManager()