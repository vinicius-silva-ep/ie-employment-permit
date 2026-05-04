"""
Utility script to test database connection and initialize the database setup.
Run this first before attempting data loads.
"""

import sys
import os
from db_config import test_connection, create_all_tables

def main():
    """Initialize database setup."""
    print("\n" + "=" * 80)
    print("DATABASE INITIALIZATION")
    print("=" * 80 + "\n")
    
    # Test connection
    print("1. Testing database connection...")
    if not test_connection():
        print("\nERROR Failed to connect to database.")
        print("   Please check your .env file and credentials.")
        return False
    
    # Create tables
    print("\n2. Creating database tables...")
    try:
        create_all_tables()
        print("\nOK Database initialized successfully!")
        return True
    except Exception as e:
        print(f"\nERROR creating tables: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
