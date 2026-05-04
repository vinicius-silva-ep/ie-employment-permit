"""
Database configuration and utilities for IE Employment Permits project.
Handles connection pooling, table creation, and data loading to PostgreSQL.
"""

import os
import psycopg2
from psycopg2 import pool, sql
from dotenv import load_dotenv
from contextlib import contextmanager

# Load environment variables from .env file
load_dotenv()

# Database configuration from .env
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
}

SCHEMA = os.getenv('DB_SCHEMA', 'ie_emp_permits')

# Connection pool
_connection_pool = None

def get_connection_pool():
    """Get or create a connection pool."""
    global _connection_pool
    if _connection_pool is None:
        _connection_pool = psycopg2.pool.SimpleConnectionPool(
            1, 5,
            **DB_CONFIG
        )
    return _connection_pool

@contextmanager
def get_db_connection():
    """Context manager for database connections."""
    pool = get_connection_pool()
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        pool.putconn(conn)

def create_schema():
    """Create schema if it doesn't exist."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(SCHEMA)
            ))
            print(f"OK Schema '{SCHEMA}' created or already exists")

def create_companies_table():
    """Create companies table."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {schema}.companies (
                    id SERIAL PRIMARY KEY,
                    year INTEGER NOT NULL,
                    company VARCHAR(255) NOT NULL,
                    total FLOAT,
                    month VARCHAR(10),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT uk_companies_year_month UNIQUE (year, company, month)
                );
                CREATE INDEX IF NOT EXISTS idx_companies_year ON {schema}.companies(year);
                CREATE INDEX IF NOT EXISTS idx_companies_company ON {schema}.companies(company);
            """).format(schema=sql.Identifier(SCHEMA)))
            print("OK Table 'companies' created or already exists")

def create_county_table():
    """Create county table."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {schema}.county (
                    id SERIAL PRIMARY KEY,
                    year INTEGER NOT NULL,
                    county VARCHAR(100) NOT NULL,
                    issued FLOAT,
                    refused FLOAT,
                    withdrawn FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT uk_county_year UNIQUE (year, county)
                );
                CREATE INDEX IF NOT EXISTS idx_county_year ON {schema}.county(year);
                CREATE INDEX IF NOT EXISTS idx_county_name ON {schema}.county(county);
            """).format(schema=sql.Identifier(SCHEMA)))
            print("OK Table 'county' created or already exists")

def create_nationality_table():
    """Create nationality table."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {schema}.nationality (
                    id SERIAL PRIMARY KEY,
                    year INTEGER NOT NULL,
                    country VARCHAR(100) NOT NULL,
                    issued FLOAT,
                    refused FLOAT,
                    withdrawn FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT uk_nationality_year UNIQUE (year, country)
                );
                CREATE INDEX IF NOT EXISTS idx_nationality_year ON {schema}.nationality(year);
                CREATE INDEX IF NOT EXISTS idx_nationality_country ON {schema}.nationality(country);
            """).format(schema=sql.Identifier(SCHEMA)))
            print("OK Table 'nationality' created or already exists")

def create_sector_table():
    """Create sector table."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {schema}.sector (
                    id SERIAL PRIMARY KEY,
                    year INTEGER NOT NULL,
                    month VARCHAR(10),
                    sector VARCHAR(100) NOT NULL,
                    issued FLOAT,
                    refused FLOAT,
                    withdrawn FLOAT,
                    prefix VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT uk_sector_year_month UNIQUE (year, month, sector)
                );
                CREATE INDEX IF NOT EXISTS idx_sector_year ON {schema}.sector(year);
                CREATE INDEX IF NOT EXISTS idx_sector_name ON {schema}.sector(sector);
                CREATE INDEX IF NOT EXISTS idx_sector_month ON {schema}.sector(month);
            """).format(schema=sql.Identifier(SCHEMA)))
            print("OK Table 'sector' created or already exists")

def create_all_tables():
    """Create all tables."""
    print("\n" + "=" * 60)
    print("Creating Database Tables")
    print("=" * 60 + "\n")
    
    try:
        create_schema()
        create_companies_table()
        create_county_table()
        create_nationality_table()
        create_sector_table()
        print("\nOK All tables created successfully!\n")
    except Exception as e:
        print(f"\nERROR creating tables: {e}\n")
        raise

def test_connection():
    """Test database connection."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version();")
                version = cur.fetchone()
                print("OK Connection successful!")
                print(f"  Database: {version[0][:80]}...")
                return True
    except Exception as e:
        print(f"ERROR Connection failed: {e}")
        return False

if __name__ == "__main__":
    print("Testing database connection...")
    if test_connection():
        create_all_tables()
    else:
        print("Cannot proceed without database connection.")
