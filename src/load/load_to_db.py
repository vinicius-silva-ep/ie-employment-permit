"""
Script to load consolidated permit data into PostgreSQL database.
Reads consolidated CSV files and inserts them into the respective tables.
Uses batch processing for optimal performance.
"""

import os
import pandas as pd
from datetime import datetime
from psycopg2 import sql
from psycopg2.extras import execute_values
from db_config import get_db_connection, SCHEMA, create_all_tables

# Configuration
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
BASE_DATA_DIR = os.path.join(PROJECT_ROOT, "data")
BATCH_SIZE = 5000  # Insert in batches for performance

def to_text(value):
    """Convert pandas missing values to SQL NULL-compatible None."""
    return str(value) if pd.notna(value) else None

def to_float(value):
    """Convert pandas missing numeric values to SQL NULL-compatible None."""
    return float(value) if pd.notna(value) else None

def consolidate_companies(df):
    """Keep one row per company/year/month, summing totals from duplicate keys."""
    return (
        df.groupby(["Year", "Company", "Month"], dropna=False, as_index=False)
        .agg({"Total": "sum"})
    )

def delete_existing_years(cur, table_name, years):
    """Clear rows for the years present in the file being loaded."""
    cur.execute(
        sql.SQL("DELETE FROM {schema}.{table} WHERE year = ANY(%s)").format(
            schema=sql.Identifier(SCHEMA),
            table=sql.Identifier(table_name),
        ),
        ([int(year) for year in years],),
    )

def insert_companies_data_batch(df):
    """Insert companies data into database using batch processing."""
    if df.empty:
        print("  No data to insert")
        return
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            df = consolidate_companies(df)
            delete_existing_years(cur, "companies", df['Year'].unique())
            
            # Prepare batch data
            rows = [
                (
                    int(row.Year),
                    str(row.Company),
                    to_float(row.Total),
                    to_text(row.Month)
                )
                for row in df.itertuples(index=False)
            ]
            
            # Insert in batches
            insert_query = sql.SQL("""
                INSERT INTO {schema}.companies
                (year, company, total, month)
                VALUES %s
                ON CONFLICT (year, company, month) DO UPDATE SET
                    total = EXCLUDED.total
            """).format(schema=sql.Identifier(SCHEMA))
            
            execute_values(cur, insert_query.as_string(cur), rows, page_size=BATCH_SIZE)
    print(f"  OK Inserted {len(df)} companies records")

def insert_county_data_batch(df):
    """Insert county data into database using batch processing."""
    if df.empty:
        print("  No data to insert")
        return
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            delete_existing_years(cur, "county", df['Year'].unique())
            
            # Prepare batch data
            rows = [
                (
                    int(row.Year),
                    str(row.County),
                    to_float(row.Issued),
                    to_float(row.Refused),
                    to_float(row.Withdrawn)
                )
                for row in df.itertuples(index=False)
            ]
            
            # Insert in batches
            insert_query = sql.SQL("""
                INSERT INTO {schema}.county
                (year, county, issued, refused, withdrawn)
                VALUES %s
                ON CONFLICT (year, county) DO UPDATE SET
                    issued = EXCLUDED.issued,
                    refused = EXCLUDED.refused,
                    withdrawn = EXCLUDED.withdrawn
            """).format(schema=sql.Identifier(SCHEMA))
            
            execute_values(cur, insert_query.as_string(cur), rows, page_size=BATCH_SIZE)
    print(f"  OK Inserted {len(df)} county records")

def insert_nationality_data_batch(df):
    """Insert nationality data into database using batch processing."""
    if df.empty:
        print("  No data to insert")
        return
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            delete_existing_years(cur, "nationality", df['Year'].unique())
            
            # Prepare batch data
            rows = [
                (
                    int(row.Year),
                    str(row.Country),
                    to_float(row.Issued),
                    to_float(row.Refused),
                    to_float(row.Withdrawn)
                )
                for row in df.itertuples(index=False)
            ]
            
            # Insert in batches
            insert_query = sql.SQL("""
                INSERT INTO {schema}.nationality
                (year, country, issued, refused, withdrawn)
                VALUES %s
                ON CONFLICT (year, country) DO UPDATE SET
                    issued = EXCLUDED.issued,
                    refused = EXCLUDED.refused,
                    withdrawn = EXCLUDED.withdrawn
            """).format(schema=sql.Identifier(SCHEMA))
            
            execute_values(cur, insert_query.as_string(cur), rows, page_size=BATCH_SIZE)
    print(f"  OK Inserted {len(df)} nationality records")

def insert_sector_data_batch(df):
    """Insert sector data into database using batch processing."""
    if df.empty:
        print("  No data to insert")
        return
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            delete_existing_years(cur, "sector", df['Year'].unique())
            
            # Prepare batch data
            rows = [
                (
                    int(row.Year),
                    to_text(row.Month),
                    str(row.Sector),
                    to_float(row.Issued),
                    to_float(row.Refused),
                    to_float(row.Withdrawn),
                    to_text(row.Prefix)
                )
                for row in df.itertuples(index=False)
            ]
            
            # Insert in batches
            insert_query = sql.SQL("""
                INSERT INTO {schema}.sector
                (year, month, sector, issued, refused, withdrawn, prefix)
                VALUES %s
                ON CONFLICT (year, month, sector) DO UPDATE SET
                    issued = EXCLUDED.issued,
                    refused = EXCLUDED.refused,
                    withdrawn = EXCLUDED.withdrawn,
                    prefix = EXCLUDED.prefix
            """).format(schema=sql.Identifier(SCHEMA))
            
            execute_values(cur, insert_query.as_string(cur), rows, page_size=BATCH_SIZE)
    print(f"  OK Inserted {len(df)} sector records")

def load_all_to_database():
    """Load all consolidated data to database using optimized batch processing."""
    print("\n" + "=" * 80)
    print(f"LOADING DATA TO DATABASE (OPTIMIZED) | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80 + "\n")
    
    # Create tables first
    try:
        create_all_tables()
    except Exception as e:
        print(f"ERROR creating tables: {e}")
        return
    
    # Load companies data
    print("[1/4] Loading Companies Data...")
    companies_file = os.path.join(BASE_DATA_DIR, "permits-by-companies-consolidated.csv")
    if os.path.exists(companies_file):
        try:
            df = pd.read_csv(companies_file)
            insert_companies_data_batch(df)
        except Exception as e:
            print(f"  ERROR: {e}")
    else:
        print(f"  ERROR File not found: {companies_file}")
    
    # Load county data
    print("\n[2/4] Loading County Data...")
    county_file = os.path.join(BASE_DATA_DIR, "permits-issued-by-county-consolidated.csv")
    if os.path.exists(county_file):
        try:
            df = pd.read_csv(county_file)
            insert_county_data_batch(df)
        except Exception as e:
            print(f"  ERROR: {e}")
    else:
        print(f"  ERROR File not found: {county_file}")
    
    # Load nationality data
    print("\n[3/4] Loading Nationality Data...")
    nationality_file = os.path.join(BASE_DATA_DIR, "permits-by-nationality-consolidated.csv")
    if os.path.exists(nationality_file):
        try:
            df = pd.read_csv(nationality_file)
            insert_nationality_data_batch(df)
        except Exception as e:
            print(f"  ERROR: {e}")
    else:
        print(f"  ERROR File not found: {nationality_file}")
    
    # Load sector data
    print("\n[4/4] Loading Sector Data...")
    sector_file = os.path.join(BASE_DATA_DIR, "permits-by-sector-consolidated.csv")
    if os.path.exists(sector_file):
        try:
            df = pd.read_csv(sector_file)
            insert_sector_data_batch(df)
        except Exception as e:
            print(f"  ERROR: {e}")
    else:
        print(f"  ERROR File not found: {sector_file}")
    
    print("\n" + "=" * 80)
    print("OK Database loading completed!")
    print("=" * 80 + "\n")

if __name__ == "__main__":
    load_all_to_database()
