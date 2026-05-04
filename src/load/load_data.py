"""
Simple script to load all consolidated permit data into PostgreSQL database.
This script loads the 4 consolidated CSV files into the database.
"""

import os
import pandas as pd
from datetime import datetime
from psycopg2.extras import execute_batch
from db_config import get_db_connection, SCHEMA, create_all_tables

# Configuration
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
BASE_DATA_DIR = os.path.join(PROJECT_ROOT, "data")
BATCH_SIZE = 1000

def consolidate_companies(df):
    """Keep one row per company/year/month, summing totals from duplicate keys."""
    return (
        df.groupby(["Year", "Company", "Month"], dropna=False, as_index=False)
        .agg({"Total": "sum"})
    )

def load_companies():
    """Load companies data."""
    print("Loading companies data...")
    file_path = os.path.join(BASE_DATA_DIR, "permits-by-companies-consolidated.csv")
    if not os.path.exists(file_path):
        print("  ERROR Companies file not found")
        return

    df = consolidate_companies(pd.read_csv(file_path))
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Prepare batch data
            rows = [(int(row['Year']), str(row['Company']),
                    float(row['Total']) if pd.notna(row['Total']) else None,
                    str(row['Month']) if pd.notna(row['Month']) else None)
                   for _, row in df.iterrows()]

            # Insert in batches with UPSERT
            query = f"""
                INSERT INTO {SCHEMA}.companies (year, company, total, month)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (year, company, month) DO UPDATE SET
                    total = EXCLUDED.total
            """
            execute_batch(cur, query, rows, page_size=BATCH_SIZE)

    print(f"  OK Loaded {len(df)} companies records")

def load_county():
    """Load county data."""
    print("Loading county data...")
    file_path = os.path.join(BASE_DATA_DIR, "permits-issued-by-county-consolidated.csv")
    if not os.path.exists(file_path):
        print("  ERROR County file not found")
        return

    df = pd.read_csv(file_path)
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Prepare batch data
            rows = [(int(row['Year']), str(row['County']),
                    float(row['Issued']) if pd.notna(row['Issued']) else None,
                    float(row['Refused']) if pd.notna(row['Refused']) else None,
                    float(row['Withdrawn']) if pd.notna(row['Withdrawn']) else None)
                   for _, row in df.iterrows()]

            # Insert in batches with UPSERT
            query = f"""
                INSERT INTO {SCHEMA}.county (year, county, issued, refused, withdrawn)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (year, county) DO UPDATE SET
                    issued = EXCLUDED.issued,
                    refused = EXCLUDED.refused,
                    withdrawn = EXCLUDED.withdrawn
            """
            execute_batch(cur, query, rows, page_size=BATCH_SIZE)

    print(f"  OK Loaded {len(df)} county records")

def load_nationality():
    """Load nationality data."""
    print("Loading nationality data...")
    file_path = os.path.join(BASE_DATA_DIR, "permits-by-nationality-consolidated.csv")
    if not os.path.exists(file_path):
        print("  ERROR Nationality file not found")
        return

    df = pd.read_csv(file_path)
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Prepare batch data
            rows = [(int(row['Year']), str(row['Country']),
                    float(row['Issued']) if pd.notna(row['Issued']) else None,
                    float(row['Refused']) if pd.notna(row['Refused']) else None,
                    float(row['Withdrawn']) if pd.notna(row['Withdrawn']) else None)
                   for _, row in df.iterrows()]

            # Insert in batches with UPSERT
            query = f"""
                INSERT INTO {SCHEMA}.nationality (year, country, issued, refused, withdrawn)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (year, country) DO UPDATE SET
                    issued = EXCLUDED.issued,
                    refused = EXCLUDED.refused,
                    withdrawn = EXCLUDED.withdrawn
            """
            execute_batch(cur, query, rows, page_size=BATCH_SIZE)

    print(f"  OK Loaded {len(df)} nationality records")

def load_sector():
    """Load sector data."""
    print("Loading sector data...")
    file_path = os.path.join(BASE_DATA_DIR, "permits-by-sector-consolidated.csv")
    if not os.path.exists(file_path):
        print("  ERROR Sector file not found")
        return

    df = pd.read_csv(file_path)
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Prepare batch data
            rows = [(int(row['Year']),
                    str(row['Month']) if pd.notna(row['Month']) else None,
                    str(row['Sector']),
                    float(row['Issued']) if pd.notna(row['Issued']) else None,
                    float(row['Refused']) if pd.notna(row['Refused']) else None,
                    float(row['Withdrawn']) if pd.notna(row['Withdrawn']) else None,
                    str(row['Prefix']) if pd.notna(row['Prefix']) else None)
                   for _, row in df.iterrows()]

            # Insert in batches with UPSERT
            query = f"""
                INSERT INTO {SCHEMA}.sector (year, month, sector, issued, refused, withdrawn, prefix)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (year, month, sector) DO UPDATE SET
                    issued = EXCLUDED.issued,
                    refused = EXCLUDED.refused,
                    withdrawn = EXCLUDED.withdrawn,
                    prefix = EXCLUDED.prefix
            """
            execute_batch(cur, query, rows, page_size=BATCH_SIZE)

    print(f"  OK Loaded {len(df)} sector records")

def main():
    """Load all data to database."""
    print("\n" + "=" * 60)
    print(f"LOADING HISTORICAL DATA TO DATABASE | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60 + "\n")

    # Create tables
    print("Setting up database...")
    create_all_tables()

    # Load all data
    load_companies()
    load_county()
    load_nationality()
    load_sector()

    print("\n" + "=" * 60)
    print("OK All historical data loaded successfully!")
    print("=" * 60 + "\n")

if __name__ == "__main__":
    main()
