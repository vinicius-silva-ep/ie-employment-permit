from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import psycopg
from dotenv import load_dotenv
from psycopg import sql

ROOT = Path(__file__).resolve().parents[3]
load_dotenv(ROOT / ".env")

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_SCHEMA = os.getenv("DB_SCHEMA", "public")

DATA_FILE = ROOT / "data" / "2009" / "permits-issued-by-county-2009_new.csv"
TABLE_NAME = "permits_issued_by_county"


def get_connection() -> psycopg.Connection:
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        sslmode="require",
    )


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["Year"] = df["Year"].astype(str)
    df["County"] = df["County"].astype(str).str.strip()
    df[["Issued", "Refused", "Withdrawn"]]
    for column in ["Issued", "Refused", "Withdrawn"]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
        else:
            df[column] = 0

    expected_columns = ["Year", "County", "Issued", "Refused", "Withdrawn"]
    return df[expected_columns]


def ensure_table_exists(conn: psycopg.Connection) -> None:
    query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            Year STRING NOT NULL,
            County STRING NOT NULL,
            Issued INT,
            Refused INT,
            Withdrawn INT,
            UNIQUE (Year, County)
        )
        """
    ).format(
        schema=sql.Identifier(DB_SCHEMA),
        table=sql.Identifier(TABLE_NAME),
    )

    with conn.cursor() as cur:
        cur.execute(query)
    conn.commit()


def upsert_dataframe(conn: psycopg.Connection, df: pd.DataFrame) -> None:
    insert_sql = sql.SQL(
        """
        INSERT INTO {schema}.{table} (Year, County, Issued, Refused, Withdrawn)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (Year, County)
        DO UPDATE SET
            Issued = EXCLUDED.Issued,
            Refused = EXCLUDED.Refused,
            Withdrawn = EXCLUDED.Withdrawn
        """
    ).format(
        schema=sql.Identifier(DB_SCHEMA),
        table=sql.Identifier(TABLE_NAME),
    )

    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]

    with conn.cursor() as cur:
        cur.executemany(insert_sql, rows)
    conn.commit()


def load_data() -> pd.DataFrame:
    if not DATA_FILE.exists():
        raise FileNotFoundError(f"CSV file not found: {DATA_FILE}")

    df = pd.read_csv(DATA_FILE)
    return normalize_dataframe(df)


def main() -> None:
    df = load_data()

    with get_connection() as conn:
        ensure_table_exists(conn)
        upsert_dataframe(conn, df)

    print(f"Loaded {len(df)} rows into {DB_SCHEMA}.{TABLE_NAME}")


if __name__ == "__main__":
    main()
