# Load Scripts Documentation

This folder contains scripts to load consolidated employment permit data into PostgreSQL/CockroachDB.

## Scripts Overview

### Database Configuration

- **`db_config.py`** - Database connection management and table creation utilities
  - Connection pooling for PostgreSQL
  - Schema and table creation with proper indexes
  - Used by all database-related scripts

- **`init_db.py`** - Database initialization script
  - Tests database connection
  - Creates schema and tables
  - **Run this first before any data loading**

### Data Loading

- **`load_data.py`** - Main data loading script
  - Loads all 4 consolidated CSV files into the database
  - Uses optimized batch processing for performance
  - **This is the main script to run after init_db.py**

## Quick Start

### 1. Initialize Database (Run Once)
```bash
python init_db.py
```
This tests the connection and creates all necessary tables.

### 2. Load Historical Data
```bash
python load_data.py
```
This will load all consolidated data (2009-2025) into the PostgreSQL database.

## Configuration

Database credentials are read from `.env` file in the project root:
```
DB_HOST = your-database-host
DB_PORT = 26257
DB_NAME = your-database-name
DB_USER = your-username
DB_PASSWORD = your-password
DB_SCHEMA = ie_emp_permits
```

## Database Tables

- `{schema}.companies` - Company permit data
- `{schema}.county` - County-level permit statistics
- `{schema}.nationality` - Nationality permit statistics
- `{schema}.sector` - Sector-wise permit data

## Features

✓ Optimized batch processing for fast loading
✓ Automatic table creation
✓ Data validation and null handling
✓ Progress tracking
✓ Schema isolation using PostgreSQL schemas

## Database Schema

Each table includes:
- Automatic unique constraints to prevent duplicates
- Indexes on commonly queried columns (year, company/county/country/sector)
- Auto-generated `id` primary key
- `created_at` timestamp

## Features

✓ Automatic year range detection (2009 to current year)
✓ Error handling and logging
✓ Data validation and null handling
✓ Duplicate prevention with UPSERT (ON CONFLICT)
✓ Connection pooling for performance
✓ Progress tracking
✓ Schema isolation using PostgreSQL schemas
