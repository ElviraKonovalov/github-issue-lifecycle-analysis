# Sample Data Export

This directory contains sample CSV files exported from the GitHub Issues database using the `export_sample_data.py` script.

> **Note**: The export script was developed with AI assistance. While functional, the implementation has not been reviewed thoroughly.

## Export Details

- **Export Date**: Generated using `src/export_sample_data.py`
- **Database**: `github_issues.duckdb`
- **Export Limit**: 1,000 rows per table (where applicable)
- **Total Database Size at Export**:
  - Issues: 10,757 rows
  - Events: 74,238 rows
  - Organizations: 3 rows
  - Repositories: 87 rows

## Files Description

### 1. organizations.csv

- **Size**: 31 bytes (5 lines including header)
- **Rows**: 3 organizations
- **Columns**: `name`
- **Description**: Contains all GitHub organizations in the database
- **Sample Data**: pola-rs, dagster-io, apache

### 2. repositories.csv

- **Size**: 2.3KB (89 lines including header)
- **Rows**: 87 repositories
- **Columns**: `name`, `organization`
- **Description**: Contains all repositories from the tracked organizations
- **Sample Data**: polars (pola-rs), dagster (dagster-io)

### 3. issues.csv

- **Size**: 172KB (1,002 lines including header)
- **Rows**: 1,000 issues (limited from 10,757 total)
- **Columns**: `id`, `number`, `title`, `state`, `created_at`, `updated_at`, `closed_at`, `repository`, `user`, `assignee`, `organization`
- **Description**: Contains GitHub issues with full metadata including creation/update timestamps, state, and assignee information

### 4. events.csv

- **Size**: 96KB (1,002 lines including header)
- **Rows**: 1,000 timeline events (limited from 74,238 total)
- **Columns**: `id`, `issue_id`, `event_type`, `created_at`, `actor`, `label_name`, `assignee_name`, `comment_author`, `comment_body`
- **Description**: Contains timeline events for issues including:
  - Event types: closed, cross-referenced, labeled, unlabeled, assigned, unassigned, commented, etc.
  - Context-specific data in dedicated columns (e.g., label_name for labeled events)

## Export Script

The data was exported using the script: `src/export_sample_data.py`

### Key Features:

- Uses Polars DataFrame for efficient data handling
- Handles schema inference issues with fallback mechanisms
- Exports to CSV format with proper encoding
- Includes comprehensive logging and error handling
- Respects the 1,000 row limit for large tables

### Usage:

```bash
# From project root directory
uv run src/export_sample_data.py
```

### Script Functionality:

1. Connects to the DuckDB database (`github_issues.duckdb`)
2. Queries each table with a LIMIT of 1,000 rows
3. Converts query results to Polars DataFrames
4. Exports DataFrames to CSV files in the `sample_data/` directory
5. Provides detailed logging of the export process

## Database Schema

The database follows this schema:

```sql
-- Organizations table
CREATE TABLE organizations (
    name TEXT PRIMARY KEY
);

-- Repositories table
CREATE TABLE repositories (
    name TEXT,
    organization TEXT REFERENCES organizations(name),
    PRIMARY KEY (name, organization)
);

-- Issues table
CREATE TABLE issues (
    id TEXT PRIMARY KEY,
    number INTEGER NOT NULL,
    title TEXT,
    state TEXT,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    closed_at TIMESTAMP,
    repository TEXT NOT NULL,
    user TEXT,
    assignee TEXT,
    organization TEXT NOT NULL,
    FOREIGN KEY (repository, organization) REFERENCES repositories(name, organization)
);

-- Events table
CREATE TABLE events (
    id TEXT PRIMARY KEY,
    issue_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    actor TEXT,
    label_name TEXT,        -- For labeled/unlabeled events
    assignee_name TEXT,     -- For assigned/unassigned events
    comment_author TEXT,    -- For commented events
    comment_body TEXT,      -- For commented events
    FOREIGN KEY (issue_id) REFERENCES issues(id)
);
```

## Notes

- The export script uses the alternative DataFrame creation approach due to compatibility issues with `pl.from_duckdb()`
- Event-specific columns (`label_name`, `assignee_name`, `comment_author`, `comment_body`) are only populated for relevant event types
- All timestamps are in ISO 8601 format
- The sample represents a subset of the full dataset and maintains referential integrity between tables
