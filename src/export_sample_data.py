#!/usr/bin/env python3
"""
Export sample data from GitHub Issues database to CSV files
Exports 1000 rows from each table to separate CSV files

Note: The export script was developed with AI assistance. While functional, 
the implementation has not been reviewed thoroughly.
"""

import polars as pl
from pathlib import Path
import logging
from database import DatabaseManager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def export_table_to_csv(db_manager, table_name, output_dir, limit=1000):
    """
    Export a table to CSV format using Polars
    
    Args:
        db_manager: DatabaseManager instance
        table_name: Name of the table to export
        output_dir: Directory to save CSV files
        limit: Maximum number of rows to export (default: 1000)
    """
    try:
        logger.info(f"Exporting {table_name} table (limit: {limit} rows)")
        
        # Use DuckDB's relation object for better compatibility with Polars
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        
        # Execute query and get relation
        relation = db_manager.connection.execute(query)
        
        # Convert to Polars DataFrame using pl.from_duckdb
        df = pl.from_duckdb(relation)
        
        if df.height == 0:
            logger.warning(f"No data found in {table_name} table")
            return
        
        # Create output file path
        output_file = output_dir / f"{table_name}.csv"
        
        # Write to CSV
        df.write_csv(output_file)
        
        logger.info(f"Successfully exported {df.height} rows from {table_name} to {output_file}")
        
    except Exception as e:
        logger.error(f"Error exporting {table_name}: {e}")
        # Try alternative approach if pl.from_duckdb fails
        try:
            logger.info(f"Trying alternative approach for {table_name}")
            
            # Query the table with limit
            query = f"SELECT * FROM {table_name} LIMIT {limit}"
            result = db_manager.connection.execute(query).fetchall()
            
            if not result:
                logger.warning(f"No data found in {table_name} table")
                return
            
            # Get column names
            columns = [desc[0] for desc in db_manager.connection.description]
            
            # Create Polars DataFrame with explicit orientation and higher infer_schema_length
            df = pl.DataFrame(result, schema=columns, orient="row", infer_schema_length=len(result))
            
            # Create output file path
            output_file = output_dir / f"{table_name}.csv"
            
            # Write to CSV
            df.write_csv(output_file)
            
            logger.info(f"Successfully exported {len(result)} rows from {table_name} to {output_file}")
            
        except Exception as e2:
            logger.error(f"Alternative approach also failed for {table_name}: {e2}")
            raise

def main():
    """
    Main function to export sample data from all tables
    """
    logger.info("Starting sample data export")
    
    # Create output directory
    output_dir = Path("sample_data")
    output_dir.mkdir(exist_ok=True)
    logger.info(f"Created output directory: {output_dir}")
    
    # Database tables to export
    tables = ['organizations', 'repositories', 'issues', 'events']
    
    try:
        # Initialize database manager
        db_manager = DatabaseManager()
        db_manager.connect()
        
        # Show current database stats
        logger.info("Current database statistics:")
        stats = db_manager.get_database_stats()
        for table, count in stats.items():
            logger.info(f"  {table}: {count} rows")
        
        # Export each table
        for table in tables:
            export_table_to_csv(db_manager, table, output_dir, limit=1000)
        
        logger.info("Sample data export completed successfully!")
        logger.info(f"CSV files saved to: {output_dir.absolute()}")
        
        # List created files
        csv_files = list(output_dir.glob("*.csv"))
        logger.info(f"Created {len(csv_files)} CSV files:")
        for file in csv_files:
            file_size = file.stat().st_size
            logger.info(f"  {file.name}: {file_size:,} bytes")
        
    except Exception as e:
        logger.error(f"Error during export: {e}")
        raise
        
    finally:
        # Close database connection
        if 'db_manager' in locals():
            db_manager.close()

if __name__ == "__main__":
    main() 