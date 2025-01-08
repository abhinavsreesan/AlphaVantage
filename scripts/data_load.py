import os

import duckdb
import polars as pl
import asyncio

class DuckDb:
    # Connect to DuckDB (create a new database if not exists)
    db_path = os.getenv('DUCKDB_PATH')
    #CONN = duckdb.connect('db_path')

    @staticmethod
    def setup_duckdb():
        try:
            with duckdb.connect(DuckDb.db_path) as con:
                # Create the time_series table if it doesn't exist
                con.execute('''
                CREATE OR REPLACE TABLE time_series (
                    key VARCHAR PRIMARY KEY,
                    symbol VARCHAR,
                    date DATE,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume BIGINT,
                    CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                )
                ''')
        except Exception as e:
            print(f"An error occurred while trying to set up Duck DB tables: {e}")


    @staticmethod
    def load_time_series(df: pl.DataFrame):
        # Insert data from Polars DataFrame into DuckDB table
        with duckdb.connect(DuckDb.db_path) as con:
            con.register('time_series_df', df)

            con.execute('''
                INSERT INTO time_series
                SELECT CONCAT(symbol, date::STRING) as key,
                       symbol, 
                       date, 
                       open, 
                       high, 
                       low, 
                       close, 
                       volume,
                       current_timestamp
                from time_series_df
                WHERE KEY NOT IN (SELECT key FROM time_series)
                ''')

        print("Data inserted successfully")

    @staticmethod
    def query_time_series():
        # Query the time_series table
        with duckdb.connect(DuckDb.db_path) as con:
            con.sql('SELECT * FROM time_series').show()