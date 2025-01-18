import os
import duckdb
import polars as pl
import asyncio
from pyiceberg.catalog import load_catalog


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
                CREATE TABLE IF NOT EXISTS time_series (
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

            # Update existing records
            con.execute('''
                        UPDATE time_series
                        SET symbol = time_series_df.symbol,
                            date = time_series_df.date,
                            open = time_series_df.open,
                            high = time_series_df.high,
                            low = time_series_df.low,
                            close = time_series_df.close,
                            volume = time_series_df.volume,
                            CreatedAt = current_timestamp
                        FROM time_series_df
                        WHERE time_series.key = CONCAT(time_series_df.symbol, time_series_df.date::STRING)
                    ''')

            print("Data updated successfully")

    @staticmethod
    def query_time_series():
        # Query the time_series table
        with duckdb.connect(DuckDb.db_path) as con:
            con.sql('SELECT Symbol, MIN(Date) MinDate, MAX(Date) MaxDate FROM time_series GROUP BY Symbol').show()


class IcebergLoader:
    def __init__(self, catalog_name: str = 'sandbox', namespace: str = 'alpha'):
        self.catalog_name = catalog_name
        self.namespace = namespace
        self.catalog = load_catalog(catalog_name)

    def create_table(self, table_name: str, df: pl.DataFrame):
        try:
            df_pa = df.to_arrow()
            self.catalog.create_table(f"{self.namespace}.{table_name}", schema=df_pa.schema)

        except Exception as e:
            print(f"An error occurred while trying to create Iceberg table: {e}")

    def load_table(self, table_name: str, df: pl.DataFrame):
        try:
            table = self.catalog.load_table(f"{self.namespace}.{table_name}")
            table.append(df.to_arrow())
        except Exception as e:
            print(f"An error occurred while trying to load data into Iceberg table: {e}")

    def read_table(self, table_name: str):
        try:
            table = self.catalog.load_table(f"{self.namespace}.{table_name}")
            return table
        except Exception as e:
            print(f"An error occurred while trying to read Iceberg table: {e}")

