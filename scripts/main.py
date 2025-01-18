import asyncio
from scripts.data_fetch import AlphaVantageAPI
from scripts.data_parse import DataParser
from scripts.data_load import DuckDb, IcebergLoader
import duckdb
import os
import polars as pl
from pyiceberg.table import Table


def iceberg_test():
    # Create a sample Polars DataFrame
    data = {
        "symbol": ["AAPL", "GOOGL", "MSFT"],
        "date": ["2023-01-01", "2023-01-02", "2023-01-03"],
        "open": [150.0, 2800.0, 300.0],
        "high": [155.0, 2850.0, 310.0],
        "low": [148.0, 2750.0, 295.0],
        "close": [152.0, 2825.0, 305.0],
        "volume": [1000000, 1500000, 1200000]
    }

    df = pl.DataFrame(data)

    print(df.schema)

    ice = IcebergLoader()
    #ice.create_table('test_data', df)
    ice.load_table('test_data', df)
    table: Table =  ice.read_table('test_data')

    print(table)

    print(table.scan().to_arrow())


async def main():
    symbols = ['NOC']
    try:
        # Fetch and parse daily time series data
        for symbol in symbols:
            raw_data_daily = await AlphaVantageAPI.fetch_time_series_daily(symbol)
            if "Information" in raw_data_daily.keys():
                print("Daily rate limit reached")
                break
            df_daily = DataParser.parse_time_series(raw_data_daily)
            print("Daily Time Series Data:")
            print(df_daily)
            DuckDb.setup_duckdb()
            DuckDb.load_time_series(df_daily)

        DuckDb.query_time_series()


        # # Fetch and parse intraday time series data
        # interval = "5min"
        # raw_data_intraday = await AlphaVantageAPI.fetch_intraday(symbol, interval)
        # df_intraday = DataParser.parse_time_series(raw_data_intraday)
        # print("\nIntraday Time Series Data:")
        # print(df_intraday)

        # # Fetch and parse company overview data
        # raw_data_overview = await AlphaVantageAPI.fetch_company_overview(symbol)
        # df_overview = DataParser.parse_overview(raw_data_overview)
        # print("\nCompany Overview Data:")
        # print(df_overview)

    except Exception as e:
        print(f"An error occurred: {e}")



if __name__ == "__main__":
    asyncio.run(main())
