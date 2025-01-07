import asyncio
from scripts.data_fetch import AlphaVantageAPI
from scripts.data_parse import DataParser

async def main():
    symbol = "AAPL"  # Replace with your desired stock symbol
    try:
        # Fetch and parse daily time series data
        raw_data_daily = await AlphaVantageAPI.fetch_time_series_daily(symbol)
        df_daily = DataParser.parse_time_series(raw_data_daily)
        print("Daily Time Series Data:")
        print(df_daily)

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