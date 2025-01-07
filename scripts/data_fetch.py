import httpx
import asyncio
import os

class AlphaVantageAPI:
    BASE_URL = "https://www.alphavantage.co/query?"
    API_KEY = os.getenv("API_KEY")

    if not API_KEY:
        raise ValueError("Please set the ALPHA_VANTAGE_API_KEY environment variable")

    @staticmethod
    async def fetch(endpoint: str, symbol:str, **params):
        params.update({
            "function": endpoint,
            "apikey": AlphaVantageAPI.API_KEY,
            "symbol": symbol,
            "datatype": "json"
        })

        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(AlphaVantageAPI.BASE_URL, params=params)
                response.raise_for_status()
                data = response.json()

                if "Error Message" in data:
                    raise ValueError(f"API Error: {data['Error Message']}")

                return data

            except httpx.HTTPStatusError as e:
                raise RuntimeError(f"HTTP error occurred: {e.response.status_code} - {e.response.text}")
            except httpx.RequestError as e:
                raise RuntimeError(f"An error occurred while requesting data: {e}")
            except ValueError as e:
                raise RuntimeError(f"API returned an error: {e}")

    @staticmethod
    async def fetch_time_series_daily(symbol: str):
        return await AlphaVantageAPI.fetch("TIME_SERIES_DAILY", symbol=symbol)

    @staticmethod
    async def fetch_intraday(symbol: str, interval: str):
        return await AlphaVantageAPI.fetch("TIME_SERIES_INTRADAY", symbol=symbol, interval=interval)

    @staticmethod
    async def fetch_company_overview(symbol: str):
        return await AlphaVantageAPI.fetch("OVERVIEW", symbol=symbol)