import httpx
import asyncio
import os

ALPHA_VANTAGE_BASE_URL = 'https://www.alphavantage.co/query'
API_KEY = os.getenv('API_KEY')

if not API_KEY:
    raise ValueError("Please set the ALPHA_VANTAGE_API_KEY environment variable")


async def fetch_data(function: str, **params):
    """
    Fetch data from the Alpha Vantage API.

    :param function: The Alpha Vantage function to call (e.g., "TIME_SERIES_DAILY").
    :param params: Additional parameters for the API call.
    :return: JSON response from the API.
    """
    params.update({
        "function": function,
        "apikey": API_KEY,
        "datatype": "json"
    })

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(ALPHA_VANTAGE_BASE_URL, params=params)
            response.raise_for_status()  # Raise an exception for HTTP errors
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

# async def test():
#     data = await fetch_data("TIME_SERIES_DAILY", symbol="AAPL")
#     print(data)
#
# asyncio.run(test())