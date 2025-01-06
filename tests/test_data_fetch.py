import pytest
import httpx
from httpx import MockTransport
from scripts.data_fetch import fetch_data
import os

API_KEY = os.getenv('API_KEY')

@pytest.fixture
def mock_transport_success():
    response_data = {
        "Time Series (Daily)": {
            "2025-01-06": {
                "1. open": "135.67",
                "2. high": "136.75",
                "3. low": "133.12",
                "4. close": "134.87",
                "5. volume": "23456789"
            },
            "2025-01-05": {
                "1. open": "134.78",
                "2. high": "135.90",
                "3. low": "132.45",
                "4. close": "135.12",
                "5. volume": "34567890"
            }
        }
    }

    def mock_response(request):
        return httpx.Response(200, json=response_data)

    return MockTransport(mock_response)

@pytest.fixture
def mock_transport_invalid_response():
    def mock_response(request):
        return httpx.Response(200, json={"Error Message": "Invalid API call"})

    return MockTransport(mock_response)

@pytest.fixture
def mock_transport_http_error():
    def mock_response(request):
        return httpx.Response(404)

    return MockTransport(mock_response)

@pytest.fixture
def mock_transport_request_error():
    def mock_response(request):
        raise httpx.RequestError("Network error")

    return MockTransport(mock_response)

@pytest.mark.asyncio
async def test_fetch_data_success(mock_transport_success):
    transport = mock_transport_success
    async with httpx.AsyncClient(transport=transport) as client:
        httpx._default_client = client
        result = await fetch_data("TIME_SERIES_DAILY", symbol="AAPL")

        expected_response = {
            "Time Series (Daily)": {
                "2025-01-06": {
                    "1. open": "135.67",
                    "2. high": "136.75",
                    "3. low": "133.12",
                    "4. close": "134.87",
                    "5. volume": "23456789"
                },
                "2025-01-05": {
                    "1. open": "134.78",
                    "2. high": "135.90",
                    "3. low": "132.45",
                    "4. close": "135.12",
                    "5. volume": "34567890"
                }
            }
        }

        assert result == expected_response

@pytest.mark.asyncio
async def test_fetch_data_invalid_response(mock_transport_invalid_response):
    transport = mock_transport_invalid_response
    async with httpx.AsyncClient(transport=transport) as client:
        httpx._default_client = client
        with pytest.raises(RuntimeError, match="API returned an error: API Error: Invalid API call"):
            await fetch_data("TIME_SERIES_DAILY", symbol="INVALID")

@pytest.mark.asyncio
async def test_fetch_data_http_error(mock_transport_http_error):
    transport = mock_transport_http_error
    async with httpx.AsyncClient(transport=transport) as client:
        httpx._default_client = client
        with pytest.raises(RuntimeError, match="HTTP error occurred: 404 - Not Found"):
            await fetch_data("TIME_SERIES_DAILY", symbol="AAPL")

@pytest.mark.asyncio
async def test_fetch_data_request_error(mock_transport_request_error):
    transport = mock_transport_request_error
    async with httpx.AsyncClient(transport=transport) as client:
        httpx._default_client = client
        with pytest.raises(RuntimeError, match="An error occurred while requesting data: Network error"):
            await fetch_data("TIME_SERIES_DAILY", symbol="AAPL")