import pytest
from unittest.mock import AsyncMock, patch
import aiohttp

async def fetch_pdf(session, url):
    print(f"Fetching PDF from URL: {url}", flush=True)
    async with session.get(url) as response:
        if response.status == 200:
            return await response.read()
        else:
            raise Exception(f"Failed to fetch PDF from {url} - Status code: {response.status}")


@pytest.mark.asyncio
async def test_fetch_pdf_success():
    url = "https://bcuathletics.com/documents/2026/3/1/BGSU3126.pdf"
    expected_content = b"%PDF-1.4 test pdf content"

    # Mock the aiohttp.ClientSession.get method
    with patch("aiohttp.ClientSession.get") as mock_get:
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.read.return_value = expected_content
        mock_get.return_value.__aenter__.return_value = mock_response

        async with aiohttp.ClientSession() as session:
            content = await fetch_pdf(session, url)
            print(f"Fetched content: {content}", flush=True)
            assert content == expected_content


@pytest.mark.asyncio
async def test_fetch_pdf_failure():
    url = "https://bcuathletics.com/documents/2026/3/1/BGSU3126.pdf"

    # Mock the aiohttp.ClientSession.get method to return a non-200 status
    with patch("aiohttp.ClientSession.get") as mock_get:
        mock_response = AsyncMock()
        mock_response.status = 404
        mock_get.return_value.__aenter__.return_value = mock_response

        async with aiohttp.ClientSession() as session:
            with pytest.raises(Exception) as exc_info:
                await fetch_pdf(session, url)
            assert "Failed to fetch PDF" in str(exc_info.value)



