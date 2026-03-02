import pytest
from unittest.mock import AsyncMock, patch
import aiohttp
import asyncio
from dataclasses import dataclass
from typing import Optional

@dataclass
class DownloadTask:  # A simple data class to hold the result of a single download attempt
    brnum: str
    url_column: str
    other_url_column: Optional[str]
    output_dir: str
    timeout: int


@dataclass
class DownloadResult:  # A simple data class to hold the status of a download attempt
    brnum: str
    status: str  # e.g., "Downloaded", "Ikke downloaded"
    url_used: str  # the URL that was actually used for the download attempt (either from url_column or other_url_column)
    error: Optional[str]

@pytest.mark.asyncio
async def test_download_task():
    # Example of creating a DownloadTask instance
    task = DownloadTask(
        brnum="12345",
        url_column="https://bcuathletics.com/documents/2026/3/1/BGSU3126.pdf",
        other_url_column=None,
        output_dir="./downloads",
        timeout=30
    )
    assert task.brnum == "12345"
    assert task.url_column == "https://bcuathletics.com/documents/2026/3/1/BGSU3126.pdf"
    assert task.other_url_column is None
    assert task.output_dir == "./downloads"
    assert task.timeout == 30


@pytest.mark.asyncio
async def test_download_result():
    # Example of creating a DownloadResult instance
    result = DownloadResult(
        brnum="12345",
        status="Downloaded",
        url_used="https://bcuathletics.com/documents/2026/3/1/BGSU3126.pdf",
        error=None
    )
    assert result.brnum == "12345"
    assert result.status == "Downloaded"
    assert result.url_used == "https://bcuathletics.com/documents/2026/3/1/BGSU3126.pdf"
    assert result.error is None


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_fetch_pdf_exception():
    url = "https://bcuathletics.com/documents/2026/3/1/BGSU3126.pdf"

    # Mock the aiohttp.ClientSession.get method to raise an exception
    with patch("aiohttp.ClientSession.get") as mock_get:
        mock_get.side_effect = Exception("Network error")

        async with aiohttp.ClientSession() as session:
            with pytest.raises(Exception) as exc_info:
                await fetch_pdf(session, url)
            assert "Network error" in str(exc_info.value)


@pytest.mark.asyncio
async def test_fetch_multiple_pdfs():
    urls = [
        "https://bcuathletics.com/documents/2026/3/1/BGSU3126.pdf",
        "https://www.shuyiwrites.com/uploads/1/3/0/4/130438914/how_to_write_and_publish_a_scientific_paper.pdf"
    ]
    expected_content = b"%PDF-1.4 test pdf content"

    # Mock the aiohttp.ClientSession.get method
    with patch("aiohttp.ClientSession.get") as mock_get:
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.read.return_value = expected_content
        mock_get.return_value.__aenter__.return_value = mock_response

        async with aiohttp.ClientSession() as session:
            tasks = [fetch_pdf(session, url) for url in urls]
            results = await asyncio.gather(*tasks)
            for content in results:
                print(f"Fetched content: {content}", flush=True)
                assert content == expected_content


if __name__ == "__main__":
    # Run tests using pytest's programmatic runner with full pytest-asyncio support
    exit_code = pytest.main([__file__, "-v"])
    if exit_code == 0:
        print("\n✅ All tests passed!", flush=True)
    else:
        print("\n❌ Some tests failed!", flush=True)
    exit(exit_code)
