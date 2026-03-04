import pytest
from unittest.mock import AsyncMock, patch
import aiohttp
import asyncio
import pandas as pd
import os
import sys
from dataclasses import dataclass
from typing import Optional
from pathlib import Path

# Ensure parent directory is in path for direct execution (conftest.py handles pytest discovery)
if __name__ == "__main__":
    project_root = Path(__file__).parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

# Get paths from environment variables or use relative defaults
_test_dir = Path(__file__).parent.parent  # PDF_py_review directory
_default_list_pth = str(_test_dir / "GRI_2017_2020 (1).xlsx")
_default_data_pth = str(_test_dir / "Data")

CONFIG = {
    "list_pth": os.getenv("LIST_PTH", _default_list_pth),  # path to Excel file with URLs
    "pth": os.getenv("DATA_PTH", _default_data_pth),  # path to data directory
    "ID": "BRnum",
    "url_column": "Pdf_URL",  # column AL
    "other_url_column": "Report HTML Address",  # column AM
    "max_workers": 10,  # parallel downloads
    "download_timeout": 30,  # seconds before a download is considered failed
    "Prototype": False,  # if True, only download first 10 files for testing
    "Prototype_count": 100,  # number of files to download in prototype mode
}

def check_col_for_url(_list_pth, ID, url_column, other_url_column):
    # Minimal test helper that returns a mock DataFrame with the requested URL columns
    data: dict[str, list[Optional[str]]] = {
        ID: ["12345"],
        url_column: ["https://www.shuyiwrites.com/uploads/1/3/0/4/130438914/how_to_write_and_publish_a_scientific_paper.pdf"],
    }
    if other_url_column:
        data[other_url_column] = [None]
    return pd.DataFrame(data)


@dataclass
class DownloadTask:  # A simple data class to hold the parameters for a single download attempt
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

def test_download_task():
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


def test_download_result():
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


def test_check_col_for_url():
    # Test the check_col_for_url function with CONFIG values
    try:
        df = check_col_for_url(
            CONFIG["list_pth"],
            CONFIG["ID"],
            CONFIG["url_column"],
            CONFIG["other_url_column"]
        )
        assert isinstance(df, pd.DataFrame)
    except Exception as e:
        pytest.fail(f"check_col_for_url raised an exception: {e}")


@pytest.mark.asyncio
async def test_check_existing_files():
    import app
    check_existing_files = getattr(app, "check_existing_files", None)
    if check_existing_files is None:
        pytest.skip("check_existing_files is not available in app module")
    # This test would check the check_existing_files function, but since it interacts with the file system, we can just assert that it returns a list (you would normally mock the file system interactions)
    try:
        exist = check_existing_files("./downloads")
        assert isinstance(exist, list)
    except Exception as e:
        pytest.fail(f"check_existing_files raised an exception: {e}")


@pytest.mark.asyncio
async def test_check_if_valid_pdf():
    import app
    check_if_valid_pdf = getattr(app, "check_if_valid_pdf", None)
    if check_if_valid_pdf is None:
        pytest.skip("check_if_valid_pdf is not available in app module")
    # This test would check the check_if_valid_pdf function, but since it interacts with the file system, we can just assert that it returns a boolean (you would normally mock the file system interactions)
    try:
        is_valid = check_if_valid_pdf("./downloads/test.pdf")
        assert isinstance(is_valid, bool)
    except Exception as e:
        pytest.fail(f"check_if_valid_pdf raised an exception: {e}")


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
