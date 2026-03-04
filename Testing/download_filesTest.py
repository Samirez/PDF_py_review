import os
import pytest
from unittest.mock import patch, MagicMock, mock_open
import pandas as pd
import sys
from pathlib import Path

# Ensure parent directory is in path for direct execution (conftest.py handles pytest discovery)
if __name__ == "__main__":
    project_root = Path(__file__).parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

from download_files import download_file, download_multiple_files


def test_download_file():
    # Mock test with sample arguments
    args = ("12345", "https://example.com/test.pdf", "./downloads", 30)
    
    with patch('download_files.requests.get') as mock_get:
        mock_response = mock_get.return_value
        mock_response.raise_for_status = MagicMock()
        mock_response.iter_content.return_value = [b'%PDF-1.4 test content']
        
        with patch('download_files.open', mock_open()) as mock_file:
            with patch('download_files.PdfReader') as mock_pdf:
                mock_pdf.return_value.pages = [1]  # Mock 1 page
                
                brnum, status, error = download_file(args)
                
                assert brnum == "12345"
                assert status == "Downloaded"
                assert error is None
                mock_get.assert_called_once()


def test_download_multiple_files():
    # Create mock DataFrame
    df2 = pd.DataFrame({
        'pdf_downloaded': [None, None],
        'Pdf_URL': ["https://www.shuyiwrites.com/uploads/1/3/0/4/130438914/how_to_write_and_publish_a_scientific_paper.pdf", "https://bcuathletics.com/documents/2026/3/1/BGSU3126.pdf"]
    }, index=["BR001", "BR002"])
    
    args_list = [
        ("BR001", "https://www.shuyiwrites.com/uploads/1/3/0/4/130438914/how_to_write_and_publish_a_scientific_paper.pdf", "./downloads", 30),
        ("BR002", "https://bcuathletics.com/documents/2026/3/1/BGSU3126.pdf", "./downloads", 30)
    ]
    
    # Mock network and file I/O, but let download_file run real logic
    with patch('download_files.requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.iter_content.return_value = [b'%PDF-1.4 test']
        mock_get.return_value = mock_response
        
        with patch('download_files.open', mock_open()):
            with patch('download_files.PdfReader') as mock_pdf:
                mock_pdf.return_value.pages = [1, 2]  # Mock pages
                
                # Actually call the orchestration function
                download_multiple_files(args_list, df2)
                
                # Verify results were written to DataFrame
                assert df2.at["BR001", "pdf_downloaded"] == "Downloaded"
                assert df2.at["BR002", "pdf_downloaded"] == "Downloaded"
                # Verify requests.get was called for each file
                assert mock_get.call_count == 2


if __name__ == "__main__":
    exit_code = pytest.main([__file__, "-v"])
    if exit_code == 0:
        print("\n✅ All tests passed!", flush=True)
    else:
        print("\n❌ Some tests failed!", flush=True)
    exit(exit_code)