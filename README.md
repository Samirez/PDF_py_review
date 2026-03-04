### Multithreaded PDF Downloader (download_pdf_improved.py)
Python script for downloading PDF files in parallel from URLs stored in an Excel file.
This version uses multithreading via ThreadPoolExecutor. Also includes async version (app.py) using aiohttp.

##### Configuration
Edit [config.py](config.py) to customize the downloader behavior:
- `list_pth`: Path to Excel file with URLs (relative to script directory)
- `pth`: Base path for downloads and logs (relative to script directory)
- `ID`: Column name for unique identifier (default: "BRnum")
- `url_column`: Column with PDF URLs (default: "Pdf_URL")
- `other_url_column`: Fallback URL column when primary is empty
- `max_workers`: Number of parallel download threads (default: 10)
- `download_timeout`: Seconds before download is considered failed (default: 30)
- `prototype`: Enable prototype mode to limit downloads for testing (default: False)
- `prototype_count`: Number of files to download in prototype mode (default: 100)

##### Dependencies
`pip install -r requirements.txt`

Test dependencies are already included in requirements.txt (pinned versions):
- pytest==9.0.2
- pytest-asyncio==1.3.0
- pytest-cov==7.0.0


| Library            | Purpose                  |
| ------------------ | ------------------------ |
| pandas             | Read/write Excel         |
| pypdf              | Validate downloaded PDFs |
| requests           | HTTP downloads           |
| openpyxl           | Excel writing engine     |
| tqdm               | Progress bar             |
| aiohttp            | Async HTTP downloads     |
| aiofiles           | Async file operations    |
| pytest             | Testing framework        |
| pytest-cov         | Code coverage reporting  |

##### Architecture 

| Component                   | Responsibility                    |
| --------------------------- | --------------------------------- |
| `config.py`                 | Configuration settings            |
| `DownloadTask`              | Represents a single download job  |
| `DownloadResult`            | Represents download result        |
| `check_col_for_url()`       | Prepares and validates input data |
| `download_file()` / `fetch_pdf()` | Downloads and validates a PDF |
| `download_multiple_files()` | Executes downloads in parallel    |
| `main()`                    | Orchestrates full workflow        |

##### How It Works

**Using the Multithreaded Version:**
```
python download_pdf_improved.py
```

**Using the Async Version:**
```
python app.py
```

**Using Docker:**
```
docker-compose up
```

**Excel File Requirements:**
- `BRnum`: Unique identifier for each row
- `Pdf_URL`: Primary column with PDF URLs
- `Report HTML Address`: Fallback column if Pdf_URL is empty

**Workflow:**
1. Read configuration from [config.py](config.py)
2. Load input Excel file
3. Set BRnum as index
4. Fill missing URLs from fallback column
5. Remove rows without URLs
6. Skip already downloaded files
7. Enable prototype mode if needed (limited to `prototype_count` files)
8. Create Download Tasks (each row becomes a task object)
9. Parallel Execution:
   - ThreadPoolExecutor with `max_workers` threads
   - Each thread: downloads PDF in chunks, saves file, validates using pypdf
10. Save results to log files (Excel and text)

##### Testing

Run all tests:
```
pytest Testing/ -v --cov
```

Tests are configured via [pytest.ini](pytest.ini) and coverage via [.coveragerc](.coveragerc)



