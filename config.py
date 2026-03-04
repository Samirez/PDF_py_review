# -*- coding: utf-8 -*-
"""
Configuration file for PDF Downloader
Edit this file to customize the downloader behavior
"""

import os

# Base configuration
CONFIG = {
    # Use relative paths for cross-platform and Docker compatibility
    "list_pth": os.path.join(os.path.dirname(__file__), "GRI_2017_2020 (1).xlsx"),  # path to Excel file with URLs
    # base path for downloads and logs - downloaded PDFs will be saved in a
    # "dwn" subfolder, logs will be saved in the base path
    "pth": os.path.dirname(__file__),
    "ID": "BRnum",  # Column name for unique identifier
    "url_column": "Pdf_URL",  # column AL
    "other_url_column": "Report HTML Address",  # column AM
    "max_workers": 10,  # parallel downloads
    "download_timeout": 30,  # seconds before a download is considered failed
    "Prototype": False,  # if True, only download first 10 files for testing
    "Prototype_count": 100,  # number of files to download in prototype mode
}
