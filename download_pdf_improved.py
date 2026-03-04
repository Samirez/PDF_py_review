# -*- coding: utf-8 -*-
"""
Created on Sun Oct 13 15:37:08 2019
@author: hewi

"""

# IF error : "ModuleNotFoundError: no module named pypdf"
# then uncomment line below (i.e. remove the #):

# pip install pypdf pandas requests openpyxl tqdm


import pandas as pd
from pypdf import PdfReader
import os
import glob
import logging
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from dataclasses import dataclass
from typing import Optional

#### Config - edit section to fit your needs ####
CONFIG = {
    # path to Excel file with URLs
    "list_pth": r"C:\Users\SPAC-O-6\Desktop\PDF_py\GRI_2017_2020 (1).xlsx",
    # base path for downloads and logs - downloaded PDFs will be saved in a
    # "dwn" subfolder, logs will be saved in the base path
    "pth": r"C:\Users\SPAC-O-6\Desktop\PDF_py",
    "ID": "BRnum",
    "url_column": "Pdf_URL",  # column AL
    "other_url_column": "Report HTML Address",  # column AM
    "max_workers": 10,  # parallel downloads
    "download_timeout": 30,  # seconds before a download is considered failed
    "Prototype": False,  # if True, only download first 10 files for testing
    "Prototype_count": 100,  # number of files to download in prototype mode
}
########################################################
####### Don't edit below this line unless you want to improve the code #######
########################################################

### Set up logging ###
logger = logging.getLogger(__name__)

### SoC: don't write your program as one solid block, instead, break up the code into chunks that are finalized tiny pieces of the system ####
# Classes and functions should be defined at the top, then the main
# execution code should be at the bottom, ideally in a main() function.
# This way, you can test and debug individual pieces of the code without
# having to run the entire program.


# Define Classes
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
    # the URL that was actually used for the download attempt (either from
    # url_column or other_url_column)
    url_used: str
    error: Optional[str]


# Define functions
def check_col_for_url(list_pth, ID, url_column, other_url_column):
    df = pd.read_excel(list_pth, sheet_name=0, index_col=ID)
    if url_column not in df.columns:
        df[url_column] = None

    if other_url_column in df.columns:
        df[url_column] = df[url_column].fillna(df[other_url_column])

    return df


def check_existing_files(dwn_pth):
    """Return list of existing PDF basenames without .pdf extension."""
    dwn_files = glob.glob(os.path.join(dwn_pth, "*.pdf"))
    exist = [os.path.splitext(os.path.basename(f))[0] for f in dwn_files]
    return exist


def check_if_valid_pdf(savefile):
    # PDF validation
    try:
        with open(savefile, "rb") as f:
            reader = PdfReader(f)
            return len(reader.pages) > 0
    except Exception:
        return False


def download_file(task):
    # task = DownloadTask(**task)  # Convert dict to DownloadTask object
    savefile = os.path.join(task.output_dir, str(task.brnum) + ".pdf")

    urls_to_try = [task.url_column]
    if task.other_url_column:
        urls_to_try.append(task.other_url_column)

    last_error = None

    for url in urls_to_try:
        try:
            response = requests.get(url, stream=True, timeout=task.timeout)
            response.raise_for_status()

            with open(savefile, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        # The `chunk_size=8192` parameter helps manage memory
                        # usage when downloading large files by reading the
                        # content in smaller pieces.
                        file.write( chunk )
            # PDF validation
            if check_if_valid_pdf(savefile):
                return DownloadResult(
    brnum=task.brnum,
    status="Downloaded",
    url_used=url,
     error=None )
            else:
                last_error = f"Downloaded - but PDF is invalid: {savefile}"
        except Exception as e:
            last_error = str(e)
            logger.warning(
    f"Ikke downloaded {
        task.brnum} from {url} - {last_error}")
            continue

    return DownloadResult(
        brnum=task.brnum,
        status="Ikke downloaded",
        url_used=urls_to_try[-1],
        error=last_error,
    )


def download_multiple_files(tasks, df2, max_workers):

    # Download files using thread pool
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
    executor.submit(
        download_file,
         task): task.brnum for task in tasks}
        progress = tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Downloading PDFs",
            unit="file",
            leave=True,
        )
        for i, future in enumerate(progress, 1):
            brnum = futures[future]

            try:
                result: DownloadResult = future.result()

            except Exception as e:
                # brnum = futures[future]
                result = DownloadResult(
    brnum=brnum,
    status="Ikke downloaded",
    url_used="",
     error=str(e) )

            df2.loc[brnum, "pdf_downloaded"] = result.status
            df2.loc[brnum, "url_used"] = result.url_used
            if result.error:
                df2.loc[result.brnum, "download_error"] = result.error
            progress.set_postfix_str(f"{result.brnum}: {result.status}")
            logger.info(f"[{i}/{len(tasks)}] {result.brnum}: {result.status}")
    return df2


def init_logging_and_dirs():
    pth = CONFIG["pth"]
    os.makedirs(pth, exist_ok=True)

    # Make logging initialization idempotent by checking if handlers already
    # exist
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s")

        # Add file handler
        file_handler = logging.FileHandler(
            os.path.join(pth, "download_log_improved.log"))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Add console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)


def main():
    init_logging_and_dirs()
    pth = CONFIG["pth"]
    dwn_pth = os.path.join(pth, "dwn")
    os.makedirs(dwn_pth, exist_ok=True)

    logger.info("Starting PDF download process")
    logger.info(
        f"Prototype: {CONFIG['Prototype']} - only downloading first {CONFIG['Prototype_count']} files for testing"
        if CONFIG["Prototype"]
        else "Downloading all files"
    )

    df = check_col_for_url(
        list_pth=CONFIG["list_pth"],
        ID=CONFIG["ID"],
        url_column=CONFIG["url_column"],
        other_url_column=CONFIG["other_url_column"],
    )
    df = df[
        df[CONFIG["url_column"]].notnull()
    ]  # Filter to only rows where we have a URL to try
    if df.empty:
        logger.error("No valid URLs found in the specified columns")
        return
    df2 = df.copy()  # This will hold the download status for each brnum

    exist = check_existing_files(dwn_pth)
    df2 = df2[
        ~df2.index.astype(str).isin(exist)
    ]  # Filter out rows where the file already exists

    # Prototype mode: only download first N files for testing
    if CONFIG["Prototype"]:
        df2 = df2.head(CONFIG["Prototype_count"])
        logger.info(
    f"Prototype mode: only downloading first {
        CONFIG['Prototype_count']} files for testing" )

    tasks = [
        DownloadTask(
            brnum=brnum,
            url_column=df2.at[brnum, CONFIG["url_column"]],
            other_url_column=(
                df2.at[brnum, CONFIG["other_url_column"]]
                if CONFIG["other_url_column"] in df2.columns
                else None
            ),
            output_dir=dwn_pth,
            timeout=CONFIG["download_timeout"],
        )
        for brnum in df2.index
    ]

    df2 = download_multiple_files(
    tasks, df2, max_workers=CONFIG["max_workers"])

    print(f"Downloaded:     {(df2['pdf_downloaded'] == 'Downloaded').sum()}")
    print(f"Not downloaded: {(df2['pdf_downloaded'] != 'Downloaded').sum()}")
    logger.info(
        f"Downloaded:     {(df2['pdf_downloaded'] == 'Downloaded').sum()}")
    logger.info(
        f"Not downloaded: {(df2['pdf_downloaded'] != 'Downloaded').sum()}")

    log_path = os.path.join(pth, "download_log_improved.xlsx")
    df2.to_excel(log_path)
    logger.info(f"Log saved to: {log_path}")


if __name__ == "__main__":
    main()
