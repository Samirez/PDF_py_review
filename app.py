# -*- coding: utf-8 -*-
"""

PDF Downloader - GRI 2017-2020 (1)
Downloads PDF reports from URLs in Excel file.
"""

#### IF error : "ModuleNotFOundError: no module named PyPDF2"
# then uncomment line below (i.e. remove the #):

# pip install PyPDF2 pandas tqdm aiohttp aiofiles tqdm-asyncio aiofiles


import aiofiles
import aiohttp
import asyncio
from tqdm.asyncio import tqdm


import pandas as pd
import PyPDF2
import os
import socket
import glob
import logging

from dataclasses import dataclass
from typing import Optional

#### Config - edit section to fit your needs ####
CONFIG = {
    #"list_pth": "/data/GRI_2017_2020 (1).xlsx", # path to Excel file with URLs
    #"pth": "/data", # base path for downloads and logs - downloaded PDFs will be saved in a "dwn" subfolder, logs will be saved in the base path
    "list_pth": r"C:\Users\SPAC-O-6\Desktop\PDF_py\GRI_2017_2020 (1).xlsx", # path to Excel file with URLs
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
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("download_log_async.log"),  # Log to file
        logging.StreamHandler(),  # Log to console
    ],
)

logger = logging.getLogger(__name__)

### Define Classes
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


### Define functions
def check_col_for_url(list_pth, ID, url_column, other_url_column):
    df = pd.read_excel(list_pth, sheet_name=0, index_col=ID)
    if url_column not in df.columns:
        df[url_column] = None

    if other_url_column in df.columns:
        df[url_column] = df[url_column].fillna(df[other_url_column])

    return df


def check_exiting_files(dwn_pth):
    dwn_files = glob.glob(os.path.join(dwn_pth, "*.pdf"))
    exist = [os.path.basename(f)[:-4] for f in dwn_files]
    return exist


def check_if_valid_pdf(savefile):
    # PDF validation
    try:
        with open(savefile, "rb") as f:
            reader = PyPDF2.PdfReader(f)
            return len(reader.pages) > 0
    except Exception:
        return False
    
async def download_file(task, sess, sem):
    
    savefile = os.path.join(task.output_dir, str(task.brnum) + ".pdf")

    urls_to_try = [task.url_column]
    if task.other_url_column:
        urls_to_try.append(task.other_url_column)

    last_error = None

    for url in urls_to_try:
        try:
            async with sem:
                timeout = aiohttp.ClientTimeout(total=task.timeout)
                async with sess.get(url, timeout=timeout) as response:
                    response.raise_for_status() # Check if the request was successful (status code 200)
                    async with aiofiles.open(savefile, "wb") as file:
                        async for chunk in response.content.iter_chunked(8192):
                            if chunk:
                                await file.write(chunk)
                        
            # The `chunk_size=8192` parameter helps manage memory usage when downloading large files by reading the content in smaller pieces.
            # PDF validation
            # Validation is done in a separate thread to avoid blocking the event loop, since PyPDF2 is not asynchonous.
            valid = await asyncio.get_running_loop().run_in_executor(
                None, check_if_valid_pdf, savefile
                )
            if valid:      
            
                return DownloadResult(
                    brnum=task.brnum, 
                    status="Downloaded", 
                    url_used=url, 
                    error=None
                )
            else:
                last_error = f"Downloaded - but PDF is invalid: {savefile}"
        except Exception as e:
            last_error = str(e)
            logger.warning(f"Ikke downloaded {task.brnum} from {url} - {last_error}")
            continue #try next URL if available

    return DownloadResult(
        brnum=task.brnum,
        status="Ikke downloaded",
        url_used="" if not urls_to_try else urls_to_try[-1],
        error=last_error,
    )
    
async def download_multiple_files(tasks, df2, max_workers):
    sem = asyncio.Semaphore(max_workers)
    connector = aiohttp.TCPConnector(limit=max_workers)
    
    async with aiohttp.ClientSession(connector=connector) as sess:
         coroutines = [
             download_file(task, sess, sem) 
             for task in tasks
             ]
         
         results = await tqdm.gather(
             *coroutines,
             total=len(coroutines),
             desc="Downloading PDFs",
             unit="file"
             #leave=True,
         )
         for i, result in enumerate(results, 1):
           

             df2.loc[result.brnum, "pdf_downloaded"] = result.status
             df2.loc[result.brnum, "url_used"] = result.url_used
             if result.error:
                 df2.loc[result.brnum, "download_error"] = result.error
             logger.info(f"[{i}/{len(tasks)}] {result.brnum}: {result.status}")
    return df2

    
async def main():
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

    exist = check_exiting_files(dwn_pth)
    df2 = df2[
        ~df2.index.astype(str).isin(exist)
    ]  # Filter out rows where the file already exists
    

    # Prototype mode: only download first N files for testing
    if CONFIG["Prototype"]:
        df2 = df2.head(CONFIG["Prototype_count"])
        logger.info(
            f"Prototype mode: only downloading first {CONFIG['Prototype_count']} files for testing"
        )

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

    df2 = await download_multiple_files(tasks, df2, max_workers=CONFIG["max_workers"])
   
    print(f"Downloaded:     {(df2['pdf_downloaded'] == 'Downloaded').sum()}")
    print(f"Not downloaded: {(df2['pdf_downloaded'] != 'Downloaded').sum()}")
    logger.info(f"Downloaded:     {(df2['pdf_downloaded'] == 'Downloaded').sum()}")
    logger.info(f"Not downloaded: {(df2['pdf_downloaded'] != 'Downloaded').sum()}")
    
    log_path = os.path.join(pth, "download_log_async.xlsx")
    df2.to_excel(log_path)
    logger.info(f"Log saved to: {log_path}")

    

if __name__ == "__main__":
    asyncio.run(main())
