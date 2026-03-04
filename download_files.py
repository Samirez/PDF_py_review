# -*- coding: utf-8 -*-
"""
Created on Sun Oct 13 15:37:08 2019
@author: hewi

PDF Downloader - GRI 2017-2020 (1)
Downloads PDF reports from URLs in Excel file.
Produces a log showing downloaded / not downloaded status.
"""

# IF error : "ModuleNotFoundError: no module named pypdf"# then uncomment
# line below (i.e. remove the #):

# pip install pypdf


import pandas as pd
from pypdf import PdfReader
import os
import socket
import glob
import requests
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

socket.setdefaulttimeout(15)  # Global timeout for all URL requests

###!!NB!! column with URL's should be called: "Pdf_URL" and the year should be in column named: "Pub_Year"
# File names will be the ID from the ID column (e.g. BR2005.pdf)

# EDIT PATH HERE:

# specify path to file containing the URLs
list_pth = "C:\\Users\\SPAC-O-9\\OneDrive - Specialisterne\\Dokumenter\\Specialisterne_kursus\\PDF_downloader_uge_5\\PDF_py_review\\GRI_2017_2020 (1).xlsx"
# specify Output folder (in this case it moves one folder up and saves in
# the script output folder)
pth = "C:\\Users\\SPAC-O-9\\OneDrive - Specialisterne\\Dokumenter\\Specialisterne_kursus\\PDF_downloader_uge_5\\PDF_py_review\\Data\\"
# Download subfolder (PDFs saved here)
dwn_pth = os.path.join(pth, "dwn")

# specify the ID column name
ID = "BRnum"

##########

# Download function


def download_file(args):
    brnum, url, output_dir, timeout = args
    savefile = os.path.join(output_dir, str(brnum) + ".pdf")

    try:
        response = requests.get(url, stream=True, timeout=timeout)
        response.raise_for_status()

        with open(savefile, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    # The `chunk_size=8192` parameter helps manage memory usage
                    # when downloading large files by reading the content in
                    # smaller pieces.
                    file.write( chunk )
        # PDF validation
        try:
            with open(savefile, "rb") as f:
                reader = PdfReader(f)
                if len(reader.pages) > 0:
                    return brnum, "Downloaded", None
                else:
                    os.remove(savefile)
                    return brnum, "Ikke downloaded", "PDF has no pages"

        except Exception as e:
            if os.path.exists(savefile):
                os.remove(savefile)
            return brnum, "Ikke downloaded", str(e)
    except Exception as e:
        if os.path.exists(savefile):
            os.remove(savefile)
        return brnum, "Ikke downloaded", str(e)


def download_multiple_files(args_list, df2):
    # Download files using thread pool
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(download_file, args): args[0] for args in args_list
        }
        progress = tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Downloading PDFs",
            unit="file",
            leave=True,
        )
        for i, future in enumerate(progress, 1):
            try:
                brnum, status, error = future.result(timeout=90)
                # Here you can log the result (e.g., update df2 with status)
            except concurrent.futures.TimeoutError:
                brnum = futures[future]
                status = "Ikke downloaded"
                # Note: future.cancel() is ineffective here since the task is already running;
                # status and error are set to indicate timeout to the caller, and the task
                # will continue executing in the background or be cleaned up
                # when the pool shuts down
                error = "timeout"
            except Exception as e:
                brnum = futures[future]
                status = "Ikke downloaded"
                error = str(e)
            df2.at[brnum, "pdf_downloaded"] = status
            if error:
                df2.at[brnum, "download_error"] = error
            progress.set_postfix_str(f"{brnum}: {status}")


def main():
    # Create download folder if it doesn't exist
    os.makedirs(dwn_pth, exist_ok=True)

    # check for files already downloaded
    dwn_files = glob.glob(os.path.join(dwn_pth, "*.pdf"))
    exist = [os.path.basename(f)[:-4] for f in dwn_files]

    # read in file
    df = pd.read_excel(list_pth, sheet_name=0, index_col=ID)

    # Filter out rows with no URL, check Report HTML Address if Pdf_URL is
    # missing
    if "Pdf_URL" not in df.columns:
        df["Pdf_URL"] = None

    if "Report HTML Address" in df.columns:
        df["Pdf_URL"] = df["Pdf_URL"].fillna(
            df["Report HTML Address"]
        )  # map it to Pdf_URL so rest of script works
    df = df[df["Pdf_URL"].notnull()]

    if df.empty:
        print("No URL column found or no URLs present — please check your Excel file")
        return

    df2 = df.copy()

    # filter out rows that have been downloaded
    df2 = df2[~df2.index.isin(exist)]
    print(f"{len(df2)} files to download, {len(exist)} already downloaded.")

    args_list = [
        (brnum, df2.at[brnum, "Pdf_URL"], dwn_pth, 15) for brnum in df2.index
    ]

    if args_list:
        download_multiple_files(args_list, df2)
        if 'pdf_downloaded' in df2.columns:
            print(
                f"Downloaded:     {(df2['pdf_downloaded'] == 'Downloaded').sum()}")
            print(
                f"Not downloaded: {(df2['pdf_downloaded'] != 'Downloaded').sum()}")
        else:
            print(
                "Warning: pdf_downloaded column was not created. Unable to report statistics.")
    else:
        print("No new files to download.")

    log_path = os.path.join(pth, "download_log.xlsx")
    df2.to_excel(log_path)


if __name__ == "__main__":
    main()
