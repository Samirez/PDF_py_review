# -*- coding: utf-8 -*-
"""
Created on Sun Oct 13 15:37:08 2019
@author: hewi

PDF Downloader - GRI 2017-2020 (1)
Downloads PDF reports from URLs in Excel file.
Produces a log showing downloaded / not downloaded status.
"""

#### IF error : "ModuleNotFOundError: no module named PyPDF2"
   # then uncomment line below (i.e. remove the #):
       
#pip install PyPDF2

import pandas as pd
import PyPDF2
import os
import os.path
import urllib.error
import urllib.request
import glob
from concurrent.futures import ThreadPoolExecutor, as_completed

###!!NB!! column with URL's should be called: "Pdf_URL" and the year should be in column named: "Pub_Year"

### File names will be the ID from the ID column (e.g. BR2005.pdf)

########## EDIT HERE:
    
### specify path to file containing the URLs
list_pth = "C:\\Users\\SPAC-O-6\\Downloads\\Data\\Data\\GRI_2017_2020 (1).xlsx"

###specify Output folder (in this case it moves one folder up and saves in the script output folder)
pth = 'C:\\Users\\SPAC-O-6\\Downloads\\Data\\Data\\'

# Download subfolder (PDFs saved here)
dwn_pth = os.path.join(pth, "dwn")
# Create download folder if it doesn't exist
os.makedirs(dwn_pth, exist_ok=True)

### cheack for files already downloaded
dwn_files = glob.glob(os.path.join(dwn_pth, "*.pdf")) 
exist = [os.path.basename(f)[:-4] for f in dwn_files]

###specify the ID column name
ID = "BRnum"


##########

### read in file
df = pd.read_excel(list_pth, sheet_name=0, index_col=ID)

## Filter out rows with no URL, check Report HTML Address if Pdf_URL is missing

if "Pdf_URL" not in df.columns:
    df["Pdf_URL"] = None
    
if "Report HTML Address" in df.columns:
    df["Pdf_URL"] = df["Pdf_URL"].fillna(df["Report HTML Address"])  # map it to Pdf_URL so rest of script works
df = df[df["Pdf_URL"].notnull()]

if df.empty:
    print("No URL column found or no URLs present — please check your Excel file")    
else:
    df2 = df.copy()  

    ### filter out rows that have been downloaded
    df2 = df2[~df2.index.isin(exist)]

    def download_file(j):
        url = df2.at[j, 'Pdf_URL']
        savefile = os.path.join(dwn_pth, str(j) + '.pdf')
        
        try:
            urllib.request.urlretrieve(url, savefile)
            try:
                with open(savefile, 'rb') as f:
                            pdfReader = PyPDF2.PdfReader(f)
                            if len(pdfReader.pages) > 0:  
                                df2.at[j, 'pdf_downloaded'] = "Downloaded"
                                return j, "Downloaded", None
                            else:
                                return j, "Not downloaded - empty PDF", None
            except Exception as e:
                return j, "Not downloaded - invalid PDF", str(e)
                
        except (urllib.error.HTTPError, urllib.error.URLError,
                ConnectionResetError, Exception) as e:
            return j, "Not downloaded", str(e)        
    # Run downloads - change max_workers to download more files in parallel

    results = []
    with ThreadPoolExecutor(max_workers=2000) as executor:
        futures = {executor.submit(download_file, j): j for j in df2.index}
        for i, future in enumerate(as_completed(futures), 1):
            j, status, error = future.result()
            df2.at[j, 'pdf_downloaded'] = status
            if error:
                df2.at[j, 'error'] = error
            print(f"[{i}/{len(df2)}] {j}: {status}")




    # Log

    log_path = os.path.join(pth, 'download_log.xlsx')
    df2.to_excel(log_path)
    print(f"\nDone! Log saved to: {log_path}")
    print(f"Downloaded: {(df2['pdf_downloaded'] == 'Downloaded').sum()}")
    print(f"Not downloaded: {(df2['pdf_downloaded'] != 'Downloaded').sum()}")
