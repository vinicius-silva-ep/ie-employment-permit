import os
import requests
from datetime import datetime

current_year = datetime.now().year

output_dir = f"G:/My Drive/ESTUDOS DATA SCIENCE/ie-employment-permit/data/raw_data/{current_year}"
os.makedirs(output_dir, exist_ok=True)

base_url = "https://assets.gov.ie/static/documents" # Different base URL for the files, as they are stored in a different location on the server
files = [
    f"686d767a/employment-permits-by-nationality-{current_year}_1.xlsx", # Note: The file names on the server have a "_1" suffix, which we will remove when saving the files locally
    f"f24ad3ec/employment-permits-issued-to-companies-{current_year}_1.xlsx",
    f"11a96607/employment-permits-by-county-{current_year}_1.xlsx",
    f"94740580/employment-permits-by-sector-{current_year}_1.xlsx",
]

for file in files:
    url = f"{base_url}/{file}"
    response = requests.get(url)

    if response.status_code == 200:
        original_name = os.path.basename(file)
        base, ext = os.path.splitext(original_name)
        if base.endswith("_1"): # Remove the "_1" suffix if it exists 
            base = base[:-2]
        if base.startswith("employment-"): # Remove the "employment-" prefix if it exists
            base = base[len("employment-"):]
        filename = f"{base}{ext}"
        output_path = os.path.join(output_dir, filename)
        with open(output_path, "wb") as f:
            f.write(response.content)
        print(f"File downloaded: {filename}")
    else:
        print(f"Failure during the process {file} - Status: {response.status_code}")
