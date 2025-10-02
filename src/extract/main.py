import os
import requests
from datetime import datetime

current_year = datetime.now().year

output_dir = f"G:/My Drive/ESTUDOS DATA SCIENCE/ie-employment-permit/data/{current_year}"
os.makedirs(output_dir, exist_ok=True)

base_url = "https://enterprise.gov.ie/en/publications/publication-files"
files = [
    f"permits-by-nationality-{current_year}.xlsx",
    f"permits-issued-to-companies-{current_year}.xlsx",
    f"permits-by-county-{current_year}.xlsx",
    f"permits-by-sector-{current_year}.xlsx",
]

for file in files:
    url = f"{base_url}/{file}"
    response = requests.get(url)

    if response.status_code == 200:
        with open(os.path.join(output_dir, file), "wb") as f:
            f.write(response.content)
        print(f"File downloaded: {file}")
    else:
        print(f"Failure during the process {file} - Status: {response.status_code}")
