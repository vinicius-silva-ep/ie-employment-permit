import os
import pandas as pd
from datetime import datetime

def transform_county():
    current_year = datetime.now().year
    year = current_year

    # Reading the raw data in the folder on my personal computer
    df = pd.read_excel(
        f"G:/My Drive/ESTUDOS DATA SCIENCE/ie-employment-permit/data/raw_data/{year}/permits-by-county-{year}.xlsx",
        header=0,
    )
    print(df)

    # As the structure of the columns were modified after 2020, I had to rename automatically the first one by "County"
    df.rename(columns={df.columns[0]: "County"}, inplace=True)

    # Removing total row because the project stores one row per county
    df = df[df["County"] != "Grand Total"].copy()

    # Creating a new column because of the new structure where the year is on top of the table
    df["Year"] = year

    # Sorting the year column to be the first one 
    df = df[["Year"] + [col for col in df.columns if col != "Year"]]

    # Ensure Withdrawn column exists for consistency
    if "Withdrawn" not in df.columns:
        df["Withdrawn"] = 0

    # Reordering columns
    cols = ["Year", "County", "Issued", "Refused", "Withdrawn"]
    df = df[[c for c in cols if c in df.columns]]

    # Creates the folder if it doesnt exist
    output_dir = f"G:/My Drive/ESTUDOS DATA SCIENCE/ie-employment-permit/data/{year}"
    os.makedirs(output_dir, exist_ok=True)

    # Save as csv
    csv_path = os.path.join(output_dir, f"permits-issued-by-county-{year}.csv")
    df.to_csv(csv_path, index=False)

    print(df)
    return df

if __name__ == "__main__":
    transform_county()