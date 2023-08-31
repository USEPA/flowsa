# write_Division_Codes_from_Census.py (scripts)
# !/usr/bin/env python3
# coding=utf-8


"""
Grabs Census Region and Division codes from a static URL.

- Writes reshaped file to datapath as csv.
"""

import pandas as pd
import numpy as np
from flowsa.settings import datapath

url = "https://www2.census.gov/programs-surveys/popest/geographies/2017/state-geocodes-v2017.xlsx"

if __name__ == '__main__':
    # Read directly into a pandas df,
    raw_df = pd.read_excel(url)

    # skip the first few rows
    df = pd.DataFrame(raw_df.loc[5:]).reset_index(drop=True)
    # Assign the column titles
    df.columns = raw_df.loc[4, ]

    # assign location system
    df['LocationSystem'] = np.where(df["Name"].str.contains("Region"),
                                    "Census_Region", None)
    df['LocationSystem'] = np.where(df["Name"].str.contains("Division"),
                                    "Census_Division", df['LocationSystem'])

    # rename columns to match flowbyactivity format
    df = df.rename(columns={"State (FIPS)": "State_FIPS"})

    df.to_csv(f"{datapath}/Census_Regions_and_Divisions.csv", index=False)
