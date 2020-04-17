# write_FIPS_from_Census.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
# ingwersen.wesley@epa.gov


"""
Grabs FIPS codes from static URL and creates crosswalk over the years.

- Shapes the set to include State and County names for all records.
- Writes reshaped file to datapath as csv.
"""

import pandas as pd
from flowsa.common import datapath, clean_str_and_capitalize

# 2017 State, County, Minor Civil Division, and Incorporated Place FIPS Codes
url = "https://www2.census.gov/programs-surveys/popest/geographies/2017/all-geocodes-v2017.xlsx"


if __name__ == '__main__':
    # Read directly into a pandas df
    raw_df = pd.read_excel(url)

    # skip the first few rows
    FIPS_df = pd.DataFrame(raw_df.loc[4:]).reindex()
    # Assign the column titles
    FIPS_df.columns = raw_df.loc[3, ]

    # new column of 2017  5 digit FIPS
    FIPS_df['FIPS_2017'] = FIPS_df['State Code (FIPS)'] + FIPS_df['County Code (FIPS)']

    # extract fips as new dataframe and drop duplicates
    df = pd.DataFrame(FIPS_df['FIPS_2017']).drop_duplicates()

    # drop FIPS outside US
    # df = df[~df['FIPS_17'].str[:2] == '72']

    ## modify columns depicting how counties have changed over the years - starting 2010
    # 2019 one FIPS code deleted and split into two FIPS
    df_19 = pd.DataFrame(df['FIPS_2017'])
    df_19['FIPS_2019'] = df_19['FIPS_2017']
    df_19.loc[df_19['FIPS_2019'] == "02261", 'FIPS_2019'] = "02063"
    df_19 = df_19.append(pd.DataFrame([["02261", "02066"]], columns=df_19.columns))

    # 2015 had two different/renamed fips
    df_15 = pd.DataFrame(df['FIPS_2017'])
    df_15['FIPS_2015'] = df_15['FIPS_2017']
    df_15.loc[df_15['FIPS_2015'] == "02158", 'FIPS_2015'] = "02270"
    df_15.loc[df_15['FIPS_2015'] == "46102", 'FIPS_2015'] = "46113"

    # # 2013 had a fips code that was merged with an existing fips
    df_13 = pd.DataFrame(df_15["FIPS_2015"])
    df_13['FIPS_2013'] = df_13['FIPS_2015']
    df_13 = df_13.append(pd.DataFrame([["51019", "51515"]], columns=df_13.columns))

    # merge 2013 with 2015 dataframe
    df_xwalk = pd.merge(df_13, df_15, on="FIPS_2015")
    # merge 2019 with 2017
    df_xwalk2 = pd.merge(df_19, df_xwalk, on="FIPS_2017")

    # create columns for remaining years and reorder
    df_xwalk2['FIPS_2014'] = df_xwalk2['FIPS_2013']
    df_xwalk2['FIPS_2016'] = df_xwalk2['FIPS_2015']
    df_xwalk2['FIPS_2018'] = df_xwalk2['FIPS_2017']
    df_xwalk2 = df_xwalk2[['FIPS_2013', 'FIPS_2014', 'FIPS_2015', 'FIPS_2016',
                           'FIPS_2017', 'FIPS_2018', 'FIPS_2019']]

    # write fips crosswalk as csv
    df_xwalk2.to_csv(datapath+"Crosswalk_FIPS.csv", index=False)
