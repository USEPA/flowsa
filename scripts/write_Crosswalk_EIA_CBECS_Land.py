# write_Crosswalk_EIA_CBECS_Land.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
Download the crosswalk from CBECS to NAICS that CBECS publishes and reshape

"""
import pandas as pd
import io
from flowsa.common import datapath, make_http_request
from flowsa.EIA_CBECS_Land import standardize_eia_cbecs_land_activity_names

if __name__ == '__main__':
    # url for excel crosswalk
    url = 'http://www.eia.gov/consumption/commercial/data/archive/cbecs/PBAvsNAICS.xls'
    # make url requestl, as defined in common.py
    r = make_http_request(url)
    # Convert response to dataframe, skipping first three rows
    df_raw = pd.io.excel.read_excel(io.BytesIO(r.content), skiprows=3)

    # Rename first column to sector (naics 2002)
    df = df_raw.rename(columns={df_raw.columns[0]: "Sector"})

    # remove row of just NAs
    df = df[df['Sector'].notna()]

    # remove description in first column
    df['Sector'] = df['Sector'].str.split('/').str[0]

    # reshape data to long format and name columns
    df = pd.melt(df, id_vars=['Sector'])
    df.columns = ['Sector', 'Activity', 'value']

    # remove all rows where the crosswalk is null
    df = df[df['value'].notna()]

    # Add additional columns
    df['ActivitySourceName'] = "EIA_CBECS_Land"
    # the original dataset is for NAICS 2002, but 3 digit NAICS have not changed between 2002 and 2012, so labeling 2012
    df['SectorSourceName'] = "NAICS_2012_Code"
    df['SectorType'] = "I"

    # standarize activity names to match those in FBA
    df = standardize_eia_cbecs_land_activity_names(df, 'Activity')

    # reorder and drop columns
    df = df[['ActivitySourceName', 'Activity', 'SectorSourceName', 'Sector', 'SectorType']]

    # sort df
    df = df.sort_values(['Activity'])

    # reset index
    df.reset_index(drop=True, inplace=True)




    # save as csv
    df.to_csv(datapath + "activitytosectormapping/" + "Crosswalk_EIA_CBECS_Land_toNAICS.csv", index=False)

