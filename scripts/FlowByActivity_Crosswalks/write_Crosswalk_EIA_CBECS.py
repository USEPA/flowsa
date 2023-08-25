# write_Crosswalk_EIA_CBECS.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
Download the crosswalk from CBECS to NAICS that CBECS publishes and reshape

Script creates crosswalks for Land and Water

"""

import io
import pandas as pd
from esupy.remote import make_url_request
from flowsa.settings import datapath
from flowsa.data_source_scripts.EIA_CBECS_Land import standardize_eia_cbecs_land_activity_names

if __name__ == '__main__':

    # url for excel crosswalk
    url = 'http://www.eia.gov/consumption/commercial/data/archive/cbecs/PBAvsNAICS.xls'
    # make url requestl, as defined in common.py
    r = make_url_request(url)
    # Convert response to dataframe, skipping first three rows
    df_raw = pd.read_excel(io.BytesIO(r.content), skiprows=3)

    # Rename first column to sector (naics 2002)
    df = df_raw.rename(columns={df_raw.columns[0]: "Sector"})

    # remove row of just NAs
    df = df[df['Sector'].notna()]

    # remove description in first column
    df['Sector'] = df['Sector'].str.split('/').str[0]

    # reshape data to long format and name columns
    df = pd.melt(df, id_vars=['Sector'])
    df.columns = ['Sector', 'Activity', 'value']

    # remove all rows where the crosswalk is null and drop value column
    df = df[df['value'].notna()].drop(columns=['value']).reset_index(drop=True)

    # Add 'vacant', which is missing from crosswalk,
    # but included in imported data. Will associate vacant with all CBECS NAICS
    sec_list = df['Sector'].drop_duplicates().values.tolist()
    for s in sec_list:
        df = pd.concat([df, pd.DataFrame(
            [[s, 'Vacant']], columns=['Sector', 'Activity'])],
             ignore_index=True, sort=False)

    # the original dataset is for NAICS 2002, but 3 digit NAICS
    # have not changed between 2002 and 2012, so labeling 2012
    df['SectorSourceName'] = "NAICS_2012_Code"
    df['SectorType'] = "I"

    # create crosswalks for water and land
    resource = ['Water', 'Land']
    for r in resource:
        # Add additional columns
        df['ActivitySourceName'] = "EIA_CBECS_" + r

        if r == 'Land':
            # standarize activity names to match those in FBA
            df = standardize_eia_cbecs_land_activity_names(df, 'Activity')

        # reorder and drop columns
        df = df[['ActivitySourceName', 'Activity', 'SectorSourceName',
                 'Sector', 'SectorType']]

        # sort df
        df = df.sort_values(['Activity', 'Sector']).reset_index(drop=True)

        # save as csv
        df.to_csv(f"{datapath}/activitytosectormapping/"
                  f"NAICS_Crosswalk_EIA_CBECS_{r}.csv", index=False)
