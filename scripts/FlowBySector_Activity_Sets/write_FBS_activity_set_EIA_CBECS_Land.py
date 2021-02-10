# write_FBS_activity_set_BLM_PLS.py (scripts)
# !/usr/bin/env python3
# coding=utf-8


"""
Write the csv called on in flowbysectormethods yaml files for land use related EIA CBECS
"""

import flowsa
from flowsa.common import flowbysectoractivitysetspath

as_year = '2012'

if __name__ == '__main__':

    # define mecs land fba parameters
    land_flowclass = ['Land']
    land_years = [as_year]
    datasource = 'EIA_CBECS_Land'

    # Read BLM PLS crosswalk
    df_import = flowsa.getFlowByActivity(land_flowclass, land_years, datasource)

    # drop unused columns
    df = df_import[['ActivityConsumedBy']].drop_duplicates()
    # drop 'all buildings' to avoid double counting
    df = df[~df['ActivityConsumedBy'].isin(['All buildings', 'Mercantile', 'Health care'])]

    # rename columns
    df = df.rename(columns={"ActivityConsumedBy": "name"})

    # assign column values
    df = df.assign(activity_set='activity_set_1')
    df = df.assign(note='')

    # reorder dataframe
    df = df[['activity_set', 'name', 'note']]
    df = df.sort_values(['activity_set', 'name']).reset_index(drop=True)

    # save df
    df.to_csv(flowbysectoractivitysetspath + datasource + '_' + as_year + "_asets.csv", index=False)
