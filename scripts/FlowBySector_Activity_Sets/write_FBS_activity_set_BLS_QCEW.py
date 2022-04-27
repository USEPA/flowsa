# write_FBS_activity_set_BLS_QCEW.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
"""
Create an activity set file file employment data. Script only needs to be
run for additional years if there are new NAICS.
"""

import pandas as pd
import flowsa
from flowsa.settings import flowbysectoractivitysetspath


def main():
    years = ['2002', '2010', '2011', '2012', '2013', '2014', '2015', '2016',
             '2017']
    # define  fba parameters
    datasource = 'BLS_QCEW'
    # empty df
    df2 = pd.DataFrame()
    for y in years:
        # Load FBS
        df_import = flowsa.getFlowByActivity(datasource, y)
        # drop unused columns
        df = df_import[['ActivityProducedBy']].drop_duplicates().reset_index(
            drop=True)
        # rename columns
        df = df.rename(columns={"ActivityProducedBy": "name"})
        # assign column values
        df = df.assign(activity_set='activity_set_1')
        df = df.assign(note='')
        # reorder dataframe
        df = df[['activity_set', 'name', 'note']]
        # concat together
        df2 = pd.concat([df2, df], ignore_index=True)
    # drop duplicates and save df
    df3 = df2.drop_duplicates()
    df3 = df3.sort_values(['activity_set', 'name'])
    df3.to_csv(f"{flowbysectoractivitysetspath}{datasource}_asets.csv",
               index=False)


if __name__ == '__main__':
    main()
