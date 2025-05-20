# write_Crosswalk_NAPCS.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
Create a crosswalk for NAPCS to NAICS.
"""
import pandas as pd
from flowsa.settings import datapath
from scripts.FlowByActivity_Crosswalks.common_scripts import order_crosswalk


if __name__ == '__main__':

    cw = (pd.read_csv(datapath / 'NAICS_to_NAPCS_Crosswalk_2017.csv')
          .filter(['NAPCS_2017_Code', 'NAPCS_2017_Name', 'NAICS_2017_Code'])
          .rename(columns={'NAPCS_2017_Code':'Activity',
                           'NAICS_2017_Code': 'Sector',
                           'NAPCS_2017_Name':'Note'})
          .drop_duplicates()
          .assign(ActivitySourceName = 'NAPCS_2017')
          .assign(SectorSourceName = 'NAICS_2017_Code')
          .assign(SectorType = '')
          )

    df = order_crosswalk(cw)
    # save as csv
    df.to_csv(f"{datapath}/activitytosectormapping/NAICS_Crosswalk_NAPCS_2017.csv",
              index=False)

