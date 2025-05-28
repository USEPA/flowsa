# write_Crosswalk_NAPCS.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
Create a crosswalk for NAPCS to NAICS for wholesaleling and retailing based on
the highest value wholesale and retail sector identified
"""
import pandas as pd
import flowsa
from flowsa.common import load_crosswalk
from flowsa.settings import datapath


if __name__ == '__main__':

    fba = flowsa.getFlowByActivity('Census_EC_PxI', 2017)
    napcs = ('4', '5')
    sectors = ('42', '44', '45')
    result = (fba
              .query('ActivityProducedBy != "00" and '
                     'FlowName.str.startswith(@napcs)')
              .query('ActivityProducedBy.str.startswith(@sectors)')
              .reset_index(drop=True)
              )

    # Find the sector with the greatest prevalence of that NAPCS code to assign mapping
    result = (result.loc[result.groupby('FlowName')['FlowAmount'].idxmax()]
              .filter(['FlowName', 'Description', 'ActivityProducedBy'])
              .sort_values(by='FlowName')
              .reset_index(drop=True)
              .merge(load_crosswalk('Sector_2017_Names'),
                     left_on='ActivityProducedBy',
                     right_on='NAICS_2017_Code', how='left')
              )

    cw = (result
          .drop(columns=['NAICS_2017_Code'])
          .rename(columns={'FlowName':'Activity',
                           'ActivityProducedBy': 'Sector',
                           'Description':'NAPCS_Description'})
          .assign(ActivitySourceName = 'NAPCS_2017')
          .assign(SectorSourceName = 'NAICS_2017_Code')
          .assign(SectorType = '')
          )
    col_order = ['ActivitySourceName'] + \
                ['Activity', 'SectorSourceName', 'Sector', 'SectorType'] + \
                ['NAPCS_Description', 'NAICS_2017_Name']

    df = cw[col_order]

    # save as csv
    df.to_csv(f"{datapath}/activitytosectormapping/NAICS_Crosswalk_NAPCS_primary_wholesale_2017.csv",
              index=False)

