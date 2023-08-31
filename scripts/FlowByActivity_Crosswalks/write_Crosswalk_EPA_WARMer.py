# write_Crosswalk_EPA_WARMer.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
"""
Create a crosswalk for EPA_WARMer from WARM processes.
"""

import pandas as pd
from flowsa.common import load_yaml_dict
from flowsa.settings import datapath
from scripts.FlowByActivity_Crosswalks.common_scripts import order_crosswalk

if __name__ == '__main__':
    datasource = 'EPA_WARMer'

    # load url
    config = load_yaml_dict(datasource, flowbytype='FBA')

    df = (pd.read_csv(config['source_url'], usecols=['ProcessName',
                                                     'ProcessCategory'])
          .drop_duplicates()
          .reset_index(drop=True)
          .rename(columns={'ProcessName': 'Activity'}))
    df['ProcessCategory'] = df['ProcessCategory'].str.split(
        '/', expand=True)[0]

    pathways = {'Anaerobic digestion': '5622191',  # Subnaics 1 for AD
               'Combustion': '562213',
               'Landfilling': '5622121',  # Subnaics 1 for MSW landfill
               'Composting': '5622192',  # Subnaics 2 for Compost
               'Recycling': '5629201'}  # Subnaics 1 for MSW MRFs

    df['Sector'] = df['ProcessCategory'].replace(pathways)

    df['Material'] = df['Activity'].str.split(' of ', expand=True)[1]
    df['Material'] = df['Material'].str.split(' \(', expand=True)[0]
    df['Material'] = df['Material'].str.split(';', expand=True)[0]

    df['SectorSourceName'] = 'NAICS_2012_Code'
    df['SectorType'] = ''
    df['ActivitySourceName'] = datasource

    # reorder
    df = order_crosswalk(df)
    # save as csv
    df.to_csv(f'{datapath}/activitytosectormapping/NAICS_Crosswalk_'
              f'{datasource}.csv', index=False)
