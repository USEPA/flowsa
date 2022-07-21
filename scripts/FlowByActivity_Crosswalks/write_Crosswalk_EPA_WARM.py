# write_Crosswalk_EPA_WARM.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
"""
Create a crosswalk for EPA_WARM from WARM processes.
"""

import pandas as pd
from flowsa.settings import datapath
from scripts.FlowByActivity_Crosswalks.common_scripts import order_crosswalk

if __name__ == '__main__':
    datasource = 'EPA_WARM'

    df = (pd.read_csv('https://raw.githubusercontent.com/USEPA/WARMer/main/warmer/data/flowsa_inputs/WARMv15_env.csv',
                      usecols=['ProcessName','ProcessCategory'])
          .drop_duplicates()
          .reset_index(drop=True)
          .rename(columns={'ProcessName': 'Activity'}))
    df['ProcessCategory'] = df['ProcessCategory'].str.split('/', expand=True)[0]

    pathways = {'Anaerobic digestion': '5622191', # Subnaics 1 for AD
               'Combustion': '562213',
               'Landfilling': '5622121', # Subnaics 1 for MSW landfill
               'Composting': '5622192', # Subnaics 2 for Compost
               'Recycling': '5629201'} # Subnaics 1 for MSW MRFs

    df['Sector'] = df['ProcessCategory'].replace(pathways)

    df['Material'] = df['Activity'].str.split(' of ', expand=True)[1]
    df['Material'] = df['Material'].str.split(' \(', expand=True)[0]
    df['Material'] = df['Material'].str.split(';', expand=True)[0]

    materials = {'Food Waste': 'F',
                 'Concrete': 'C',}

    df['MaterialCode'] = df['Material'].map(materials)
    df['MaterialCode'] = df['MaterialCode'].fillna('X')
    df['Sector'] = df['Sector'] + df['MaterialCode']

    df['SectorSourceName'] = 'NAICS_2012_Code'
    df['SectorType'] = ''
    df['ActivitySourceName'] = datasource

    # reorder
    df = order_crosswalk(df)
    # save as csv
    df.to_csv(datapath + "activitytosectormapping/" +
              "NAICS_Crosswalk_" + datasource + ".csv", index=False)