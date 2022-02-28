# EPA_StateGHGI.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Inventory of US GHGs from EPA disaggregated to States
"""
import json
import pandas as pd
from flowsa.settings import externaldatapath
from flowsa.location import apply_county_FIPS
from flowsa.flowbyfunctions import assign_fips_location_system


def epa_state_ghgi_parse(*, source, year, config, **_):

    with open(externaldatapath + config.get('file')) as f:
        data = json.load(f)

    data_df = pd.DataFrame(data)
    activity_cols = ['SECTOR', 'SOURCE', 'SUBSOURCE', 'FUEL_TYPE',
                     'SUB_REFERENCE', 'SECSUB_REFERENCE']

    states = data_df[['STATE']].drop_duplicates()
    flows = data_df[['GHG_NAME']].drop_duplicates()

    df = data_df.melt(id_vars = activity_cols + ['STATE'] + ['GHG_NAME'],
                      value_vars=f'EMISSION_{year}',
                      var_name = 'Year',
                      value_name = 'FlowAmount')
    df['Year'] = year
    df['Unit'] = 'MMT CO2e' # TODO confirm units
    df['FlowType'] = 'ELEMENTARY_FLOW'
    df['SourceName'] = source
    df['Class'] = 'Chemicals'
    df['Compartment'] = 'air'

    df.rename(columns={'STATE': 'State',
                       'GHG_NAME': 'FlowName'},
              inplace=True)


    df['ActivityProducedBy'] = df[activity_cols
                                  ].apply(lambda row: ' - '.join(
                                      row.values.astype(str)), axis=1)
    df['ActivityProducedBy'] = df['ActivityProducedBy'].str.replace(' - None', '')
    df.drop(columns=activity_cols, inplace=True)
    activities = df[['ActivityProducedBy']].drop_duplicates()

    df['County'] = ''
    df= apply_county_FIPS(df)
    df = assign_fips_location_system(df, '2015')
    df.drop(columns=['County'], inplace=True)

    return df

if __name__ == '__main__':
    import flowsa
    flowsa.flowbyactivity.main(source='EPA_StateGHGI', year='2017')
    fba = flowsa.getFlowByActivity('EPA_StateGHGI', '2017')