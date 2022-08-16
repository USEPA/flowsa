# EPA_SIT.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Loads EPA State Inventory Tool (SIT) data for state specified from external
data directory. Parses EPA SIT data to flowbyactivity format.
"""

import pandas as pd
import os
from flowsa.settings import externaldatapath, log
from flowsa.flowbyfunctions import assign_fips_location_system, \
    load_fba_w_standardized_units
from flowsa.location import apply_county_FIPS
from flowsa.schema import flow_by_activity_fields

def ME_biogenic_parse(*, source, year, config, **_):
    
    # initialize the dataframe
    df0 = pd.DataFrame()
    
    filename = config['filename']
    
    log.info(f'Loading data from file {filename}...')
    
    filepath = f"{externaldatapath}/StateGHGI_data/ME/{filename}"
    
    # dictionary containing Excel sheet-specific information
    file_dict = config['file_dict']

    if not os.path.exists(filepath):
        raise FileNotFoundError(f'StateGHGI file not found in {filepath}')
        
    # for each data table in the Excel file...
    for table, table_dict in file_dict.items():        
        
        log.info(f'Loading data from table {table}...')
    
        # read in data from Excel sheet
        df = pd.read_excel(filepath,
                           header=table_dict.get('header'),
                           usecols="A:AG",
                           nrows=table_dict.get('nrows'))
        df.columns = df.columns.map(str)
        
        # rename certain columns
        df.rename(columns = {
            'Gas':'FlowName',
            'Sector/Activity':'ActivityProducedBy',
            'Units':'Unit',
            year:'FlowAmount'
            }, inplace = True)
        df['ActivityProducedBy'] = df['ActivityProducedBy'] + ", " + table[15:]
        df['FlowName'] = 'Biogenic ' + df['FlowName']

        # drop all years except the desired emissions year
        df = df.filter(['FlowAmount', 'FlowName', 'ActivityProducedBy', 'Unit'])

        # concatenate dataframe from each table with existing master dataframe
        df0 = pd.concat([df0, df])        
        
    # add hardcoded data
    df0['Description'] = "Maine supplementary biogenic emissions data"
    df0['Class'] = 'Chemicals'
    df0['SourceName'] = source
    df0['FlowType'] = "ELEMENTARY_FLOW"
    df0['Compartment'] = 'air'
    df0['Year'] = year
    df0['DataReliability'] = 5
    df0['DataCollection'] = 5

    # add state FIPS code
    df0['State'] = 'ME'
    df0['County'] = ''
    df0 = apply_county_FIPS(df0, year='2015', source_state_abbrev=True)
    # add FIPS location system
    df0 = assign_fips_location_system(df0, '2015')

    return df0


if __name__ == '__main__':
    import flowsa
    flowsa.flowbyactivity.main(source='StateGHGI_ME', year='2019')
    fba = flowsa.getFlowByActivity('StateGHGI_ME', '2019')
