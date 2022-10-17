# StateGHGI.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Loads state specific GHGI data to supplement EPA State Inventory Tool (SIT).
"""

import pandas as pd
import os
from flowsa.settings import externaldatapath, log
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.location import apply_county_FIPS
from flowsa.common import load_yaml_dict


data_path = f"{externaldatapath}/StateGHGI_data"

def ME_biogenic_parse(*, source, year, config, **_):
    
    df0 = pd.DataFrame()
    
    filename = config['filename']
    filepath = f"{data_path}/{filename}"
    
    # dictionary containing Excel sheet-specific information
    table_dicts = config['table_dict']

    if not os.path.exists(filepath):
        raise FileNotFoundError(f'StateGHGI file not found in {filepath}')

    log.info(f'Loading data from file {filename}...')
        
    # for each data table in the Excel file...
    for table, table_dict in table_dicts.items():

        log.info(f'Loading data from table {table}...')
    
        # read in data from Excel sheet
        df = pd.read_excel(filepath,
                           header=table_dict.get('header'),
                           # usecols="A:AG",
                           nrows=table_dict.get('nrows'))
        df.columns = df.columns.map(str)
        
        # rename certain columns
        df = df.rename(columns = {'Gas':'FlowName',
                                  'Sector/Activity':'ActivityProducedBy',
                                  'Units':'Unit',
                                  year:'FlowAmount'})
        df['ActivityProducedBy'] = df['ActivityProducedBy'] + ", " + table
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
    df0 = apply_county_FIPS(df0, year='2015', source_state_abbrev=True)
    # add FIPS location system
    df0 = assign_fips_location_system(df0, '2015')

    return df0

def VT_supplementary_parse(*, source, year, config, **_):
    
    df0 = pd.DataFrame()
    
    filename = config['filename']
    
    filepath = f"{data_path}/{filename}"
    
    # dictionary containing Excel sheet-specific information
    table_dicts = config['table_dict']

    if not os.path.exists(filepath):
        raise FileNotFoundError(f'{filename} file not found in {filepath}')
    log.info(f'Loading data from file {filename}...')
        
    # for each data table in the Excel file...
    for table, table_dict in table_dicts.items():
        
        log.info(f'Loading data from table {table}...')
    
        # read in data from Excel sheet
        df = pd.read_excel(filepath,
                           header=table_dict.get('header'),
                           # usecols="A:AG",
                           nrows=table_dict.get('nrows'))
        df.columns = df.columns.map(str)
        
        # rename certain columns
        df = df.rename(columns = {'Gas': 'FlowName',
                                  'Sector/Activity': 'ActivityProducedBy',
                                  'Units': 'Unit',
                                  year:'FlowAmount'})

        # drop all years except the desired emissions year
        df = df.filter(['FlowAmount', 'FlowName', 'ActivityProducedBy',
                        'Unit'])

        # concatenate dataframe from each table with existing master dataframe
        df0 = pd.concat([df0, df])        
        
    # add hardcoded data
    df0['Description'] = "Vermont supplementary emissions data"
    df0['Class'] = 'Chemicals'
    df0['SourceName'] = source
    df0['FlowType'] = "ELEMENTARY_FLOW"
    df0['Compartment'] = 'air'
    df0['Year'] = year
    df0['DataReliability'] = 5
    df0['DataCollection'] = 5

    # add state FIPS code
    df0['State'] = 'VT'
    df0 = apply_county_FIPS(df0, year='2015', source_state_abbrev=True)
    # add FIPS location system
    df0 = assign_fips_location_system(df0, '2015')

    return df0


def VT_remove_dupicate_activities(df_subset):
    """Remove activities from standard SIT that are captured by supplementary
    file."""
    fba_config = load_yaml_dict('StateGHGI_VT', flowbytype='FBA')
    for table, table_dict in fba_config['table_dict'].items():
        proxy = table_dict['SIT_APB_proxy']
        proxy = [proxy] if isinstance(proxy, str) else proxy
        df_subset = df_subset.drop(df_subset[
            ((df_subset.Location == '50000') & 
             (df_subset.ActivityProducedBy.isin(proxy))
             )].index)

    return df_subset


if __name__ == '__main__':
    import flowsa
    flowsa.flowbyactivity.main(source='StateGHGI_ME', year='2019')
    fba = flowsa.getFlowByActivity('StateGHGI_ME', '2019')
