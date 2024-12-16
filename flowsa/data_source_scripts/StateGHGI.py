# StateGHGI.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Loads state specific GHGI data to supplement EPA State Inventory Tool (SIT).
"""

import pandas as pd
import os
from io import BytesIO

import flowsa.flowbyactivity
from flowsa.settings import externaldatapath
from flowsa.flowbyfunctions import assign_fips_location_system, \
    load_fba_w_standardized_units
from flowsa.flowsa_log import log
from flowsa.location import apply_county_FIPS
from flowsa.common import load_yaml_dict


data_path = externaldatapath / "StateGHGI_data"

def ME_biogenic_parse(*, source, year, config, **_):
    
    df0 = pd.DataFrame()
    
    filename = config['filename']
    filepath = data_path / filename
    
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

def NY_call(*, resp, year, **_):
    # read in data from Excel sheet
    df = pd.read_csv(BytesIO(resp.content))
    return df

def NY_customized_parse(df_list, source, year, config, **_):
    df = pd.concat(df_list, ignore_index=True)
    
    # make sure the 'year' column is a string
    df['year'] = df['year'].astype(str)

    # drop all years except the desired emissions year
    df.drop(df.index[df['year'] != year], inplace = True)
        
    # drop all data except the conventional accounting method
    df.drop(df.index[df['conventional_accounting'] != 'Yes'], inplace = True)
    
    # add emissions source
    df['ActivityProducedBy'] = df['economic_sector'] + ", " + \
                               df['sector'] + ", " + \
                               df['category'] + ", " + \
                               df['sub_category_1'] + ", " + \
                               df['sub_category_2'] + ", " + \
                               df['sub_category_3']
    
    # rename certain columns
    df = df.rename(columns = {'gas':'FlowName',
                              'mt_co2e_ar5_20_yr':'FlowAmount'})
    
    # drop all other columns
    df = df.filter(['ActivityProducedBy', 'FlowAmount', 'FlowName'])  
    
    # # disaggregate HFC and PFC emissions
    # # dictionary of activities where GHG emissions need to be disaggregated
    # activity_dict = config['disagg_activity_dict']
    # # for all activities included in the dictionary...
    # for activity_name, activity_properties in activity_dict.items():
    #     for flow_group, flow_properties in activity_properties.items():
    #         # name of table to be used for proportional split
    #         table_name = flow_properties.get('table')
    #         # load percentages to be used for proportional split
    #         splits = load_fba_w_standardized_units(datasource=table_name, year=year)
    #         # there are certain circumstances where one or more rows need to be 
    #         # excluded from the table
    #         drop_rows = flow_properties.get('drop_rows')
    #         if drop_rows is not None:
    #             splits = splits[~splits['FlowName'].isin(drop_rows)]
    #         splits['pct'] = splits['FlowAmount'] / splits['FlowAmount'].sum()
    #         splits = splits[['FlowName', 'pct']]
    #         # split fba dataframe to include only those items matching the activity and flow type
    #         mask = (df['ActivityProducedBy'] == activity_name) & (df['FlowName'] == flow_group)
    #         df_activity = df[mask]
    #         df_main = df[~mask]
    #         # apply proportional split to activity data
    #         speciated_df = df_activity.apply(lambda x: [p * x['FlowAmount'] for p in splits['pct']],
    #                         axis=1, result_type='expand')
    #         speciated_df.columns = splits['FlowName']
    #         speciated_df = pd.concat([df_activity, speciated_df], axis=1)
    #         speciated_df = speciated_df.melt(id_vars=['ActivityProducedBy', 'FlowAmount', 'FlowName'],
    #                                          var_name='Flow')
    #         speciated_df['FlowName'] = speciated_df['Flow']
    #         speciated_df['FlowAmount'] = speciated_df['value']
    #         speciated_df.drop(columns=['Flow', 'value'], inplace=True)
    #         # merge split dataframes back together
    #         df = pd.concat([df_main, speciated_df], axis=0, join='inner')
    
    # add hardcoded data
    df['Description'] = 'New York GHGI'
    df['Class'] = 'Chemicals'
    df['SourceName'] = source
    df['FlowType'] = 'ELEMENTARY_FLOW'
    df['Compartment'] = 'air'
    df['Year'] = year
    df['Unit'] = 'MT CO2e (AR5-20)'
    df['DataReliability'] = 5
    df['DataCollection'] = 5

    # add state FIPS code
    df['State'] = 'NY'
    df = apply_county_FIPS(df, year='2015', source_state_abbrev=True)
    # add FIPS location system
    df = assign_fips_location_system(df, '2015')
    
    return df


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

#%%
if __name__ == '__main__':
    import flowsa
    for y in range(2019, 2020):
        flowsa.generateflowbyactivity.main(source='StateGHGI_NY', year=y)
        fba = flowsa.flowbyactivity.getFlowByActivity('StateGHGI_NY', y)
