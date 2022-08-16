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

def epa_sit_parse(*, source, year, config, **_):

    state = config['state']
    
    # initialize the dataframe
    df0 = pd.DataFrame()
    
    # for each Excel data file listed in the .yaml...
    for file in config['files']:
        print(f'Loading data from {file}...')
        filepath = f"{externaldatapath}/SIT_data/{state}/{file}"
        # dictionary containing Excel sheet-specific information
        file_dict = config['files'][file]['file_dict']

        if not os.path.exists(filepath):
            raise FileNotFoundError(f'SIT file not found in {filepath}')
    
        # for each sheet in the Excel file containing data...
        for sheet, sheet_dict in file_dict.items():
            sheetname = sheet_dict.get('sheetname', sheet)
            tablename = sheet_dict.get('tablename')
            if tablename:
                sheetandtable = f'{sheetname}, {tablename}'
            else:
                sheetandtable = sheetname
            tablename = sheet_dict.get('tablename', sheetname)
            log.debug(f'Loading data from: {sheetname}...')
            # read in data from Excel sheet
            df = pd.read_excel(filepath,
                               sheet_name = sheetname,
                               header=sheet_dict.get('header', 2),
                               skiprows=range(sheet_dict.get('skiprowstart', 0),
                                              sheet_dict.get('skiprowend', 0)),
                               usecols="B:AG",
                               nrows=sheet_dict.get('nrows'))
            df.columns = df.columns.map(str)
            df['ActivityProducedBy'] = df.iloc[:,0]
    
            # for each row in the data table...
            # ...emissions categories will be renamed with the format
            # 'sheet name, emissions category'
            # ...emissions subcategories will be renamed with the format
            # 'sheet name, emissions category, emissions subcategory'
            for ind in df.index:
                current_header = df['ActivityProducedBy'][ind].strip()
                # for level 1 headers...
                if current_header in sheet_dict.get('headers'):
                    active_header = current_header
                    if sheet_dict.get('subgroup') == 'activitybyflow':
                        df.loc[ind, 'FlowName'] = active_header
                    elif sheet_dict.get('subgroup') == 'flow':
                        df.loc[ind, 'FlowName'] = 'Total N2O and CH4 Emissions'
                    df.loc[ind,'ActivityProducedBy'] = (
                        f'{sheetandtable}, {active_header}')
                # for level 2 headers...
                elif current_header not in sheet_dict.get('subsubheaders',''):
                    active_subheader = df['ActivityProducedBy'][ind].strip()
                    if sheet_dict.get('subgroup') == 'flow':
                        df.loc[ind, 'FlowName'] = active_subheader
                        df.loc[ind,'ActivityProducedBy'] = (
                            f'{sheetandtable}, {active_header}')
                    elif sheet_dict.get('subgroup') == 'activitybyflow':
                        df.loc[ind, 'FlowName'] = active_header
                        df.loc[ind,'ActivityProducedBy'] = (
                            f'{sheetandtable}, {active_subheader}')
                    else:
                        df.loc[ind,'ActivityProducedBy'] = (
                            f'{sheetandtable}, {active_header}, '
                            f'{active_subheader}')
                # for level 3 headers (only occur in IndirectCO2 and Agriculture tabs)...
                else:
                    subsubheader = df['ActivityProducedBy'][ind].strip()
                    df.loc[ind,'ActivityProducedBy'] = (
                        f'{sheetandtable}, {active_header}, '
                        f'{active_subheader}, {subsubheader}')
    
            # drop all columns except the desired emissions year and the
            # emissions activity source
            df = df.filter([year, 'ActivityProducedBy', 'FlowName'])
            # rename columns
            df = df.rename(columns={year: 'FlowAmount'})
            # add sheet-specific hardcoded data
            if 'subgroup' not in sheet_dict:
                df['FlowName'] = sheet_dict.get('flow')
            df['Unit'] = sheet_dict.get('unit')
            df['Description'] = sheet
    
            # concatenate dataframe from each sheet with existing master dataframe
            df0 = pd.concat([df0, df])

    # add general hardcoded data
    df0['Class'] = 'Chemicals'
    df0['SourceName'] = source
    df0['FlowType'] = "ELEMENTARY_FLOW"
    df0['Compartment'] = 'air'
    df0['Year'] = year
    df0['DataReliability'] = 5
    df0['DataCollection'] = 5

    # add state FIPS code
    df0['State'] = state
    df0['County'] = ''
    df0 = apply_county_FIPS(df0, year='2015', source_state_abbrev=True)
    # add FIPS location system
    df0 = assign_fips_location_system(df0, '2015')

    return df0

def disaggregate_emissions(fba, source_dict, **_):
    """
    clean_fba_before_mapping_df_fxn to assign specific flow names to flows
    that are an amalgamation of multiple different GHGs
    (e.g., "HFC, PFC, SF6, and NF3 emissions")
    
    """
    
    # dictionary of activities where GHG emissions need to be disaggregated
    activity_dict = source_dict['clean_activity_dict']

    # for all activities included in the dictionary...
    for activity_name, activity_properties in activity_dict.items():
        disaggregation_method = activity_properties.get('disaggregation_method')

        # if emissions need to be split across multiple species, 
        # allocate emissions proportionally based per the table listed
        if disaggregation_method == 'table':
            # name of table to be used for proportional split
            table_name = activity_properties.get('disaggregation_data_source')
            # load percentages to be used for proportional split
            splits = load_fba_w_standardized_units(datasource=table_name,
                year=source_dict['year'])
            drop_rows = activity_properties.get('drop_rows')
            # there are certain circumstances where one or more rows need to be 
            # excluded from the table
            if drop_rows is not None:
                splits = splits[~splits['FlowName'].isin(drop_rows)]
            splits['pct'] = splits['FlowAmount'] / splits['FlowAmount'].sum()
            splits = splits[['FlowName', 'pct']]
            # split fba dataframe to include only those items matching the activity
            fba_activity = fba[fba['ActivityProducedBy'] == activity_name]
            fba_main = fba[fba['ActivityProducedBy'] != activity_name]
            # apply proportional split to activity data
            speciated_df = fba_activity.apply(lambda x: [p * x['FlowAmount'] for p in splits['pct']],
                            axis=1, result_type='expand')
            speciated_df.columns = splits['FlowName']
            speciated_df = pd.concat([fba_activity, speciated_df], axis=1)
            speciated_df = speciated_df.melt(id_vars=flow_by_activity_fields.keys(),
                                             var_name='Flow')
            speciated_df['FlowName'] = speciated_df['Flow']
            speciated_df['FlowAmount'] = speciated_df['value']
            speciated_df.drop(columns=['Flow', 'value'], inplace=True)
            # merge split dataframes back together
            fba = pd.concat([fba_main, speciated_df], axis=0, join='inner')

        # if emissions are attributable to only one species,
        # allocate emissions directly to single species listed
        elif disaggregation_method == 'direct':
            species_name = activity_properties.get('disaggregation_data_source')
            fba.loc[fba.ActivityProducedBy == activity_name, 'FlowName'] = species_name

    return fba

if __name__ == '__main__':
    import flowsa
    flowsa.flowbyactivity.main(source='EPA_SIT', year='2017')
    fba = flowsa.getFlowByActivity('EPA_SIT', '2017')
