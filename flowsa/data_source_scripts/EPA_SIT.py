# EPA_SIT.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Loads EPA State Inventory Tool (SIT) data for state specified from external
data directory. Parses EPA SIT data to flowbyactivity format.
"""

import pandas as pd
from pathlib import Path

import flowsa.flowbyactivity
from flowsa.settings import externaldatapath
from flowsa.flowbyactivity import FlowByActivity
from flowsa.flowbyfunctions import assign_fips_location_system, \
    load_fba_w_standardized_units
from flowsa.flowsa_log import log    
from flowsa.location import apply_county_FIPS
from flowsa.schema import flow_by_activity_fields

def epa_sit_parse(*, source, year, config, **_):

    # initialize the dataframe
    df0 = pd.DataFrame()   

    # for each state listed in the method file...
    for state, state_dict in config['state_list'].items():
        if not Path.is_dir(externaldatapath / f"SIT_data/{state}/"):
            log.warning(f"Skipping {state}, data not found")
            continue
        log.info(f'Parsing data for {state}...')
        schema_dict = config['files'].get(state_dict['schema'], {})
        # for each Excel data file listed in the .yaml...
        for file, file_dict in schema_dict['workbooks'].items():
            log.info(f'Loading data from {file}...')
            filepath = externaldatapath / f"SIT_data/{state}/{file}"
            # dictionary containing Excel sheet-specific information

            if not Path.exists(filepath):
                raise FileNotFoundError(f'SIT file not found in {filepath}')

            # for each sheet in the Excel file containing data...
            for sheet, sheet_dict in file_dict.items():
                # log.info(f'processing SIT: {sheet}')
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
        
                df = df.melt(id_vars=[c for c in ('ActivityProducedBy', 'FlowName')
                                      if c in df.columns],
                             var_name= 'Year', value_name='FlowAmount')
                df = df[pd.to_numeric(df['Year'], errors='coerce').notnull()]
                # add sheet-specific hardcoded data
                if 'subgroup' not in sheet_dict:
                    df['FlowName'] = sheet_dict.get('flow')
                df['Unit'] = sheet_dict.get('unit')
                df['Description'] = sheet
                df['State'] = state

                # If required, adjust the units based on the GWP factors used by the state
                if state_dict.get('GWP', schema_dict['GWP']) != schema_dict['GWP']:
                    df['Unit'] = df['Unit'].str.replace(schema_dict['GWP'],
                                                        state_dict['GWP'])
                # concatenate dataframe from each sheet with existing master dataframe
                df0 = pd.concat([df0, df])

    # add general hardcoded data
    df0['Class'] = 'Chemicals'
    df0['SourceName'] = source
    df0['FlowType'] = "ELEMENTARY_FLOW"
    df0['Compartment'] = 'air'
    df0['DataReliability'] = 5
    df0['DataCollection'] = 5

    # add state FIPS code
    df0 = apply_county_FIPS(df0, year='2015', source_state_abbrev=True)
    # add FIPS location system
    df0 = assign_fips_location_system(df0, '2015')

    return df0

def disaggregate_emissions(fba: FlowByActivity, **_) -> FlowByActivity:
    """
    clean_fba_before_mapping_df_fxn to assign specific flow names to flows
    that are an amalgamation of multiple different GHGs
    (e.g., "HFC, PFC, SF6, and NF3 emissions")
    
    """
    attributes_to_save = {
        attr: getattr(fba, attr) for attr in fba._metadata + ['_metadata']
    }
    # dictionary of activities where GHG emissions need to be disaggregated
    activity_dict = fba.config.get('clean_activity_dict')
    year = fba.config.get('year')
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
                                                   year=year)
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
            if len(fba_activity) == 0:
                log.warning(f'unable to disaggregate {activity_name}')
                continue
            # apply proportional split to activity data
            speciated_df = fba_activity.apply(lambda x: [p * x['FlowAmount'] for p in splits['pct']],
                            axis=1, result_type='expand')
            speciated_df.columns = splits['FlowName']
            speciated_df = pd.concat([fba_activity, speciated_df], axis=1)
            speciated_df = speciated_df.melt(
                id_vars=[c for c in flow_by_activity_fields.keys()
                         if c in speciated_df.columns],
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

    new_fba = FlowByActivity(fba)
    for attr in attributes_to_save:
        setattr(new_fba, attr, attributes_to_save[attr])

    return new_fba

def clean_up_state_data(fba: FlowByActivity, **_):
    """
    clean_fba_before_activity_sets to:
    
    (i) remove states OTHER THAN those selected for
    alternate data sources. State abbreviations must be passed as list
    in method parameter 'state_list'.
    
    (ii) remove specific SIT data for specific states.
    these data must be excluded to avoid double counting because some states 
    have opted to use custom methods (e.g., Vermont estimates emissions from 
    natural gas distribution separately from the SIT tool).
    """
    state_list = fba.config.get('state_list')
    
    # (i) drop all states OTHER THAN those selected for alternative data sources
    state_df = pd.DataFrame(state_list, columns=['State'])
    state_df = apply_county_FIPS(state_df)
    df_subset = fba[fba['Location'].isin(state_df['Location'])]
    
    # (ii) drop unused SIT data from specific states
    
    # if Vermont is included in the inventory, exclude certain data
    # (these data will later be replaced with custom data in the 'StateGHGI'
    # stage)
    if ('VT' in state_list): # and ('StateGHGI_VT' in method['source_names'].keys())
        from flowsa.data_source_scripts.StateGHGI import VT_remove_dupicate_activities
        df_subset = VT_remove_dupicate_activities(df_subset)
   
    return df_subset

if __name__ == '__main__':
    import flowsa
    flowsa.generateflowbyactivity.main(source='EPA_SIT', year='2012-2020')
    fba = flowsa.flowbyactivity.getFlowByActivity('EPA_SIT', 2019)
