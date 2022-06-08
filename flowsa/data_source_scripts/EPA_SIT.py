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
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.location import apply_county_FIPS

def epa_sit_parse(*, source, year, config, **_):

    state = config['state']
    filepath = f"{externaldatapath}/SIT_data/{state}/{config['file']}"
    # dictionary containing Excel sheet-specific information
    sheet_dict = config['sheet_dict']
    # initialize the dataframe
    df0 = pd.DataFrame()

    if not os.path.exists(filepath):
        raise FileNotFoundError(f'SIT file not found in {filepath}')

    # for each sheet in the Excel file containing data...
    for sheet, sheet_dict in config.get('sheet_dict').items():
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
        df['Description'] = sheetname

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

if __name__ == '__main__':
    import flowsa
    flowsa.flowbyactivity.main(source='EPA_SIT', year='2017')
    fba = flowsa.getFlowByActivity('EPA_SIT', '2017')
