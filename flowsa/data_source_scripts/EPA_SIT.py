# -*- coding: utf-8 -*-
"""
Loads EPA State Inventory Tool (SIT) data for state specified from external data directory.
Parses EPA SIT data to flowbyactivity format.
"""

import pandas as pd
from flowsa.settings import externaldatapath, log
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.location import apply_county_FIPS

def epa_sit_parse(*, source, year, config, **_):

    state = 'ME' # two-letter state abbreviation
    filename = 'Synthesis Tool.xlsm' # name of file containing SIT data
    filepath = externaldatapath + 'SIT_data/' + state + '/' + filename # filepath with location of SIT data
    sheet_dict = config['sheet_dict'] # dictionary containing Excel sheet-specific information
    # initialize the dataframe
    df0 = pd.DataFrame()

    # for each sheet in the Excel file containing data...
    for table, sheet_dict in config.get('sheet_dict').items():
        sheetname = sheet_dict.get('sheetname', table)
        log.info(f'Loading data from sheet: {sheetname}...')
        # read in data from Excel sheet
        df = pd.read_excel(filepath,
                           sheet_name = sheetname,
                           header=sheet_dict.get('header', 2),
                           usecols="B:AG",
                           nrows=sheet_dict.get('nrows'))
        df.columns = df.columns.map(str) # make sure column headers are imported as strings
        df['ActivityProducedBy'] = df.iloc[:,0]

        # for each row in the data table...
        # ...emissions categories will be renamed with the format 'sheet name, emissions category'
        # ...emissions subcategories will be renamed with the format 'sheet name, emissions category, emissions subcategory'
        for ind in df.index:
            current_header = df['ActivityProducedBy'][ind].rstrip()
            if current_header in sheet_dict.get('headers'):
                active_header = current_header
                if sheet_dict.get('subgroup') == 'activitybyflow':
                    df.loc[ind, 'FlowName'] = active_header
                df.loc[ind,'ActivityProducedBy'] = f'{sheetname}, {active_header}'
            else:
                subheader = df['ActivityProducedBy'][ind].lstrip()
                if sheet_dict.get('subgroup') == 'flow':
                    df.loc[ind, 'FlowName'] = subheader
                    df.loc[ind,'ActivityProducedBy'] = f'{sheetname}, {active_header}'
                elif sheet_dict.get('subgroup') == 'activitybyflow':
                    df.loc[ind, 'FlowName'] = active_header
                    df.loc[ind,'ActivityProducedBy'] = f'{sheetname}, {subheader}'
                else:
                    df.loc[ind,'ActivityProducedBy'] = f'{sheetname}, {active_header}, {subheader}'

        # drop all columns except the desired emissions year and the emissions activity source
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
