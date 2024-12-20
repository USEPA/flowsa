# USDA_ERS_FIWS.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
USDA Economic Research Service (ERS) Farm Income and Wealth Statistics (FIWS)
https://www.ers.usda.gov/data-products/farm-income-and-wealth-statistics/

Downloads the Dec 3, 2024 update
"""

import zipfile
import io
import pandas as pd
from flowsa.location import US_FIPS, get_all_state_FIPS_2, us_state_abbrev
from flowsa.flowbyfunctions import assign_fips_location_system


def fiws_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    # extract data from zip file (only one csv)
    with zipfile.ZipFile(io.BytesIO(resp.content), "r") as f:
        # read in file names
        for name in f.namelist():
            data = f.open(name)
            df = pd.read_csv(data, encoding="ISO-8859-1")
        return df


def fiws_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    # concat dataframes
    df = pd.concat(df_list, sort=False)
    # select data for chosen year, cast year as string to match argument
    df['Year'] = df['Year'].astype(str)
    df = df[df['Year'] == year].reset_index(drop=True)
    # add state fips codes, reading in datasets from common.py
    fips = get_all_state_FIPS_2().reset_index(drop=True)
    # ensure capitalization of state names
    fips['State'] = fips['State'].apply(lambda x: x.title())
    fips['StateAbbrev'] = fips['State'].map(us_state_abbrev)
    # pad zeroes
    fips['FIPS_2'] = fips['FIPS_2'].apply(lambda x: x.ljust(3 + len(x), '0'))
    df = pd.merge(
        df, fips, how='left', left_on='State', right_on='StateAbbrev')
    # set us location code
    df.loc[df['State_x'] == 'US', 'FIPS_2'] = US_FIPS
    # drop "All" in variabledescription2
    df.loc[df['VariableDescriptionPart2'] ==
           'All', 'VariableDescriptionPart2'] = 'drop'
    # combine variable descriptions to create Activity name and remove ", drop"
    df['ActivityProducedBy'] = df['VariableDescriptionPart1'] + \
                               ', ' + df['VariableDescriptionPart2']
    df['ActivityProducedBy'] = \
        df['ActivityProducedBy'].str.replace(", drop", "", regex=True)
    # trim whitespace
    df['ActivityProducedBy'] = df['ActivityProducedBy'].str.strip()
    # drop columns
    df = df.drop(
        columns=['artificialKey', 'PublicationDate', 'Source',
                 'ChainType_GDP_Deflator', 'VariableDescriptionPart1',
                 'VariableDescriptionPart2', 'State_x', 'State_y',
                 'StateAbbrev', 'unit_desc'])
    # rename columns
    df = df.rename(columns={"VariableDescriptionTotal": "Description",
                            "Amount": "FlowAmount",
                            "FIPS_2": "Location"})
    # assign flowname, based on comma placement
    df['FlowName'] = df['Description'].str.split(',').str[0]
    # add location system based on year of data
    df['Year'] = df['Year'].astype(int)
    df = assign_fips_location_system(df, year)
    # drop unnecessary rows
    df = df[df['FlowName'].str.contains("Cash receipts")]
    # the unit is $1000 USD, so multiply FlowAmount by 1000 and
    # set unit as 'USD'
    df['FlowAmount'] = df['FlowAmount'].astype(float)
    df['FlowAmount'] = df['FlowAmount'] * 1000
    # hard code data
    df['Class'] = 'Money'
    df['SourceName'] = 'USDA_ERS_FIWS'
    df['Unit'] = 'USD'
    # Add DQ scores
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp
    # sort df
    df = df.sort_values(['Location', 'FlowName'])
    # reset index
    df.reset_index(drop=True, inplace=True)

    return df

if __name__ == "__main__":
    import flowsa
    flowsa.generateflowbyactivity.main(year='2012-2023', source='USDA_ERS_FIWS')
    fba = flowsa.getFlowByActivity('USDA_ERS_FIWS', year=2023)
