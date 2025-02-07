# Census_CBP.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Pulls County Business Patterns data in NAICS from the Census Bureau
Writes out to various FlowBySector class files for these data items
EMP = Number of employees, Class = Employment
PAYANN = Annual payroll ($1,000), Class = Money
ESTAB = Number of establishments, Class = Other
This script is designed to run with a configuration parameter
--year = 'year' e.g. 2015
"""
import json
import pandas as pd
import numpy as np
from flowsa.location import get_all_state_FIPS_2, get_county_FIPS
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.flowsa_log import log


def Census_CBP_URL_helper(*, build_url, year, **_):
    """
    This helper function uses the "build_url" input from generateflowbyactivity.py,
    which is a base url for data imports that requires parts of the url text
    string to be replaced with info specific to the data year. This function
    does not parse the data, only modifies the urls from which data
    is obtained.
    :param build_url: string, base url
    :param year: year
    :return: list, urls to call, concat, parse, format into
        Flow-By-Activity format
    """
    urls_census = []
    # This section gets the census data by county instead of by state.
    # This is only for years 2010 and 2011. This is done because the State
    # query that gets all counties returns too many results and errors out.
    if int(year) <= 2012:
        if 2008 <= int(year) <= 2011:
            naics = "NAICS2007"
        elif 2003 <= int(year) <= 2007:
            naics = "NAICS2002"
        elif 1997 <= int(year) <= 2002:
            naics = "NAICS1997"
        else:
            raise flowsa.exceptions.FBANotAvailableError()
        county_fips_df = get_county_FIPS('2010')
        county_fips = county_fips_df.groupby('State')
        for state, counties_df in county_fips:
            # grab first two digits for the state FIPS
            state_digit = counties_df.FIPS.iloc[0][:2]
            for i in range(0, len(counties_df), 20):
                # Group counties in chunks of 20
                chunk = counties_df.iloc[i:i+20]['FIPS']
                url = build_url
                county_digit = ",".join(chunk.str[2:5])
                url = url.replace("__NAICS__", naics)
                url = url.replace("__stateFIPS__", state_digit)
                url = url.replace("__countyFIPS__", county_digit)
                urls_census.append(url)

    else:
        FIPS_2 = get_all_state_FIPS_2()['FIPS_2']
        for state in FIPS_2:
            url = build_url
            url = url.replace("__stateFIPS__", state)
            # specified NAICS code year depends on year of data
            if 2012 <= int(year) <= 2016:
                url = url.replace("__NAICS__", "NAICS2012")
            elif int(year) >= 2017:
                url = url.replace("__NAICS__", "NAICS2017")
            url = url.replace("__countyFIPS__", "*")
            urls_census.append(url)

    return urls_census


def census_cbp_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe, begin
        parsing df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    if(resp.status_code == 204):
        # No content warning, return empty dataframe
        log.warning(f"No content found for {resp.url}")
        return pd.DataFrame()
    cbp_json = json.loads(resp.text)
    # convert response to dataframe
    df_census = pd.DataFrame(
        data=cbp_json[1:len(cbp_json)], columns=cbp_json[0])
    return df_census


def census_cbp_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    # concat dataframes
    df = pd.concat(df_list, sort=False)
    # Add year
    df['Year'] = year
    # convert county='999' to line for full state
    df.loc[df['county'] == '999', 'county'] = '000'
    # Make FIPS as a combo of state and county codes
    df['Location'] = df['state'] + df['county']

    naics_col = [c for c in df.columns if c.startswith('NAICS')][0]
    df = (df
          .drop(columns=['state', 'county'])
          .rename(columns={naics_col: 'ActivityProducedBy'})
          .assign(Description = naics_col)
          .assign(ActivityProducedBy = lambda x: x['ActivityProducedBy'].str.strip())
          .query('ActivityProducedBy != "00"')
          )

    # use "melt" fxn to convert colummns into rows, also keep Flags and merge
    # them back in
    df1 = df.melt(
        id_vars=["Location", "ActivityProducedBy", "Year", "Description"],
        value_vars=['ESTAB', 'EMP', 'PAYANN'],
        var_name="FlowName",
        value_name="FlowAmount")
    df2 = df.melt(
        id_vars=["Location", "ActivityProducedBy", "Year", "Description"],
        value_vars=['ESTAB_F', 'EMP_F', 'PAYANN_F'],
        var_name="FlowName_F",
        value_name="Note")
    df2['FlowName'] = df2['FlowName_F'].str.replace('_F', '')
    df2 = df2.drop(columns=['FlowName_F'])
    df = pd.merge(df1, df2, on=["Location", "ActivityProducedBy", "Year",
                                "Description", "FlowName"])
    # Assign suppressed column
    df = (df.assign(
        Suppressed = np.where(df.Note.isnull(),
                              np.nan, df.Note),
        # FlowAmount = np.where(df.Note.isnull()),
        #                       0, df.FlowAmount)
        )
            .drop(columns='Note'))
    # specify unit based on flowname, payroll in units of thousdand USD
    df = (df
          .assign(Unit = lambda x: np.where(x['FlowName'] == 'PAYANN',
                                           'USD', 'p'))
          .assign(FlowAmount = lambda x: np.where(x['FlowName'] == 'PAYANN',
                                                  x['FlowAmount'].astype(float) * 1000,
                                                  x['FlowAmount']))
          .assign(FlowName = lambda x: x['FlowName'].map(
              {'ESTAB': 'Number of establishments',
               'EMP': 'Number of employees',
               'PAYANN': 'Annual payroll'}))
          )

    # specify class
    df['Class'] = np.select([df['FlowName'] == 'Number of employees',
                             df['FlowName'] == 'Number of establishments',
                             df['FlowName'] == 'Annual payroll'],
                            ['Employment', 'Other', 'Money'])
    # add location system based on year of data
    df = assign_fips_location_system(df, year)
    # hard code data
    df['SourceName'] = 'Census_CBP'
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = None
    return df

if __name__ == "__main__":
    import flowsa
    flowsa.generateflowbyactivity.main(source='Census_CBP', year='2002-2010')
    fba = flowsa.getFlowByActivity('Census_CBP', 2001)
